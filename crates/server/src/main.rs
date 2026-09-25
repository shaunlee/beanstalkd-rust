//! beanstalkd-rs: network front-end for bstk-engine.
//!
//! Architecture (see docs/DESIGN.md §3, §6): one accept loop and one task
//! per connection (`conn::handle_connection`) on a tokio runtime, and one
//! engine actor that is the sole owner of `bstk_engine::Engine` and, with
//! `-b`, of the write-ahead log (`engine_actor::Actor`). With `-b` the
//! actor runs on a dedicated OS thread; without it, as a tokio task (see
//! `engine_actor` for why).
//!
//! Startup: parse flags, bind and listen (like the reference, a port
//! error comes before any binlog work), then with `-b` lock and replay the
//! binlog and rebuild the engine from it, and only then accept
//! connections. (The reference already serves the listening socket from
//! its event loop only after `srv_acquire_wal`, so the difference is just
//! that our listen backlog is not drained during recovery either.)
//!
//! Exit statuses: 0 after SIGINT / SIGTERM; 1 for a startup error (socket,
//! binlog replay or I/O); 5 for a usage error (`-u`); 10 when another
//! process holds the binlog directory lock (as the reference); 20 after a
//! binlog write, fsync or compaction error while serving
//! (`engine_actor::EXIT_WAL_FAILURE`); clap's usage errors exit with 2.

mod cli;
mod conn;
mod engine_actor;
mod sysinfo;

use std::io;
use std::net::SocketAddr;
use std::process::ExitCode;

use tokio::net::{TcpListener, TcpSocket};
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::{mpsc, oneshot};

use bstk_engine::{ConnId, Engine, EngineConfig};
use bstk_store::{Wal, WalError, WalOptions};

use cli::Cli;
use engine_actor::{Actor, Clock, EngineHandle, EngineMsg};
use sysinfo::ProcessSysInfo;

/// Startup failure (socket, binlog replay or I/O).
const EXIT_STARTUP: u8 = 1;
/// Usage error the reference reports through `usage(5)`.
const EXIT_USAGE: i32 = 5;
/// `srv_acquire_wal`: the binlog directory is locked by another process.
const EXIT_LOCKED: u8 = 10;

fn main() -> ExitCode {
    let cli = Cli::from_env();

    if cli.version {
        println!("beanstalkd-rs {}", env!("CARGO_PKG_VERSION"));
        return ExitCode::SUCCESS;
    }

    if let Some(user) = &cli.user {
        // Dropping privileges is out of scope; refusing is safer than
        // silently running as the current (possibly root) user.
        eprintln!(
            "beanstalkd-rs: -u {user}: changing user is not supported; \
             start beanstalkd-rs as the desired user instead"
        );
        std::process::exit(EXIT_USAGE);
    }

    tracing_subscriber::fmt()
        .with_max_level(cli::tracing_level(cli.verbose))
        .with_writer(std::io::stderr)
        .init();

    sysinfo::raise_nofile_limit();

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("beanstalkd-rs: cannot start the runtime: {e}");
            return ExitCode::from(EXIT_STARTUP);
        }
    };
    let addr = SocketAddr::new(cli.listen_addr, cli.port);
    let listener = match runtime.block_on(async { listen(addr) }) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("beanstalkd-rs: cannot listen on {addr}: {e}");
            return ExitCode::from(EXIT_STARTUP);
        }
    };
    tracing::info!(%addr, "beanstalkd-rs listening");

    // Binlog replay is blocking file I/O: it runs here, on the main
    // thread, outside the runtime.
    let engine_tx = match start_engine(&cli, runtime.handle()) {
        Ok(tx) => tx,
        Err(code) => return code,
    };
    runtime.block_on(serve(listener, engine_tx, cli.max_job_size))
}

fn listen(addr: SocketAddr) -> io::Result<TcpListener> {
    let socket = if addr.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };
    socket.set_reuseaddr(true)?;
    socket.bind(addr)?;
    // Matches `listen(fd, 1024)` in net.c.
    socket.listen(1024)
}

/// Builds (or, with `-b`, recovers) the engine and starts the actor
/// thread. Blocking; runs before any connection is accepted.
fn start_engine(cli: &Cli, runtime: &tokio::runtime::Handle) -> Result<EngineHandle, ExitCode> {
    let clock = Clock::start();
    let sys = Box::new(ProcessSysInfo::collect());
    let mut cfg = EngineConfig {
        max_job_size: cli.max_job_size,
        // `-s` is reported even without `-b`, as the reference does.
        binlog_max_size: cli.binlog_file_size,
        journal: false,
    };
    let actor = match &cli.binlog_dir {
        None => Actor::<Wal>::new(clock, Engine::new(clock.now(), cfg, sys), None),
        Some(dir) => {
            let opts = WalOptions {
                dir: dir.clone(),
                file_size: cli.binlog_file_size,
                sync: cli.sync,
            };
            let (wal, recovery) = match Wal::open(opts) {
                Ok(opened) => opened,
                Err(WalError::Locked) => {
                    eprintln!("beanstalkd-rs: failed to lock wal dir {}", dir.display());
                    return Err(ExitCode::from(EXIT_LOCKED));
                }
                Err(e) => {
                    eprintln!(
                        "beanstalkd-rs: failed to replay log in {}: {e}",
                        dir.display()
                    );
                    return Err(ExitCode::from(EXIT_STARTUP));
                }
            };
            tracing::info!(
                dir = %dir.display(),
                jobs = recovery.jobs.len(),
                sync = ?cli.sync,
                "binlog replayed"
            );
            cfg.journal = true;
            let engine = Engine::recover(clock.now(), cfg, sys, recovery);
            Actor::new(clock, engine, Some((wal, cli.sync)))
        }
    };
    match actor.spawn(runtime) {
        Ok(tx) => Ok(tx),
        Err(e) => {
            eprintln!("beanstalkd-rs: cannot start the engine thread: {e}");
            Err(ExitCode::from(EXIT_STARTUP))
        }
    }
}

async fn serve(listener: TcpListener, engine_tx: EngineHandle, max_job_size: u32) -> ExitCode {
    // SIGUSR1 enters drain mode (never leaves it, matching the reference);
    // SIGINT/SIGTERM stop accepting, let the actor sync the binlog, and
    // exit 0.
    let (mut sigusr1, mut sigint, mut sigterm) = match (
        signal(SignalKind::user_defined1()),
        signal(SignalKind::interrupt()),
        signal(SignalKind::terminate()),
    ) {
        (Ok(a), Ok(b), Ok(c)) => (a, b, c),
        (Err(e), _, _) | (_, Err(e), _) | (_, _, Err(e)) => {
            eprintln!("beanstalkd-rs: cannot install signal handlers: {e}");
            return ExitCode::from(EXIT_STARTUP);
        }
    };

    let mut next_id: ConnId = 1;

    loop {
        tokio::select! {
            _ = sigusr1.recv() => {
                tracing::warn!("SIGUSR1 received: entering drain mode");
                let _ = engine_tx.send(EngineMsg::SetDraining(true));
            }
            _ = sigint.recv() => {
                tracing::info!("SIGINT received: shutting down");
                break;
            }
            _ = sigterm.recv() => {
                tracing::info!("SIGTERM received: shutting down");
                break;
            }
            accepted = listener.accept() => {
                let (stream, _peer) = match accepted {
                    Ok(pair) => pair,
                    Err(e) => {
                        tracing::warn!("accept() failed: {e}");
                        continue;
                    }
                };
                if let Err(e) = stream.set_nodelay(true) {
                    tracing::debug!("set_nodelay() failed: {e}");
                }
                let conn = next_id;
                next_id += 1;
                let (reply_tx, reply_rx) = mpsc::unbounded_channel();
                // Connect must reach the engine before this connection's
                // first command; sending it here, before spawning the
                // connection task, guarantees that ordering on the shared,
                // FIFO engine channel.
                if engine_tx.send(EngineMsg::Connect { conn, reply_tx }).is_err() {
                    tracing::error!("engine actor is gone; dropping new connection");
                    continue;
                }
                tokio::spawn(conn::handle_connection(
                    stream,
                    conn,
                    engine_tx.clone(),
                    reply_rx,
                    max_job_size,
                ));
            }
        }
    }

    drop(listener);
    let (done, synced) = oneshot::channel();
    if engine_tx.send(EngineMsg::Shutdown { done }).is_ok() {
        // The actor finishes the messages queued before this one, syncs
        // the binlog and acknowledges. An error means it is already gone.
        let _ = synced.await;
    }
    ExitCode::SUCCESS
}
