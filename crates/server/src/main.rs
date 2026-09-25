//! beanstalkd-rs: network front-end for bstk-engine (task T4).
//!
//! Architecture (see docs/DESIGN.md §3, §6): one accept loop, one task per
//! connection (`conn::handle_connection`), and one engine actor task that
//! is the sole owner of `bstk_engine::Engine` (`engine_actor::run`).

mod cli;
mod conn;
mod engine_actor;
mod sysinfo;

use std::io;
use std::net::SocketAddr;

use clap::Parser;
use tokio::net::TcpSocket;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::mpsc;

use bstk_engine::{ConnId, EngineConfig};

use cli::Cli;
use engine_actor::EngineMsg;
use sysinfo::ProcessSysInfo;

#[tokio::main]
async fn main() -> io::Result<()> {
    let cli = Cli::parse();

    if cli.version {
        println!("beanstalkd-rs {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }

    tracing_subscriber::fmt()
        .with_max_level(cli::tracing_level(cli.verbose))
        .with_writer(std::io::stderr)
        .init();

    sysinfo::raise_nofile_limit();

    run(cli).await
}

async fn run(cli: Cli) -> io::Result<()> {
    let addr = SocketAddr::new(cli.listen_addr, cli.port);
    let socket = if addr.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };
    socket.set_reuseaddr(true)?;
    socket.bind(addr)?;
    // Matches `listen(fd, 1024)` in net.c.
    let listener = socket.listen(1024)?;
    tracing::info!(%addr, "beanstalkd-rs listening");

    let cfg = EngineConfig {
        max_job_size: cli.max_job_size,
        ..EngineConfig::default()
    };
    let engine_tx = engine_actor::spawn(cfg, Box::new(ProcessSysInfo::collect()));

    // Signal handling per T4 requirement 9: SIGUSR1 enters drain mode
    // (never leaves it, matching the reference); SIGINT/SIGTERM stop
    // accepting and exit 0.
    let mut sigusr1 = signal(SignalKind::user_defined1())
        .expect("SIGUSR1 is always installable as a tokio signal handler on Unix");
    let mut sigint = signal(SignalKind::interrupt())
        .expect("SIGINT is always installable as a tokio signal handler on Unix");
    let mut sigterm = signal(SignalKind::terminate())
        .expect("SIGTERM is always installable as a tokio signal handler on Unix");

    let mut next_id: ConnId = 1;

    loop {
        tokio::select! {
            _ = sigusr1.recv() => {
                tracing::warn!("SIGUSR1 received: entering drain mode");
                let _ = engine_tx.send(EngineMsg::SetDraining(true));
            }
            _ = sigint.recv() => {
                tracing::info!("SIGINT received: shutting down");
                return Ok(());
            }
            _ = sigterm.recv() => {
                tracing::info!("SIGTERM received: shutting down");
                return Ok(());
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
                    cli.max_job_size,
                ));
            }
        }
    }
}
