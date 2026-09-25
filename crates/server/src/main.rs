//! beanstalkd-rs: network front-end for bstk-engine.
//!
//! Architecture (see docs/DESIGN.md §3, §6): one accept loop per listener
//! and one task per connection (`conn`) on a tokio runtime, and one engine
//! actor that is the sole owner of `bstk_engine::Engine` and, with `-b`,
//! of the write-ahead log (`engine_actor::Actor`). With `-b` the actor
//! runs on a dedicated OS thread; without it, as a tokio task (see
//! `engine_actor` for why). An optional HTTP listener (`http`) serves
//! health, readiness and monitoring endpoints.
//!
//! Startup:
//! 1. parse flags; `--check-config` validates the configuration (including
//!    the TLS files), prints a summary and exits;
//! 2. load `--config` (if any) and resolve it against the flags; set up
//!    logging per `[log]`; load the TLS certificates;
//! 3. bind every listener (like the reference, a port error comes before
//!    any binlog work);
//! 4. bind and start the HTTP listener, if configured (`/readyz` is 503
//!    until step 6);
//! 5. with a binlog, lock and replay it and rebuild the engine from it;
//! 6. start the accept loops (the server is now ready).
//!
//! (The reference already serves the listening socket from its event loop
//! only after `srv_acquire_wal`, so the difference is just that our listen
//! backlog is not drained during recovery either.) Without a configuration
//! file this is exactly the P1 behavior: one plaintext listener from
//! `-l` / `-p`, no HTTP listener.
//!
//! Exit statuses: 0 after SIGINT / SIGTERM, and for a valid
//! `--check-config`; 1 for a startup error (invalid configuration, TLS
//! files, socket, binlog replay or I/O); 5 for a usage error (`-u`); 10
//! when another process holds the binlog directory lock (as the
//! reference); 20 after a binlog write, fsync or compaction error while
//! serving (`engine_actor::EXIT_WAL_FAILURE`); clap's usage errors exit
//! with 2.

mod auth;
mod cli;
mod config;
mod conn;
mod engine_actor;
mod http;
mod metrics;
mod pending;
mod sysinfo;
mod tls;

use std::io;
use std::net::SocketAddr;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::net::{TcpListener, TcpSocket};
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio_rustls::TlsAcceptor;

use bstk_engine::{Engine, EngineConfig};
use bstk_store::{Wal, WalError};

use auth::TokenSet;
use cli::Cli;
use config::{AuthMode, LogFormat, LogSettings, ResolvedConfig};
use conn::{TlsAuth, TlsListener};
use engine_actor::{Actor, Clock, EngineHandle, EngineMsg};
use pending::ServerCounters;
use sysinfo::ProcessSysInfo;
use tls::TlsConfigs;

/// Startup failure (configuration, TLS files, socket, binlog replay or
/// I/O).
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

    if cli.check_config {
        return check_config(&cli);
    }

    let config = match config::load(&cli) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("beanstalkd-rs: invalid configuration: {e}");
            return ExitCode::from(config::EXIT_CONFIG);
        }
    };

    init_logging(config.log);
    for w in &config.warnings {
        tracing::warn!("{w}");
    }

    let tls = match config.tls.as_ref().map(tls::load).transpose() {
        Ok(t) => t,
        Err(e) => {
            eprintln!("beanstalkd-rs: {e}");
            return ExitCode::from(EXIT_STARTUP);
        }
    };

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

    let listeners = match bind_listeners(&runtime, &config, tls.as_ref()) {
        Ok(l) => l,
        Err(code) => return code,
    };

    // SIGINT / SIGTERM set this; every accept loop and the HTTP listener
    // stop on it.
    let (stop_tx, stop_rx) = watch::channel(false);

    // Pending TLS connections and authentication counters (only TLS
    // listeners touch them; the HTTP listener exports them).
    let counters = ServerCounters::new(config.max_pending_connections);

    // Started before the binlog replay, so /healthz and /readyz answer
    // during recovery.
    let http = match &config.http {
        None => None,
        Some(settings) => {
            let listener = match runtime.block_on(async { http::bind(settings.addr) }) {
                Ok(l) => l,
                Err(e) => {
                    eprintln!(
                        "beanstalkd-rs: cannot listen on {} (http): {e}",
                        settings.addr
                    );
                    return ExitCode::from(EXIT_STARTUP);
                }
            };
            tracing::info!(addr = %settings.addr, "HTTP listener started");
            let state = http::HttpState::new(settings, Arc::clone(&counters));
            let task = runtime.spawn(http::serve(listener, Arc::clone(&state), stop_rx.clone()));
            Some((state, task))
        }
    };

    // Binlog replay is blocking file I/O: it runs here, on the main
    // thread, outside the runtime (whose workers serve HTTP meanwhile).
    let engine_tx = match start_engine(&config, runtime.handle()) {
        Ok(tx) => tx,
        Err(code) => return code,
    };

    let tokens = Arc::new(TokenSet::new(&config.tokens));
    runtime.block_on(serve(
        listeners,
        engine_tx,
        ConnSettings {
            max_job_size: config.max_job_size,
            auth_timeout: config.auth_timeout,
            tokens,
            counters,
        },
        http,
        stop_tx,
        stop_rx,
    ))
}

/// What `serve` hands to the connections of every listener.
struct ConnSettings {
    max_job_size: u32,
    /// `auth.timeout` (token listeners only).
    auth_timeout: std::time::Duration,
    /// Accepted tokens (token listeners only).
    tokens: Arc<TokenSet>,
    /// Pending-connection accounting (TLS listeners only).
    counters: Arc<ServerCounters>,
}

/// `--check-config`: resolves the configuration and also loads its TLS
/// files, so a bad certificate is caught here too. Exit status 0 when
/// valid, `config::EXIT_CONFIG` otherwise.
fn check_config(cli: &Cli) -> ExitCode {
    let mut out = io::stdout();
    let mut err = io::stderr();
    // Report a TLS problem first; otherwise print the summary (or the
    // configuration error) exactly as `config::run_check` does.
    if let Ok(config) = config::load(cli)
        && let Some(files) = &config.tls
        && let Err(e) = tls::load(files)
    {
        eprintln!("beanstalkd-rs: invalid configuration: {e}");
        return ExitCode::from(config::EXIT_CONFIG);
    }
    ExitCode::from(config::run_check(cli, &mut out, &mut err))
}

fn init_logging(log: LogSettings) {
    let builder = tracing_subscriber::fmt()
        .with_max_level(log.level.to_tracing())
        .with_writer(io::stderr);
    match log.format {
        LogFormat::Text => builder.init(),
        LogFormat::Json => builder.json().init(),
    }
}

pub(crate) fn listen(addr: SocketAddr) -> io::Result<TcpListener> {
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

/// How connections on a bound listener are served.
enum Kind {
    Plain,
    Tls { acceptor: TlsAcceptor, auth: Auth },
}

/// Authentication on a TLS listener (the tokens are attached in `serve`).
#[derive(Clone, Copy)]
enum Auth {
    /// None beyond the handshake (`auth = "none"` or `"mtls"`).
    Handshake,
    Token,
}

struct Bound {
    listener: TcpListener,
    kind: Kind,
}

/// Binds every configured listener, in order.
fn bind_listeners(
    runtime: &tokio::runtime::Runtime,
    config: &ResolvedConfig,
    tls: Option<&TlsConfigs>,
) -> Result<Vec<Bound>, ExitCode> {
    let mut bound = Vec::with_capacity(config.listeners.len());
    for l in &config.listeners {
        let kind = if l.tls {
            // `config::resolve` guarantees `[tls]` for a TLS listener and
            // `client_ca` for an mTLS one.
            let server_config = match (l.auth, tls) {
                (AuthMode::Mtls, Some(t)) => t.mtls.clone(),
                (_, Some(t)) => Some(Arc::clone(&t.plain)),
                (_, None) => None,
            };
            let Some(server_config) = server_config else {
                eprintln!(
                    "beanstalkd-rs: listener {}: missing TLS configuration",
                    l.addr
                );
                return Err(ExitCode::from(EXIT_STARTUP));
            };
            let auth = match l.auth {
                AuthMode::Token => Auth::Token,
                AuthMode::None | AuthMode::Mtls => Auth::Handshake,
            };
            Kind::Tls {
                acceptor: TlsAcceptor::from(server_config),
                auth,
            }
        } else {
            Kind::Plain
        };
        let listener = match runtime.block_on(async { listen(l.addr) }) {
            Ok(listener) => listener,
            Err(e) => {
                eprintln!("beanstalkd-rs: cannot listen on {}: {e}", l.addr);
                return Err(ExitCode::from(EXIT_STARTUP));
            }
        };
        let addr = listener.local_addr().unwrap_or(l.addr);
        if l.tls {
            tracing::info!(%addr, auth = ?l.auth, "beanstalkd-rs listening (TLS)");
        } else {
            tracing::info!(%addr, "beanstalkd-rs listening");
        }
        bound.push(Bound { listener, kind });
    }
    Ok(bound)
}

/// Builds (or, with a binlog, recovers) the engine and starts the actor.
/// Blocking; runs before any connection is accepted.
fn start_engine(
    config: &ResolvedConfig,
    runtime: &tokio::runtime::Handle,
) -> Result<EngineHandle, ExitCode> {
    let clock = Clock::start();
    let sys = Box::new(ProcessSysInfo::collect());
    let mut cfg = EngineConfig {
        max_job_size: config.max_job_size,
        // `-s` is reported even without `-b`, as the reference does.
        binlog_max_size: config.binlog.file_size,
        journal: false,
    };
    let actor = match config.binlog.wal_options() {
        None => Actor::<Wal>::new(clock, Engine::new(clock.now(), cfg, sys), None),
        Some(opts) => {
            let dir = opts.dir.clone();
            let sync = opts.sync;
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
                sync = ?sync,
                "binlog replayed"
            );
            cfg.journal = true;
            let engine = Engine::recover(clock.now(), cfg, sys, recovery);
            Actor::new(clock, engine, Some((wal, sync)))
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

async fn serve(
    listeners: Vec<Bound>,
    engine_tx: EngineHandle,
    settings: ConnSettings,
    http: Option<(Arc<http::HttpState>, JoinHandle<()>)>,
    stop_tx: watch::Sender<bool>,
    stop_rx: watch::Receiver<bool>,
) -> ExitCode {
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

    // Connection ids are unique across listeners.
    let next_id = Arc::new(AtomicU64::new(1));
    let mut tasks = Vec::with_capacity(listeners.len() + 1);
    for Bound { listener, kind } in listeners {
        let engine_tx = engine_tx.clone();
        let next_id = Arc::clone(&next_id);
        let stop = stop_rx.clone();
        tasks.push(match kind {
            Kind::Plain => tokio::spawn(accept_plain(
                listener,
                next_id,
                engine_tx,
                settings.max_job_size,
                stop,
            )),
            Kind::Tls { acceptor, auth } => {
                let auth = match auth {
                    Auth::Handshake => TlsAuth::Handshake,
                    Auth::Token => TlsAuth::Token(Arc::clone(&settings.tokens)),
                };
                let shared = Arc::new(TlsListener {
                    acceptor,
                    auth,
                    next_id,
                    engine_tx,
                    max_job_size: settings.max_job_size,
                    counters: Arc::clone(&settings.counters),
                    auth_timeout: settings.auth_timeout,
                });
                tokio::spawn(accept_tls(listener, shared, stop))
            }
        });
    }
    // Every listener is bound and the engine is recovered: ready.
    if let Some((state, task)) = http {
        state.set_ready(engine_tx.clone());
        tasks.push(task);
    }

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
        }
    }

    // Stop accepting everywhere (the listening sockets close as their
    // tasks end), then shut the engine down.
    let _ = stop_tx.send(true);
    for task in tasks {
        let _ = task.await;
    }
    let (done, synced) = oneshot::channel();
    if engine_tx.send(EngineMsg::Shutdown { done }).is_ok() {
        // The actor finishes the messages queued before this one, syncs
        // the binlog and acknowledges. An error means it is already gone.
        let _ = synced.await;
    }
    ExitCode::SUCCESS
}

/// Accept loop of a plaintext listener: exactly the P1 path. `Connect` is
/// sent here, before the connection task is spawned.
async fn accept_plain(
    listener: TcpListener,
    next_id: Arc<AtomicU64>,
    engine_tx: EngineHandle,
    max_job_size: u32,
    mut stop: watch::Receiver<bool>,
) {
    loop {
        let accepted = tokio::select! {
            _ = stop.wait_for(|&s| s) => return,
            accepted = listener.accept() => accepted,
        };
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
        let conn = next_id.fetch_add(1, Ordering::Relaxed);
        let (reply_tx, reply_rx) = mpsc::unbounded_channel();
        // Connect must reach the engine before this connection's first
        // command; sending it here, before spawning the connection task,
        // guarantees that ordering on the shared, FIFO engine channel.
        if engine_tx
            .send(EngineMsg::Connect { conn, reply_tx })
            .is_err()
        {
            tracing::error!("engine actor is gone; dropping new connection");
            continue;
        }
        tokio::spawn(conn::handle_plain(
            stream,
            conn,
            engine_tx.clone(),
            reply_rx,
            max_job_size,
        ));
    }
}

/// Accept loop of a TLS listener. The handshake (and authentication) runs
/// in the connection task, so a slow client never holds up accepting.
/// Every accepted connection counts as pending (`pending`) until its
/// handshake or authentication is done; past `server.max_pending_connections`
/// a new connection is closed right away.
async fn accept_tls(
    listener: TcpListener,
    shared: Arc<TlsListener>,
    mut stop: watch::Receiver<bool>,
) {
    loop {
        let accepted = tokio::select! {
            _ = stop.wait_for(|&s| s) => return,
            accepted = listener.accept() => accepted,
        };
        let (stream, peer) = match accepted {
            Ok(pair) => pair,
            Err(e) => {
                tracing::warn!("accept() failed: {e}");
                continue;
            }
        };
        let Some(pending) = shared.counters.try_acquire() else {
            // Too many pending connections: close this one (counted and
            // logged, rate-limited, by `try_acquire`).
            drop(stream);
            continue;
        };
        if let Err(e) = stream.set_nodelay(true) {
            tracing::debug!("set_nodelay() failed: {e}");
        }
        tokio::spawn(conn::handle_tls(stream, peer, Arc::clone(&shared), pending));
    }
}
