//! Spawning and lifecycle management of a server binary under test.

use std::collections::HashSet;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

use crate::dsl::StopMode;

const STARTUP_TIMEOUT: Duration = Duration::from_secs(5);
const STOP_TIMEOUT: Duration = Duration::from_secs(5);
const SPAWN_ATTEMPTS: u32 = 3;

/// Ports currently claimed by a `ServerProcess` somewhere in this process.
///
/// `TcpListener::bind(("127.0.0.1", 0))` picks an OS-assigned free port, but
/// there is a gap between releasing that listener and the child process
/// actually binding the same port. Under this harness's parallel case
/// runner, two worker threads can race and get handed the *same* ephemeral
/// port in that gap, causing two unrelated cases to collide on one server.
/// This registry closes that race within our own process: a port is held
/// for the entire lifetime of the `ServerProcess`, not just during spawn.
static CLAIMED_PORTS: LazyLock<Mutex<HashSet<u16>>> = LazyLock::new(|| Mutex::new(HashSet::new()));

fn claim_free_port() -> std::io::Result<u16> {
    loop {
        let listener = TcpListener::bind(("127.0.0.1", 0))?;
        let port = listener.local_addr()?.port();
        drop(listener);
        let mut claimed = CLAIMED_PORTS.lock().expect("claimed-ports lock poisoned");
        if claimed.insert(port) {
            return Ok(port);
        }
        // Another case in this process already claimed this exact port
        // (extremely unlikely, but cheap to retry); pick another one.
    }
}

fn release_port(port: u16) {
    CLAIMED_PORTS
        .lock()
        .expect("claimed-ports lock poisoned")
        .remove(&port);
}

/// A per-server temporary binlog directory, removed (recursively) on drop.
#[derive(Debug)]
pub struct BinlogDir {
    path: PathBuf,
}

impl BinlogDir {
    /// Create a fresh, empty directory under the system temp directory.
    pub fn create() -> std::io::Result<Self> {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let base = std::env::temp_dir();
        loop {
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = base.join(format!("bstk-compat-binlog-{}-{n}", std::process::id()));
            match std::fs::create_dir(&path) {
                Ok(()) => return Ok(BinlogDir { path }),
                // Left over from an earlier run that reused our pid.
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(e),
            }
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for BinlogDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// How to start a server: the binary, the case's extra arguments, and
/// whether it gets its own binlog directory.
#[derive(Debug, Clone)]
pub struct ServerConfig<'a> {
    pub bin: &'a Path,
    pub extra_args: &'a [String],
    pub binlog: bool,
}

/// A running server process, spawned on a free `127.0.0.1` port. Killed and
/// reaped when dropped, so a panicking test never leaves an orphan process
/// behind. Its port is released from [`CLAIMED_PORTS`] and its binlog
/// directory (if any) is removed on drop as well.
pub struct ServerProcess {
    child: Child,
    addr: SocketAddr,
    bin: PathBuf,
    /// Full argument list after `-l 127.0.0.1 -p <port>` (the case's extra
    /// args plus `-b <dir>` when a binlog directory is used).
    args: Vec<String>,
    // Declared last so it is dropped after the process has been reaped
    // (fields drop after `Drop::drop` runs).
    binlog_dir: Option<BinlogDir>,
}

impl ServerProcess {
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// OS process id of the server (target of `signal` steps). Changes
    /// after a restart.
    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    /// The binlog directory passed as `-b`, if any.
    pub fn binlog_dir(&self) -> Option<&Path> {
        self.binlog_dir.as_ref().map(BinlogDir::path)
    }

    /// Stop the server (`Term`: SIGTERM, `Kill`: SIGKILL), wait for it to
    /// exit, sleep `downtime`, then start the same binary with the same
    /// arguments and binlog directory. The same port is reused when
    /// possible; otherwise a new one is claimed and [`Self::addr`] changes.
    pub fn restart(&mut self, how: StopMode, downtime: Duration) -> Result<(), SpawnError> {
        self.stop(how)?;
        std::thread::sleep(downtime);

        let port = self.addr.port();
        let mut last_err = String::new();
        // First try the same port a few times (the old listener is gone
        // once the process has been reaped; servers use SO_REUSEADDR).
        for _ in 0..SPAWN_ATTEMPTS {
            match start_on_port(&self.bin, port, &self.args) {
                Ok(child) => {
                    self.child = child;
                    return Ok(());
                }
                Err(e) => last_err = e.0,
            }
        }
        // Fall back to a fresh port.
        for _ in 0..SPAWN_ATTEMPTS {
            let new_port = claim_free_port()
                .map_err(|e| SpawnError(format!("could not find a free port: {e}")))?;
            match start_on_port(&self.bin, new_port, &self.args) {
                Ok(child) => {
                    self.child = child;
                    release_port(port);
                    self.addr = ([127, 0, 0, 1], new_port).into();
                    return Ok(());
                }
                Err(e) => {
                    release_port(new_port);
                    last_err = e.0;
                }
            }
        }
        Err(SpawnError(format!(
            "server {} failed to restart: {last_err}",
            self.bin.display()
        )))
    }

    fn stop(&mut self, how: StopMode) -> Result<(), SpawnError> {
        match how {
            StopMode::Kill => {
                self.child
                    .kill()
                    .map_err(|e| SpawnError(format!("SIGKILL failed: {e}")))?;
            }
            StopMode::Term => {
                // Via kill(1): keeps this crate free of `unsafe` and libc.
                let status = Command::new("kill")
                    .arg("-TERM")
                    .arg(self.child.id().to_string())
                    .status()
                    .map_err(|e| SpawnError(format!("failed to run kill: {e}")))?;
                if !status.success() {
                    return Err(SpawnError(format!("kill -TERM exited with {status}")));
                }
            }
        }
        let deadline = Instant::now() + STOP_TIMEOUT;
        loop {
            match self.child.try_wait() {
                Ok(Some(_)) => return Ok(()),
                Ok(None) => {}
                Err(e) => return Err(SpawnError(format!("wait failed: {e}"))),
            }
            if Instant::now() >= deadline {
                let _ = self.child.kill();
                let _ = self.child.wait();
                return Err(SpawnError(format!(
                    "server did not exit within {STOP_TIMEOUT:?} after {}",
                    how.directive()
                )));
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        release_port(self.addr.port());
    }
}

#[derive(Debug)]
pub struct SpawnError(pub String);

impl std::fmt::Display for SpawnError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for SpawnError {}

/// Spawn `bin` with `-l 127.0.0.1 -p <port>` plus `extra_args`, and block
/// until it accepts TCP connections (or the startup timeout elapses).
pub fn spawn_server(bin: &Path, extra_args: &[String]) -> Result<ServerProcess, SpawnError> {
    spawn(&ServerConfig {
        bin,
        extra_args,
        binlog: false,
    })
}

/// Spawn a server as described by `cfg`: like [`spawn_server`], plus a
/// fresh binlog directory passed as `-b <dir>` when `cfg.binlog` is set.
pub fn spawn(cfg: &ServerConfig<'_>) -> Result<ServerProcess, SpawnError> {
    let bin = cfg.bin;
    if !bin.exists() {
        return Err(SpawnError(format!(
            "server binary not found: {}",
            bin.display()
        )));
    }

    let mut args = cfg.extra_args.to_vec();
    let binlog_dir = if cfg.binlog {
        let dir = BinlogDir::create()
            .map_err(|e| SpawnError(format!("could not create binlog directory: {e}")))?;
        args.push("-b".to_string());
        args.push(dir.path().to_string_lossy().into_owned());
        Some(dir)
    } else {
        None
    };

    let mut last_err = String::new();
    for _ in 0..SPAWN_ATTEMPTS {
        let port = claim_free_port()
            .map_err(|e| SpawnError(format!("could not find a free port: {e}")))?;
        match start_on_port(bin, port, &args) {
            Ok(child) => {
                return Ok(ServerProcess {
                    child,
                    addr: ([127, 0, 0, 1], port).into(),
                    bin: bin.to_path_buf(),
                    args,
                    binlog_dir,
                });
            }
            Err(e) => {
                release_port(port);
                last_err = e.0;
            }
        }
    }

    Err(SpawnError(format!(
        "server {} failed to start after {SPAWN_ATTEMPTS} attempts: {last_err}",
        bin.display()
    )))
}

/// Start `bin` on `port` (already claimed by the caller) and wait until it
/// accepts connections. On failure the child is killed and reaped; the
/// port stays claimed.
fn start_on_port(bin: &Path, port: u16, args: &[String]) -> Result<Child, SpawnError> {
    let addr: SocketAddr = ([127, 0, 0, 1], port).into();
    let mut cmd = Command::new(bin);
    cmd.arg("-l")
        .arg("127.0.0.1")
        .arg("-p")
        .arg(port.to_string())
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = cmd
        .spawn()
        .map_err(|e| SpawnError(format!("failed to spawn {}: {e}", bin.display())))?;

    match wait_until_accepting(&mut child, addr, STARTUP_TIMEOUT) {
        Ok(()) => Ok(child),
        Err(e) => {
            let _ = child.kill();
            let _ = child.wait();
            Err(SpawnError(e))
        }
    }
}

fn wait_until_accepting(
    child: &mut Child,
    addr: SocketAddr,
    timeout: Duration,
) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait().map_err(|e| e.to_string())? {
            let stderr = read_all(child.stderr.take());
            let stdout = read_all(child.stdout.take());
            return Err(format!(
                "server exited early with {status}; stderr={stderr:?} stdout={stdout:?}"
            ));
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if probe_ready(addr, remaining.max(Duration::from_millis(500))) {
            // The probe also sees EOF when the process died while it was
            // waiting (e.g. binlog lock held elsewhere); re-check.
            std::thread::sleep(Duration::from_millis(5));
            if child.try_wait().map_err(|e| e.to_string())?.is_none() {
                return Ok(());
            }
            continue;
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "server did not accept connections within {timeout:?}"
            ));
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Check readiness with a full request/response round trip (`quit`), not a
/// bare connect-then-drop: under heavy parallel load, a connection that is
/// accepted at the TCP level but abandoned by the client before the server
/// reads or writes anything can otherwise leave the freshly-spawned
/// process's event loop in a state where it stops accepting further
/// connections, causing every real client of that process to hang. Doing
/// one real command exchange exercises the same code path a real client
/// will use and avoids that class of startup race entirely.
///
/// Once the TCP connect succeeds, the probe waits up to `read_timeout` (the
/// rest of the startup budget) for the reply instead of giving up early and
/// retrying: the server may accept at the kernel level before it serves
/// (the reference binds its socket before replaying / preallocating the
/// binlog, which can take a while under load), and every retried probe
/// would count as one more `total-connections` on that side only.
fn probe_ready(addr: SocketAddr, read_timeout: Duration) -> bool {
    use std::io::{Read, Write};
    let Ok(mut stream) = TcpStream::connect_timeout(&addr, Duration::from_millis(200)) else {
        return false;
    };
    if stream.set_read_timeout(Some(read_timeout)).is_err() {
        return false;
    }
    if stream.write_all(b"quit\r\n").is_err() {
        return false;
    }
    // "quit" has no reply; a clean EOF (or simply not erroring) confirms the
    // server processed our command rather than merely accepting the socket.
    let mut buf = [0u8; 16];
    stream.read(&mut buf).is_ok()
}

fn read_all(pipe: Option<impl std::io::Read>) -> String {
    let Some(mut pipe) = pipe else {
        return String::new();
    };
    let mut buf = String::new();
    let _ = pipe.read_to_string(&mut buf);
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binlog_dir_is_created_empty_and_removed_on_drop() {
        let dir = BinlogDir::create().expect("create");
        let path = dir.path().to_path_buf();
        assert!(path.is_dir());
        std::fs::write(path.join("binlog.1"), b"x").expect("write");
        let other = BinlogDir::create().expect("create");
        assert_ne!(other.path(), path);
        drop(dir);
        assert!(!path.exists());
    }

    /// Restart keeps the binlog directory and (normally) the port, and the
    /// directory is removed when the server is dropped. Uses the reference
    /// binary, which must exist (built by scripts/build-ref.sh).
    #[test]
    fn restart_keeps_binlog_dir_and_drop_removes_it() {
        let bin = crate::runner::default_ref_bin();
        assert!(bin.exists(), "reference binary missing: {}", bin.display());
        let mut server = spawn(&ServerConfig {
            bin: &bin,
            extra_args: &[],
            binlog: true,
        })
        .expect("spawn");
        let dir = server.binlog_dir().expect("binlog dir").to_path_buf();
        assert!(dir.is_dir());
        let pid = server.pid();
        for how in [StopMode::Term, StopMode::Kill] {
            server.restart(how, Duration::ZERO).expect("restart");
            assert_eq!(server.binlog_dir(), Some(dir.as_path()));
            assert!(probe_ready(server.addr(), Duration::from_secs(2)));
        }
        assert_ne!(server.pid(), pid);
        drop(server);
        assert!(!dir.exists());
    }
}
