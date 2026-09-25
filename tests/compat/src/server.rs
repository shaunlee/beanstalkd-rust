//! Spawning and lifecycle management of a server binary under test.

use std::collections::HashSet;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

const STARTUP_TIMEOUT: Duration = Duration::from_secs(5);
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

/// A running server process, spawned on a free `127.0.0.1` port. Killed and
/// reaped when dropped, so a panicking test never leaves an orphan process
/// behind. Its port is released from [`CLAIMED_PORTS`] on drop as well.
pub struct ServerProcess {
    child: Child,
    addr: SocketAddr,
}

impl ServerProcess {
    pub fn addr(&self) -> SocketAddr {
        self.addr
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
    if !bin.exists() {
        return Err(SpawnError(format!(
            "server binary not found: {}",
            bin.display()
        )));
    }

    let mut last_err = String::new();
    for _ in 0..SPAWN_ATTEMPTS {
        let port = claim_free_port()
            .map_err(|e| SpawnError(format!("could not find a free port: {e}")))?;
        let addr: SocketAddr = ([127, 0, 0, 1], port).into();

        let mut cmd = Command::new(bin);
        cmd.arg("-l")
            .arg("127.0.0.1")
            .arg("-p")
            .arg(port.to_string())
            .args(extra_args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let mut child = match cmd.spawn() {
            Ok(c) => c,
            Err(e) => {
                release_port(port);
                return Err(SpawnError(format!(
                    "failed to spawn {}: {e}",
                    bin.display()
                )));
            }
        };

        match wait_until_accepting(&mut child, addr, STARTUP_TIMEOUT) {
            Ok(()) => return Ok(ServerProcess { child, addr }),
            Err(e) => {
                let _ = child.kill();
                let _ = child.wait();
                release_port(port);
                last_err = e;
            }
        }
    }

    Err(SpawnError(format!(
        "server {} failed to start after {SPAWN_ATTEMPTS} attempts: {last_err}",
        bin.display()
    )))
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
        if probe_ready(addr) {
            return Ok(());
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
fn probe_ready(addr: SocketAddr) -> bool {
    use std::io::{Read, Write};
    let Ok(mut stream) = TcpStream::connect_timeout(&addr, Duration::from_millis(200)) else {
        return false;
    };
    if stream
        .set_read_timeout(Some(Duration::from_millis(500)))
        .is_err()
    {
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
