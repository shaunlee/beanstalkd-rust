//! Helpers for the P2 (operability) tests: servers started from a
//! generated `--config` file, throwaway TLS certificates, a blocking TLS
//! client, a protocol client generic over the stream, and a minimal HTTP
//! client.

use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rcgen::{
    BasicConstraints, CertificateParams, CertifiedKey, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, ClientConnection, RootCertStore, StreamOwned};

use super::{DEFAULT_TIMEOUT, claim_free_port, find_crlf, release_port};

pub const BIN: &str = env!("CARGO_BIN_EXE_beanstalkd-rs");

/// A free port, claimed for this test binary until [`release`]d (see
/// `CLAIMED_PORTS`).
pub fn claim_port() -> u16 {
    claim_free_port()
}

pub fn release(port: u16) {
    release_port(port);
}

// ---------------------------------------------------------------------------
// Certificates
// ---------------------------------------------------------------------------

/// Which client certificate a TLS client presents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientCert {
    None,
    /// Signed by the CA the server trusts for client certificates.
    Valid,
    /// Signed by an unrelated CA.
    WrongCa,
}

/// A CA, a server certificate for `localhost` / `127.0.0.1` signed by it,
/// client certificates signed by it and by a second, untrusted CA. Written
/// as `ca.pem`, `server.pem`, `server.key` into `dir`.
pub struct Certs {
    ca_der: CertificateDer<'static>,
    valid_client: (Vec<CertificateDer<'static>>, String),
    wrong_client: (Vec<CertificateDer<'static>>, String),
}

fn ca(name: &str) -> (rcgen::Certificate, KeyPair) {
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.distinguished_name.push(DnType::CommonName, name);
    params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    let key = KeyPair::generate().unwrap();
    let cert = params.self_signed(&key).unwrap();
    (cert, key)
}

fn leaf(
    names: Vec<String>,
    usage: ExtendedKeyUsagePurpose,
    ca: &rcgen::Certificate,
    ca_key: &KeyPair,
) -> CertifiedKey {
    let mut params = CertificateParams::new(names).unwrap();
    params
        .distinguished_name
        .push(DnType::CommonName, "bstk test leaf");
    params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyEncipherment,
    ];
    params.extended_key_usages = vec![usage];
    let key_pair = KeyPair::generate().unwrap();
    let cert = params.signed_by(&key_pair, ca, ca_key).unwrap();
    CertifiedKey { cert, key_pair }
}

impl Certs {
    pub fn generate(dir: &Path) -> Certs {
        let (ca_cert, ca_key) = ca("bstk test CA");
        let (other_ca, other_key) = ca("bstk untrusted CA");
        let server = leaf(
            vec!["localhost".to_owned(), "127.0.0.1".to_owned()],
            ExtendedKeyUsagePurpose::ServerAuth,
            &ca_cert,
            &ca_key,
        );
        let good = leaf(
            vec!["client".to_owned()],
            ExtendedKeyUsagePurpose::ClientAuth,
            &ca_cert,
            &ca_key,
        );
        let bad = leaf(
            vec!["client".to_owned()],
            ExtendedKeyUsagePurpose::ClientAuth,
            &other_ca,
            &other_key,
        );
        std::fs::write(dir.join("ca.pem"), ca_cert.pem()).unwrap();
        std::fs::write(dir.join("server.pem"), server.cert.pem()).unwrap();
        std::fs::write(dir.join("server.key"), server.key_pair.serialize_pem()).unwrap();
        Certs {
            ca_der: ca_cert.der().clone(),
            valid_client: (vec![good.cert.der().clone()], good.key_pair.serialize_pem()),
            wrong_client: (vec![bad.cert.der().clone()], bad.key_pair.serialize_pem()),
        }
    }

    pub fn client_config(&self, cert: ClientCert) -> Arc<ClientConfig> {
        let mut roots = RootCertStore::empty();
        roots.add(self.ca_der.clone()).unwrap();
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let builder = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots);
        let identity = |(chain, key): &(Vec<CertificateDer<'static>>, String)| {
            use rustls::pki_types::pem::PemObject;
            (
                chain.clone(),
                PrivateKeyDer::from_pem_slice(key.as_bytes()).unwrap(),
            )
        };
        let config = match cert {
            ClientCert::None => builder.with_no_client_auth(),
            ClientCert::Valid => {
                let (chain, key) = identity(&self.valid_client);
                builder.with_client_auth_cert(chain, key).unwrap()
            }
            ClientCert::WrongCa => {
                let (chain, key) = identity(&self.wrong_client);
                builder.with_client_auth_cert(chain, key).unwrap()
            }
        };
        Arc::new(config)
    }
}

// ---------------------------------------------------------------------------
// Servers started from a configuration file
// ---------------------------------------------------------------------------

/// A `beanstalkd-rs` started with `--config`, killed on drop. Its stderr
/// goes to a file (a pipe could fill up at trace level).
pub struct ConfigServer {
    pub child: Child,
    pub dir: tempfile::TempDir,
    /// Ports substituted for `{port0}`, `{port1}`, ... in the template.
    pub ports: Vec<u16>,
    pub log: PathBuf,
}

impl ConfigServer {
    /// Writes `template` (with `{portN}` replaced by free ports) as
    /// `config.toml` into a fresh directory (with the test certificates,
    /// see [`Certs`]) and starts the server with `--config` plus `args`.
    /// Waits until every port accepts TCP connections.
    pub fn start(template: &str, nports: usize, args: &[&str]) -> (ConfigServer, Certs) {
        let dir = tempfile::tempdir().unwrap();
        let certs = Certs::generate(dir.path());
        (Self::start_in(dir, template, nports, args), certs)
    }

    pub fn start_in(
        dir: tempfile::TempDir,
        template: &str,
        nports: usize,
        args: &[&str],
    ) -> ConfigServer {
        let ports: Vec<u16> = (0..nports).map(|_| claim_free_port()).collect();
        let mut text = template.to_owned();
        for (i, p) in ports.iter().enumerate() {
            text = text.replace(&format!("{{port{i}}}"), &p.to_string());
        }
        let config = dir.path().join("config.toml");
        std::fs::write(&config, text).unwrap();
        let log = dir.path().join("stderr.log");
        let child = Command::new(BIN)
            .arg("--config")
            .arg(&config)
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(std::fs::File::create(&log).unwrap())
            .spawn()
            .unwrap();
        let mut server = ConfigServer {
            child,
            dir,
            ports,
            log,
        };
        server.wait_until_listening();
        server
    }

    fn wait_until_listening(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(20);
        for &port in &self.ports {
            loop {
                if let Some(status) = self.child.try_wait().unwrap() {
                    panic!("server exited early with {status}: {}", self.stderr());
                }
                let addr: SocketAddr = ([127, 0, 0, 1], port).into();
                if TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_ok() {
                    break;
                }
                assert!(Instant::now() < deadline, "port {port} never listened");
                std::thread::sleep(Duration::from_millis(20));
            }
        }
    }

    pub fn addr(&self, i: usize) -> SocketAddr {
        ([127, 0, 0, 1], self.ports[i]).into()
    }

    /// A plaintext protocol connection to port `i`.
    pub fn plain(&self, i: usize) -> Proto<TcpStream> {
        let s = TcpStream::connect_timeout(&self.addr(i), Duration::from_secs(2)).unwrap();
        s.set_read_timeout(Some(DEFAULT_TIMEOUT)).unwrap();
        Proto::new(s)
    }

    /// A TLS protocol connection to port `i` (handshake completed).
    pub fn tls(&self, i: usize, config: Arc<ClientConfig>) -> Proto<TlsStream> {
        tls_connect(self.addr(i), config).expect("TLS handshake")
    }

    /// Everything written to stderr so far.
    pub fn stderr(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }

    /// Sends `sig` and waits up to 10 s for the process to exit.
    pub fn stop(&mut self, sig: nix::sys::signal::Signal) -> ExitStatus {
        let pid = nix::unistd::Pid::from_raw(i32::try_from(self.child.id()).unwrap());
        nix::sys::signal::kill(pid, sig).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                return status;
            }
            assert!(Instant::now() < deadline, "server did not exit after {sig}");
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

impl Drop for ConfigServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if std::thread::panicking() {
            eprintln!("--- server stderr ---\n{}", self.stderr());
        }
        for &p in &self.ports {
            release_port(p);
        }
    }
}

/// Runs the binary with `args` and waits for it to exit on its own.
pub fn run(args: &[&str]) -> (ExitStatus, String, String) {
    let out = Command::new(BIN)
        .args(args)
        .stdin(Stdio::null())
        .output()
        .unwrap();
    (
        out.status,
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

// ---------------------------------------------------------------------------
// Clients
// ---------------------------------------------------------------------------

pub type TlsStream = StreamOwned<ClientConnection, TcpStream>;

/// Connects and completes the TLS handshake (as far as the client can
/// tell: with TLS 1.3 a rejected client certificate only shows up on the
/// first read).
pub fn tls_connect(addr: SocketAddr, config: Arc<ClientConfig>) -> io::Result<Proto<TlsStream>> {
    let mut sock = TcpStream::connect_timeout(&addr, Duration::from_secs(2))?;
    sock.set_read_timeout(Some(DEFAULT_TIMEOUT))?;
    sock.set_nodelay(true)?;
    let name = ServerName::try_from("localhost").unwrap();
    let mut conn = ClientConnection::new(config, name).map_err(io::Error::other)?;
    while conn.is_handshaking() {
        conn.complete_io(&mut sock)?;
    }
    Ok(Proto::new(StreamOwned::new(conn, sock)))
}

/// A protocol client over any byte stream, with its own buffer.
pub struct Proto<S> {
    pub stream: S,
    buf: Vec<u8>,
}

/// How reading until the peer closes ended.
#[derive(Debug, PartialEq, Eq)]
pub enum End {
    /// EOF (or a TLS end of stream, with or without close_notify).
    Closed,
    /// A connection reset or another error; carries its text.
    Error(String),
    /// Nothing more within the timeout, and still open.
    Timeout,
}

impl<S: Read + Write> Proto<S> {
    pub fn new(stream: S) -> Self {
        Proto {
            stream,
            buf: Vec::new(),
        }
    }

    pub fn send(&mut self, data: &[u8]) {
        self.stream.write_all(data).expect("write");
        self.stream.flush().expect("flush");
    }

    /// Reads one line (CRLF included); panics on EOF or an error.
    pub fn read_line(&mut self) -> String {
        loop {
            if let Some(pos) = find_crlf(&self.buf) {
                let line: Vec<u8> = self.buf.drain(..pos + 2).collect();
                return String::from_utf8_lossy(&line).into_owned();
            }
            let mut chunk = [0u8; 4096];
            let n = self.stream.read(&mut chunk).expect("read");
            assert!(n > 0, "connection closed unexpectedly");
            self.buf.extend_from_slice(&chunk[..n]);
        }
    }

    fn read_n(&mut self, n: usize) -> Vec<u8> {
        while self.buf.len() < n {
            let mut chunk = [0u8; 4096];
            let got = self.stream.read(&mut chunk).expect("read");
            assert!(got > 0, "connection closed unexpectedly");
            self.buf.extend_from_slice(&chunk[..got]);
        }
        self.buf.drain(..n).collect()
    }

    /// Sends `line` + CRLF and returns the one-line reply without CRLF.
    pub fn cmd(&mut self, line: &str) -> String {
        self.send(format!("{line}\r\n").as_bytes());
        self.read_line().trim_end_matches("\r\n").to_owned()
    }

    pub fn put(&mut self, body: &[u8]) -> String {
        let mut msg = format!("put 0 0 60 {}\r\n", body.len()).into_bytes();
        msg.extend_from_slice(body);
        msg.extend_from_slice(b"\r\n");
        self.send(&msg);
        self.read_line().trim_end_matches("\r\n").to_owned()
    }

    /// A reply with a body (`OK <n>` / `RESERVED <id> <n>`): (header, body).
    pub fn body_reply(&mut self, line: &str) -> (String, Vec<u8>) {
        self.send(format!("{line}\r\n").as_bytes());
        let header = self.read_line();
        let n: usize = header
            .trim_end_matches("\r\n")
            .rsplit(' ')
            .next()
            .unwrap()
            .parse()
            .unwrap_or_else(|e| panic!("{line}: {header:?}: {e}"));
        let mut body = self.read_n(n + 2);
        body.truncate(n);
        (header.trim_end_matches("\r\n").to_owned(), body)
    }

    pub fn yaml(&mut self, line: &str) -> String {
        let (header, body) = self.body_reply(line);
        assert!(header.starts_with("OK "), "{line}: {header:?}");
        String::from_utf8(body).unwrap()
    }

    pub fn stat(&mut self, line: &str, key: &str) -> String {
        stat_of(&self.yaml(line), key)
    }

    /// Reads everything until the peer closes (or `timeout` passes).
    pub fn read_to_end(&mut self, timeout: Duration) -> (Vec<u8>, End) {
        let deadline = Instant::now() + timeout;
        let mut out = std::mem::take(&mut self.buf);
        loop {
            if Instant::now() >= deadline {
                return (out, End::Timeout);
            }
            let mut chunk = [0u8; 4096];
            match self.stream.read(&mut chunk) {
                Ok(0) => return (out, End::Closed),
                Ok(n) => out.extend_from_slice(&chunk[..n]),
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return (out, End::Closed),
                Err(e)
                    if e.kind() == io::ErrorKind::WouldBlock
                        || e.kind() == io::ErrorKind::TimedOut => {}
                Err(e) => return (out, End::Error(e.to_string())),
            }
        }
    }
}

/// The value of `key` in a YAML stats body.
pub fn stat_of(yaml: &str, key: &str) -> String {
    yaml.lines()
        .find_map(|l| l.strip_prefix(&format!("{key}: ")))
        .unwrap_or_else(|| panic!("{key} missing from {yaml}"))
        .trim()
        .to_owned()
}

// ---------------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------------

pub struct HttpResponse {
    pub status: u16,
    /// Header lines, lowercased names (`name: value`).
    pub headers: Vec<(String, String)>,
    pub body: String,
}

impl HttpResponse {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    }
}

/// Sends a raw HTTP/1.1 request and reads the response until the server
/// closes the connection (it never keeps connections alive).
pub fn http(addr: SocketAddr, method: &str, path: &str, extra: &str) -> io::Result<HttpResponse> {
    let mut s = TcpStream::connect_timeout(&addr, Duration::from_secs(2))?;
    s.set_read_timeout(Some(Duration::from_secs(5)))?;
    write!(
        s,
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\n{extra}\r\n"
    )?;
    let mut raw = Vec::new();
    s.read_to_end(&mut raw)?;
    let text = String::from_utf8_lossy(&raw).into_owned();
    let (head, body) = text
        .split_once("\r\n\r\n")
        .ok_or_else(|| io::Error::other(format!("no header end in {text:?}")))?;
    let mut lines = head.split("\r\n");
    let status_line = lines.next().unwrap_or_default();
    let status = status_line
        .split(' ')
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| io::Error::other(format!("bad status line {status_line:?}")))?;
    let headers = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(n, v)| (n.trim().to_ascii_lowercase(), v.trim().to_owned()))
        .collect();
    Ok(HttpResponse {
        status,
        headers,
        body: body.to_owned(),
    })
}

pub fn get(addr: SocketAddr, path: &str) -> HttpResponse {
    http(addr, "GET", path, "").unwrap_or_else(|e| panic!("GET {path}: {e}"))
}
