//! A minimal beanstalkd protocol client, just enough for the load
//! generator: buffered command writes (for pipelining) and strict reply
//! parsing. Any reply the caller did not expect is an error.

use std::fmt;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

/// Every reply must arrive within this long, or the run fails as a hang.
pub const REPLY_TIMEOUT: Duration = Duration::from_secs(10);

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

/// A parsed server reply line (plus body, where the reply carries one).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reply {
    Inserted(u64),
    Reserved {
        id: u64,
        body_len: usize,
    },
    Deleted,
    TimedOut,
    Using,
    Watching(u32),
    /// `OK <n>` followed by `n` bytes of YAML (read by [`Client::read_reply`]).
    Ok(usize),
    /// Anything else (errors such as `NOT_FOUND`, `DRAINING`, ...).
    Other(String),
}

/// A reply that did not match what the scenario expected.
#[derive(Debug)]
pub struct Unexpected {
    pub op: &'static str,
    pub reply: Reply,
}

impl fmt::Display for Unexpected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unexpected reply to {}: {:?}", self.op, self.reply)
    }
}

impl std::error::Error for Unexpected {}

pub fn unexpected(op: &'static str, reply: Reply) -> Error {
    Box::new(Unexpected { op, reply })
}

/// Parses one reply line (without the trailing `\r\n`).
pub fn parse_reply_line(line: &[u8]) -> Reply {
    let s = String::from_utf8_lossy(line);
    let mut parts = s.split(' ');
    let head = parts.next().unwrap_or("");
    let a1 = parts.next();
    let a2 = parts.next();
    let extra = parts.next().is_some();
    let other = || Reply::Other(s.to_string());
    match (head, a1, a2, extra) {
        ("INSERTED", Some(id), None, false) => id.parse().map_or_else(|_| other(), Reply::Inserted),
        ("RESERVED", Some(id), Some(n), false) => match (id.parse(), n.parse()) {
            (Ok(id), Ok(body_len)) => Reply::Reserved { id, body_len },
            _ => other(),
        },
        ("DELETED", None, None, false) => Reply::Deleted,
        ("TIMED_OUT", None, None, false) => Reply::TimedOut,
        ("USING", Some(_), None, false) => Reply::Using,
        ("WATCHING", Some(n), None, false) => n.parse().map_or_else(|_| other(), Reply::Watching),
        ("OK", Some(n), None, false) => n.parse().map_or_else(|_| other(), Reply::Ok),
        _ => other(),
    }
}

/// Extracts `key: <integer>` from a stats YAML body.
pub fn stats_u64(yaml: &str, key: &str) -> Option<u64> {
    stats_str(yaml, key).and_then(|v| v.parse().ok())
}

/// Extracts `key: <float>` from a stats YAML body.
pub fn stats_f64(yaml: &str, key: &str) -> Option<f64> {
    stats_str(yaml, key).and_then(|v| v.parse().ok())
}

fn stats_str<'a>(yaml: &'a str, key: &str) -> Option<&'a str> {
    yaml.lines().find_map(|l| {
        let (k, v) = l.split_once(": ")?;
        (k == key).then_some(v.trim())
    })
}

pub struct Client {
    rd: BufReader<OwnedReadHalf>,
    wr: OwnedWriteHalf,
    wbuf: Vec<u8>,
    line: Vec<u8>,
    /// Body of the last `RESERVED` / `OK` reply.
    pub body: Vec<u8>,
}

impl Client {
    pub async fn connect(addr: &str) -> Result<Self> {
        let stream = tokio::time::timeout(REPLY_TIMEOUT, TcpStream::connect(addr))
            .await
            .map_err(|_| format!("connect to {addr} timed out"))??;
        stream.set_nodelay(true)?;
        let (rd, wr) = stream.into_split();
        Ok(Self {
            rd: BufReader::with_capacity(64 * 1024, rd),
            wr,
            wbuf: Vec::with_capacity(64 * 1024),
            line: Vec::with_capacity(128),
            body: Vec::new(),
        })
    }

    /// Queues a raw command line (the caller omits the `\r\n`).
    pub fn queue(&mut self, cmd: &str) {
        self.wbuf.extend_from_slice(cmd.as_bytes());
        self.wbuf.extend_from_slice(b"\r\n");
    }

    pub fn queue_put(&mut self, pri: u32, delay: u32, ttr: u32, body: &[u8]) {
        self.queue(&format!("put {pri} {delay} {ttr} {}", body.len()));
        self.wbuf.extend_from_slice(body);
        self.wbuf.extend_from_slice(b"\r\n");
    }

    /// Writes all queued commands.
    pub async fn flush(&mut self) -> Result<()> {
        if !self.wbuf.is_empty() {
            tokio::time::timeout(REPLY_TIMEOUT, self.wr.write_all(&self.wbuf))
                .await
                .map_err(|_| "write timed out")??;
            self.wbuf.clear();
        }
        Ok(())
    }

    /// Reads one reply; for `RESERVED` and `OK` also reads the body into
    /// `self.body`.
    pub async fn read_reply(&mut self) -> Result<Reply> {
        tokio::time::timeout(REPLY_TIMEOUT, self.read_reply_inner())
            .await
            .map_err(|_| format!("no reply within {REPLY_TIMEOUT:?} (server hang?)"))?
    }

    async fn read_reply_inner(&mut self) -> Result<Reply> {
        self.line.clear();
        let n = self.rd.read_until(b'\n', &mut self.line).await?;
        if n == 0 {
            return Err("server closed the connection".into());
        }
        let Some(line) = self.line.strip_suffix(b"\r\n") else {
            return Err(format!(
                "malformed reply line: {:?}",
                String::from_utf8_lossy(&self.line)
            )
            .into());
        };
        let reply = parse_reply_line(line);
        let body_len = match reply {
            Reply::Reserved { body_len, .. } => Some(body_len),
            Reply::Ok(n) => Some(n),
            _ => None,
        };
        if let Some(len) = body_len {
            self.body.resize(len + 2, 0);
            self.rd.read_exact(&mut self.body).await?;
            if !self.body.ends_with(b"\r\n") {
                return Err("reply body not terminated by CRLF".into());
            }
            self.body.truncate(len);
        }
        Ok(reply)
    }

    /// Sends one command and returns its reply.
    pub async fn call(&mut self, cmd: &str) -> Result<Reply> {
        self.queue(cmd);
        self.flush().await?;
        self.read_reply().await
    }

    /// Makes `tube` the only used and watched tube.
    pub async fn use_and_watch_only(&mut self, tube: &str) -> Result<()> {
        match self.call(&format!("use {tube}")).await? {
            Reply::Using => {}
            r => return Err(unexpected("use", r)),
        }
        match self.call(&format!("watch {tube}")).await? {
            Reply::Watching(_) => {}
            r => return Err(unexpected("watch", r)),
        }
        if tube != "default" {
            match self.call("ignore default").await? {
                Reply::Watching(1) => {}
                r => return Err(unexpected("ignore", r)),
            }
        }
        Ok(())
    }

    /// Runs `stats` and returns the YAML body.
    pub async fn stats(&mut self) -> Result<String> {
        match self.call("stats").await? {
            Reply::Ok(_) => Ok(String::from_utf8_lossy(&self.body).into_owned()),
            r => Err(unexpected("stats", r)),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn parses_replies() {
        assert_eq!(parse_reply_line(b"INSERTED 42"), Reply::Inserted(42));
        assert_eq!(
            parse_reply_line(b"RESERVED 7 16"),
            Reply::Reserved {
                id: 7,
                body_len: 16
            }
        );
        assert_eq!(parse_reply_line(b"DELETED"), Reply::Deleted);
        assert_eq!(parse_reply_line(b"TIMED_OUT"), Reply::TimedOut);
        assert_eq!(parse_reply_line(b"USING foo"), Reply::Using);
        assert_eq!(parse_reply_line(b"WATCHING 2"), Reply::Watching(2));
        assert_eq!(parse_reply_line(b"OK 900"), Reply::Ok(900));
    }

    #[test]
    fn malformed_replies_are_other() {
        for line in [
            &b"INSERTED"[..],
            b"INSERTED x",
            b"INSERTED 1 2",
            b"DELETED 1",
            b"RESERVED 1",
            b"NOT_FOUND",
            b"",
        ] {
            assert!(
                matches!(parse_reply_line(line), Reply::Other(_)),
                "{:?}",
                String::from_utf8_lossy(line)
            );
        }
    }

    #[test]
    fn parses_stats_yaml() {
        let yaml = "---\ncurrent-jobs-ready: 3\ncurrent-jobs-reserved: 0\nrusage-utime: 1.250000\nid: abc\n";
        assert_eq!(stats_u64(yaml, "current-jobs-ready"), Some(3));
        assert_eq!(stats_u64(yaml, "current-jobs-reserved"), Some(0));
        assert_eq!(stats_f64(yaml, "rusage-utime"), Some(1.25));
        assert_eq!(stats_u64(yaml, "id"), None);
        assert_eq!(stats_u64(yaml, "missing"), None);
        assert_eq!(stats_u64(yaml, "current-jobs"), None);
    }
}
