//! A small beanstalk protocol client (tokio) for the multi-process harness.

use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

use crate::history::{Cmd, Reply};

pub struct BsClient {
    r: BufReader<OwnedReadHalf>,
    w: OwnedWriteHalf,
}

fn bad(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

impl BsClient {
    pub async fn connect(addr: SocketAddr, timeout: Duration) -> io::Result<BsClient> {
        let s = tokio::time::timeout(timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "connect timed out"))??;
        s.set_nodelay(true)?;
        let (r, w) = s.into_split();
        Ok(BsClient {
            r: BufReader::new(r),
            w,
        })
    }

    /// Sends `cmd` (it must be the only command in flight).
    pub async fn send(&mut self, cmd: &Cmd) -> io::Result<()> {
        let mut out = cmd.line().into_bytes();
        out.extend_from_slice(b"\r\n");
        if let Cmd::Put { body, .. } = cmd {
            out.extend_from_slice(body);
            out.extend_from_slice(b"\r\n");
        }
        self.w.write_all(&out).await
    }

    /// Sends an arbitrary line (no history).
    pub async fn raw(&mut self, line: &str) -> io::Result<(String, Vec<u8>)> {
        self.w.write_all(format!("{line}\r\n").as_bytes()).await?;
        let l = self.line().await?;
        let body = match l.split(' ').collect::<Vec<_>>().as_slice() {
            ["OK", n] => self.body(n).await?,
            _ => Vec::new(),
        };
        Ok((l, body))
    }

    async fn line(&mut self) -> io::Result<String> {
        let mut buf = Vec::new();
        let n = self.r.read_until(b'\n', &mut buf).await?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed",
            ));
        }
        if !buf.ends_with(b"\r\n") {
            return Err(bad(format!("unterminated reply {buf:?}")));
        }
        buf.truncate(buf.len() - 2);
        String::from_utf8(buf).map_err(|e| bad(e.to_string()))
    }

    async fn body(&mut self, n: &str) -> io::Result<Vec<u8>> {
        let n: usize = n.parse().map_err(|_| bad(format!("bad length {n}")))?;
        let mut b = vec![0u8; n + 2];
        self.r.read_exact(&mut b).await?;
        if !b.ends_with(b"\r\n") {
            return Err(bad("body not terminated".into()));
        }
        b.truncate(n);
        Ok(b)
    }

    /// Reads the reply to `cmd`.
    pub async fn recv(&mut self, cmd: &Cmd) -> io::Result<Reply> {
        let line = self.line().await?;
        let parts: Vec<&str> = line.split(' ').collect();
        let id = |s: &str| {
            s.parse::<u64>()
                .map_err(|_| bad(format!("bad id in {line:?}")))
        };
        Ok(match parts.as_slice() {
            ["INSERTED", i] => Reply::Inserted(id(i)?),
            ["BURIED", i] => Reply::BuriedId(id(i)?),
            ["BURIED"] => Reply::Buried,
            ["RESERVED", i, n] => {
                let body = self.body(n).await?;
                Reply::Reserved { id: id(i)?, body }
            }
            ["FOUND", i, n] => {
                let body = self.body(n).await?;
                Reply::Found { id: id(i)?, body }
            }
            ["OK", n] => {
                let body = self.body(n).await?;
                if matches!(cmd, Cmd::StatsJob(_)) {
                    Reply::from_stats_yaml(&body)
                } else {
                    Reply::Other(line.clone())
                }
            }
            ["KICKED", n] => Reply::Kicked(id(n)?),
            ["KICKED"] => Reply::KickedJob,
            ["DELETED"] => Reply::Deleted,
            ["RELEASED"] => Reply::Released,
            ["TOUCHED"] => Reply::Touched,
            ["NOT_FOUND"] => Reply::NotFound,
            ["TIMED_OUT"] => Reply::TimedOut,
            ["DEADLINE_SOON"] => Reply::DeadlineSoon,
            _ => Reply::Other(line.clone()),
        })
    }

    pub async fn call(&mut self, cmd: &Cmd) -> io::Result<Reply> {
        self.send(cmd).await?;
        self.recv(cmd).await
    }
}

/// `GET path` over HTTP/1.1 (`Connection: close`): status and body.
pub async fn http_get(addr: SocketAddr, path: &str, timeout: Duration) -> Option<(u16, String)> {
    let fut = async {
        let mut s = TcpStream::connect(addr).await.ok()?;
        let req = format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n");
        s.write_all(req.as_bytes()).await.ok()?;
        let mut buf = Vec::new();
        s.read_to_end(&mut buf).await.ok()?;
        let text = String::from_utf8_lossy(&buf).into_owned();
        let status = text.split(' ').nth(1)?.parse().ok()?;
        let body = text.split_once("\r\n\r\n").map(|(_, b)| b.to_string())?;
        Some((status, body))
    };
    tokio::time::timeout(timeout, fut).await.ok().flatten()
}
