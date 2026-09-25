//! Durability torture and compaction churn tests for `-b` (docs/PLAN.md
//! §4.4 "Durability" and "Compaction").
//!
//! # Durability torture
//!
//! For each fsync mode (`-F`, the default `-f 50`, `-f0`), many rounds of:
//! several concurrent clients run random put / delete / reserve-job + bury /
//! reserve-job + release (with and without a delay) / kick-job on random
//! tubes with random bodies, recording every acknowledged outcome; the
//! server is SIGKILLed at a random moment; it is restarted on the same
//! directory and the recovered state is checked against the model:
//!
//! - every job whose `INSERTED` was acknowledged and whose `DELETED` was not
//!   exists with the right tube, body and pri;
//! - every acknowledged `DELETED` job is gone;
//! - the state (ready / buried / delayed) and pri are those of the last
//!   acknowledged *journaled* change (docs/COMPAT.md, Binlog §1: reserve and
//!   `release <id> <pri> 0` are not journaled, so they are lost on restart);
//!   a delayed job is delayed or ready according to its deadline (either is
//!   accepted within `DELAY_MARGIN_MS` of it);
//! - a change whose reply was not received may or may not have happened;
//!   a put whose reply was not received may exist only with its exact tube,
//!   body and pri;
//! - the server holds no other job (`stats` totals).
//!
//! Rounds continue on the same directory, alternating `-s 65536` and
//! `-s 262144`, so rollover, compaction and segment deletion all happen.
//! Every client owns the jobs it works on (it put them, or it inherited
//! them by id after a restart), so at most one change per job is ever
//! unacknowledged.
//!
//! The always-on test runs a few rounds per mode. The full run (≥ 100
//! rounds per mode) is ignored by default:
//!
//! ```sh
//! cargo test --release -p bstk-server --test durability -- --ignored --nocapture durability_torture_full
//! ```
//!
//! Environment: `BSTK_DURABILITY_ROUNDS` (rounds per mode, default 100 for
//! the full run), `BSTK_DURABILITY_MODES` (comma-separated subset of
//! `F,default,f0`), `BSTK_DURABILITY_SEED` (reproduce a run; the seed is
//! printed).
//!
//! # Compaction churn
//!
//! With `-s 65536`: a steady set of 1,000 live jobs (ready, buried and
//! delayed, spread over several tubes) plus a long churn of put + delete
//! pairs from several pipelining connections, with a slice of the steady
//! set replaced as the churn goes. A sampler thread records the total size
//! of the `binlog.*` files; it must stay bounded. Then the server is
//! stopped with SIGTERM, restarted and the live set checked exactly (every
//! job's state, pri, tube and body, and the `stats` totals); more churn
//! runs; the server is SIGKILLed, restarted and checked again.
//!
//! ```sh
//! cargo test --release -p bstk-server --test durability -- --ignored --nocapture compaction_churn_full
//! ```
//!
//! `BSTK_CHURN_OPS` sets the number of put + delete pairs of the first
//! churn phase (default 1,000,000 for the full run).

#![allow(clippy::unwrap_used)]

mod common;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use nix::sys::signal::Signal;

use common::Server;

// ---------------------------------------------------------------------
// Small utilities
// ---------------------------------------------------------------------

/// xorshift64*: deterministic, dependency-free and good enough here.
#[derive(Clone)]
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Rng {
        Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    /// Uniform in `0..n` (`n > 0`).
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.below(hi - lo + 1)
    }
    fn percent(&mut self, p: u64) -> bool {
        self.below(100) < p
    }
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn seed() -> u64 {
    env_u64(
        "BSTK_DURABILITY_SEED",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64,
    )
}

/// Wall clock in milliseconds (the server's job times are wall-anchored).
fn wall_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// Total size of the `binlog.*` files in `dir` and their count.
fn binlog_usage(dir: &Path) -> (u64, u64) {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return (0, 0);
    };
    let mut bytes = 0;
    let mut files = 0;
    for e in rd.flatten() {
        if e.file_name().to_string_lossy().starts_with("binlog.")
            && let Ok(m) = e.metadata()
        {
            bytes += m.len();
            files += 1;
        }
    }
    (bytes, files)
}

fn start(dir: &Path, extra: &[&str]) -> Server {
    let mut args = vec!["-b", dir.to_str().unwrap()];
    args.extend_from_slice(extra);
    Server::start(&args)
}

/// A connection whose I/O errors are values: the server may be killed at
/// any moment while clients are talking to it.
struct Conn {
    stream: TcpStream,
    buf: Vec<u8>,
}

impl Conn {
    fn connect(addr: SocketAddr) -> io::Result<Conn> {
        let stream = TcpStream::connect_timeout(&addr, Duration::from_secs(2))?;
        stream.set_read_timeout(Some(Duration::from_secs(10)))?;
        stream.set_nodelay(true)?;
        Ok(Conn {
            stream,
            buf: Vec::new(),
        })
    }

    fn send(&mut self, data: &[u8]) -> io::Result<()> {
        self.stream.write_all(data)
    }

    fn fill(&mut self) -> io::Result<()> {
        let mut chunk = [0u8; 16384];
        let n = self.stream.read(&mut chunk)?;
        if n == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "closed"));
        }
        self.buf.extend_from_slice(&chunk[..n]);
        Ok(())
    }

    /// One reply line without its CRLF.
    fn line(&mut self) -> io::Result<String> {
        loop {
            if let Some(p) = self.buf.windows(2).position(|w| w == b"\r\n") {
                let line: Vec<u8> = self.buf.drain(..p + 2).collect();
                return Ok(String::from_utf8_lossy(&line[..p]).into_owned());
            }
            self.fill()?;
        }
    }

    /// `n` body bytes plus the trailing CRLF (stripped).
    fn body(&mut self, n: usize) -> io::Result<Vec<u8>> {
        while self.buf.len() < n + 2 {
            self.fill()?;
        }
        let mut b: Vec<u8> = self.buf.drain(..n + 2).collect();
        b.truncate(n);
        Ok(b)
    }

    fn cmd(&mut self, line: &str) -> io::Result<String> {
        self.send(format!("{line}\r\n").as_bytes())?;
        self.line()
    }

    fn put_msg(pri: u32, delay: u32, ttr: u32, body: &[u8]) -> Vec<u8> {
        let mut msg = format!("put {pri} {delay} {ttr} {}\r\n", body.len()).into_bytes();
        msg.extend_from_slice(body);
        msg.extend_from_slice(b"\r\n");
        msg
    }

    /// Reads a reply that is either `<word> <..> <n>` + body, or one line.
    fn reply_with_body(&mut self) -> io::Result<(String, Option<Vec<u8>>)> {
        let line = self.line()?;
        if line.starts_with("OK ") || line.starts_with("FOUND ") || line.starts_with("RESERVED ") {
            let n: usize = line.rsplit(' ').next().unwrap_or("0").parse().unwrap_or(0);
            let b = self.body(n)?;
            return Ok((line, Some(b)));
        }
        Ok((line, None))
    }

    /// YAML of `line` as key -> value, or `None` on `NOT_FOUND`.
    fn yaml(&mut self, line: &str) -> io::Result<Option<HashMap<String, String>>> {
        self.send(format!("{line}\r\n").as_bytes())?;
        let (head, body) = self.reply_with_body()?;
        match body {
            Some(b) if head.starts_with("OK ") => {
                let text = String::from_utf8_lossy(&b).into_owned();
                let map = text
                    .lines()
                    .filter_map(|l| l.split_once(": "))
                    .map(|(k, v)| (k.to_owned(), v.trim().trim_matches('"').to_owned()))
                    .collect();
                Ok(Some(map))
            }
            _ if head == "NOT_FOUND" => Ok(None),
            _ => Err(io::Error::other(format!(
                "{line}: unexpected reply {head:?}"
            ))),
        }
    }

    fn peek(&mut self, id: u64) -> io::Result<Option<Vec<u8>>> {
        self.send(format!("peek {id}\r\n").as_bytes())?;
        let (head, body) = self.reply_with_body()?;
        match body {
            Some(b) if head.starts_with("FOUND ") => Ok(Some(b)),
            _ if head == "NOT_FOUND" => Ok(None),
            _ => Err(io::Error::other(format!(
                "peek {id}: unexpected reply {head:?}"
            ))),
        }
    }

    /// Sum of ready, reserved, delayed and buried jobs.
    fn total_jobs(&mut self) -> io::Result<u64> {
        let s = self.yaml("stats")?.unwrap_or_default();
        let get = |k: &str| s.get(k).and_then(|v| v.parse::<u64>().ok()).unwrap_or(0);
        Ok(get("current-jobs-ready")
            + get("current-jobs-reserved")
            + get("current-jobs-delayed")
            + get("current-jobs-buried"))
    }
}

// ---------------------------------------------------------------------
// Job model
// ---------------------------------------------------------------------

/// A job state as it is (or will be after a restart) on disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum St {
    Ready,
    Buried,
    /// Deadline between `lo` and `hi` (wall-clock ms).
    Delayed {
        lo: u64,
        hi: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Durable {
    pri: u32,
    st: St,
}

/// A job as the server holds it while it runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Live {
    Ready,
    Buried,
    /// Deadline in wall-clock ms.
    Delayed(u64),
    /// Reserved by its owner (after `reserve-job`).
    Reserved,
}

#[derive(Debug, Clone)]
struct Job {
    id: u64,
    tube: String,
    body: Vec<u8>,
    /// The last acknowledged journaled state.
    dur: Durable,
    /// An unacknowledged journaled change: `Some(None)` = maybe deleted,
    /// `Some(Some(d))` = maybe in state `d`.
    alt: Option<Option<Durable>>,
    live: Live,
    live_pri: u32,
}

impl Job {
    fn reset_live(&mut self) {
        self.live = match self.dur.st {
            St::Ready => Live::Ready,
            St::Buried => Live::Buried,
            St::Delayed { hi, .. } => Live::Delayed(hi),
        };
        self.live_pri = self.dur.pri;
    }
}

/// A put whose reply was not received.
#[derive(Debug, Clone)]
struct PendingPut {
    tube: String,
    body: Vec<u8>,
    pri: u32,
}

#[derive(Debug, Default, Clone, Copy)]
struct Counts {
    inserted: u64,
    deleted: u64,
    buried: u64,
    released_delay: u64,
    kicked: u64,
    /// Acknowledged unjournaled changes (reserve-job, release with delay 0).
    unjournaled: u64,
    /// Changes (journaled or not) whose reply never arrived.
    in_flight: u64,
}

impl Counts {
    fn acked(&self) -> u64 {
        self.inserted + self.deleted + self.buried + self.released_delay + self.kicked
    }
    fn add(&mut self, o: &Counts) {
        self.inserted += o.inserted;
        self.deleted += o.deleted;
        self.buried += o.buried;
        self.released_delay += o.released_delay;
        self.kicked += o.kicked;
        self.unjournaled += o.unjournaled;
        self.in_flight += o.in_flight;
    }
}

const TUBES: [&str; 5] = [
    "default",
    "t-a",
    "t-b",
    "t-c",
    "t-long-tube-name-for-bigger-records",
];
const TTR: u32 = 600;
/// Tolerance around a delayed job's deadline when checking ready/delayed.
const DELAY_MARGIN_MS: u64 = 1000;

struct Worker {
    idx: usize,
    rng: Rng,
    jobs: Vec<Job>,
    target: usize,
    counts: Counts,
    pending_puts: Vec<PendingPut>,
    deleted: Vec<u64>,
    violations: Vec<String>,
    seq: u64,
    round: u64,
}

/// What an operation returned: `Err` means the connection is gone (the
/// server was killed) and the operation is in flight.
type OpResult = io::Result<()>;

impl Worker {
    fn run(mut self, addr: SocketAddr, stop: Arc<AtomicBool>) -> Worker {
        let Ok(mut c) = Conn::connect(addr) else {
            return self;
        };
        let mut used = "default".to_owned();
        while !stop.load(Ordering::Relaxed) {
            if self.step(&mut c, &mut used).is_err() {
                break;
            }
        }
        self
    }

    fn violation(&mut self, msg: String) {
        self.violations.push(msg);
    }

    fn step(&mut self, c: &mut Conn, used: &mut String) -> OpResult {
        let n = self.jobs.len();
        let want_put = n == 0 || (n < self.target && self.rng.percent(45)) || self.rng.percent(10);
        if want_put {
            return self.put(c, used);
        }
        let i = self.rng.below(n as u64) as usize;
        match self.jobs[i].live {
            Live::Ready => match self.rng.below(100) {
                0..25 => self.delete(c, i),
                25..45 => self.reserve(c, i),
                45..65 => self.reserve_then(c, i, Then::Bury),
                65..85 => self.reserve_then(c, i, Then::ReleaseDelay),
                _ => self.reserve_then(c, i, Then::Release0),
            },
            Live::Buried => match self.rng.below(100) {
                0..40 => self.kick(c, i),
                40..65 => self.delete(c, i),
                65..85 => self.reserve_then(c, i, Then::ReleaseDelay),
                _ => self.reserve_then(c, i, Then::Release0),
            },
            Live::Delayed(_) => match self.rng.below(100) {
                0..40 => self.kick(c, i),
                40..70 => self.delete(c, i),
                _ => self.reserve_then(c, i, Then::Bury),
            },
            Live::Reserved => match self.rng.below(100) {
                0..30 => self.delete(c, i),
                30..55 => self.bury(c, i),
                55..80 => self.release(c, i, true),
                _ => self.release(c, i, false),
            },
        }
    }

    fn put(&mut self, c: &mut Conn, used: &mut String) -> OpResult {
        let tube = TUBES[self.rng.below(TUBES.len() as u64) as usize].to_owned();
        if *used != tube {
            let r = c.cmd(&format!("use {tube}"))?;
            if r != format!("USING {tube}") {
                self.violation(format!("use {tube}: {r:?}"));
                return Err(io::Error::other("bad use reply"));
            }
            *used = tube.clone();
        }
        self.seq += 1;
        let len = match self.rng.below(100) {
            0..70 => self.rng.range(0, 200),
            70..95 => self.rng.range(200, 2000),
            _ => self.rng.range(2000, 4096),
        } as usize;
        let mut body = format!("w{}r{}s{}:", self.idx, self.round, self.seq).into_bytes();
        while body.len() < len {
            body.push(b'a' + (self.rng.below(26) as u8));
        }
        let pri = self.rng.below(5000) as u32;
        let msg = Conn::put_msg(pri, 0, TTR, &body);
        let pending = PendingPut {
            tube: tube.clone(),
            body: body.clone(),
            pri,
        };
        let reply = c.send(&msg).and_then(|()| c.line());
        let reply = match reply {
            Ok(r) => r,
            Err(e) => {
                self.pending_puts.push(pending);
                self.counts.in_flight += 1;
                return Err(e);
            }
        };
        let Some(id) = reply.strip_prefix("INSERTED ").and_then(|s| s.parse().ok()) else {
            self.violation(format!("put: unexpected reply {reply:?}"));
            return Err(io::Error::other("bad put reply"));
        };
        self.counts.inserted += 1;
        let d = Durable { pri, st: St::Ready };
        self.jobs.push(Job {
            id,
            tube,
            body,
            dur: d,
            alt: None,
            live: Live::Ready,
            live_pri: pri,
        });
        Ok(())
    }

    /// Sends a journaled change for job `i`, recording `alt` as its
    /// possible outcome until the reply arrives. Returns the reply.
    fn journaled(
        &mut self,
        c: &mut Conn,
        i: usize,
        line: &str,
        alt: Option<Durable>,
    ) -> io::Result<String> {
        match c.cmd(line) {
            Ok(r) => Ok(r),
            Err(e) => {
                self.jobs[i].alt = Some(alt);
                self.counts.in_flight += 1;
                Err(e)
            }
        }
    }

    fn unexpected(&mut self, what: &str, id: u64, reply: &str) -> OpResult {
        self.violation(format!("{what} {id}: unexpected reply {reply:?}"));
        Err(io::Error::other("unexpected reply"))
    }

    fn delete(&mut self, c: &mut Conn, i: usize) -> OpResult {
        let id = self.jobs[i].id;
        let r = self.journaled(c, i, &format!("delete {id}"), None)?;
        if r != "DELETED" {
            return self.unexpected("delete", id, &r);
        }
        self.counts.deleted += 1;
        self.deleted.push(id);
        self.jobs.swap_remove(i);
        Ok(())
    }

    fn reserve(&mut self, c: &mut Conn, i: usize) -> OpResult {
        let id = self.jobs[i].id;
        let r = match c.cmd(&format!("reserve-job {id}")) {
            Ok(r) => r,
            Err(e) => {
                self.counts.in_flight += 1;
                return Err(e);
            }
        };
        if !r.starts_with(&format!("RESERVED {id} ")) {
            return self.unexpected("reserve-job", id, &r);
        }
        let n = self.jobs[i].body.len();
        c.body(n)?;
        self.counts.unjournaled += 1;
        self.jobs[i].live = Live::Reserved;
        Ok(())
    }

    fn reserve_then(&mut self, c: &mut Conn, i: usize, then: Then) -> OpResult {
        self.reserve(c, i)?;
        match then {
            Then::Bury => self.bury(c, i),
            Then::ReleaseDelay => self.release(c, i, true),
            Then::Release0 => self.release(c, i, false),
        }
    }

    fn bury(&mut self, c: &mut Conn, i: usize) -> OpResult {
        let id = self.jobs[i].id;
        let pri = self.rng.below(5000) as u32;
        let d = Durable {
            pri,
            st: St::Buried,
        };
        let r = self.journaled(c, i, &format!("bury {id} {pri}"), Some(d))?;
        if r != "BURIED" {
            return self.unexpected("bury", id, &r);
        }
        self.counts.buried += 1;
        let j = &mut self.jobs[i];
        j.dur = d;
        j.live = Live::Buried;
        j.live_pri = pri;
        Ok(())
    }

    fn release(&mut self, c: &mut Conn, i: usize, delayed: bool) -> OpResult {
        let id = self.jobs[i].id;
        let pri = self.rng.below(5000) as u32;
        if !delayed {
            // Not journaled: the durable state stays as it was.
            let r = match c.cmd(&format!("release {id} {pri} 0")) {
                Ok(r) => r,
                Err(e) => {
                    self.counts.in_flight += 1;
                    return Err(e);
                }
            };
            if r != "RELEASED" {
                return self.unexpected("release", id, &r);
            }
            self.counts.unjournaled += 1;
            let j = &mut self.jobs[i];
            j.live = Live::Ready;
            j.live_pri = pri;
            return Ok(());
        }
        let delay = if self.rng.percent(60) {
            self.rng.range(1, 3)
        } else {
            3600
        };
        let sent = wall_ms();
        let r = match c.cmd(&format!("release {id} {pri} {delay}")) {
            Ok(r) => r,
            Err(e) => {
                // The server applied it (if at all) before it died, which
                // is before this error was seen.
                self.jobs[i].alt = Some(Some(Durable {
                    pri,
                    st: St::Delayed {
                        lo: sent + delay * 1000,
                        hi: wall_ms() + delay * 1000,
                    },
                }));
                self.counts.in_flight += 1;
                return Err(e);
            }
        };
        if r != "RELEASED" {
            return self.unexpected("release", id, &r);
        }
        let acked = wall_ms();
        self.counts.released_delay += 1;
        let j = &mut self.jobs[i];
        j.dur = Durable {
            pri,
            st: St::Delayed {
                lo: sent + delay * 1000,
                hi: acked + delay * 1000,
            },
        };
        j.live = Live::Delayed(acked + delay * 1000);
        j.live_pri = pri;
        Ok(())
    }

    fn kick(&mut self, c: &mut Conn, i: usize) -> OpResult {
        let id = self.jobs[i].id;
        let pri = self.jobs[i].live_pri;
        let d = Durable { pri, st: St::Ready };
        let r = self.journaled(c, i, &format!("kick-job {id}"), Some(d))?;
        if r == "NOT_FOUND"
            && let Live::Delayed(at) = self.jobs[i].live
            && wall_ms() + 1000 >= at
        {
            // The delay expired on its own just before the kick (not
            // journaled): the job is ready now, still delayed on disk.
            self.jobs[i].alt = None;
            self.jobs[i].live = Live::Ready;
            return Ok(());
        }
        if r != "KICKED" {
            return self.unexpected("kick-job", id, &r);
        }
        self.counts.kicked += 1;
        let j = &mut self.jobs[i];
        j.dur = d;
        j.live = Live::Ready;
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum Then {
    Bury,
    ReleaseDelay,
    Release0,
}

/// The state of a durability run across rounds.
struct Model {
    jobs: BTreeMap<u64, Job>,
    /// Ids ever acknowledged or resolved (so gaps can be probed).
    known: BTreeSet<u64>,
    pending_puts: Vec<PendingPut>,
    deleted: Vec<u64>,
    violations: Vec<String>,
    jobs_checked: u64,
    unknown_adopted: u64,
}

/// Checks one job against the model; returns the observed durable state
/// (`None` if absent) or a violation.
fn check_job(c: &mut Conn, j: &Job) -> io::Result<Result<Option<Durable>, String>> {
    let before = wall_ms();
    let stats = c.yaml(&format!("stats-job {}", j.id))?;
    let after = wall_ms();
    let Some(s) = stats else {
        if j.alt == Some(None) {
            return Ok(Ok(None));
        }
        return Ok(Err(format!(
            "job {} lost (expected {:?}, alt {:?})",
            j.id, j.dur, j.alt
        )));
    };
    let body = c.peek(j.id)?;
    if s.get("tube").map(String::as_str) != Some(j.tube.as_str()) {
        return Ok(Err(format!(
            "job {}: tube {:?}, expected {:?}",
            j.id,
            s.get("tube"),
            j.tube
        )));
    }
    if body.as_deref() != Some(j.body.as_slice()) {
        return Ok(Err(format!("job {}: body differs", j.id)));
    }
    let pri: u32 = s
        .get("pri")
        .and_then(|v| v.parse().ok())
        .unwrap_or(u32::MAX);
    let state = s.get("state").cloned().unwrap_or_default();
    let mut options = vec![j.dur];
    if let Some(Some(d)) = j.alt {
        options.push(d);
    }
    for d in options {
        if d.pri != pri {
            continue;
        }
        let ok = match (d.st, state.as_str()) {
            (St::Ready, "ready") | (St::Buried, "buried") => Some(d),
            // Must still be delayed if the deadline is clearly ahead;
            // must be ready if it clearly passed; either near it.
            (St::Delayed { hi, .. }, "delayed") if before < hi + DELAY_MARGIN_MS => Some(d),
            (St::Delayed { lo, .. }, "ready") if after + DELAY_MARGIN_MS >= lo => {
                Some(Durable { pri, st: St::Ready })
            }
            _ => None,
        };
        if let Some(d) = ok {
            return Ok(Ok(Some(d)));
        }
    }
    Ok(Err(format!(
        "job {}: state {state} pri {pri} (at {before}..{after}), expected {:?} or {:?}",
        j.id, j.dur, j.alt
    )))
}

impl Model {
    fn new() -> Model {
        Model {
            jobs: BTreeMap::new(),
            known: BTreeSet::new(),
            pending_puts: Vec::new(),
            deleted: Vec::new(),
            violations: Vec::new(),
            jobs_checked: 0,
            unknown_adopted: 0,
        }
    }

    /// Verifies the restarted server against the model and resolves every
    /// in-flight change to what was observed.
    fn verify(&mut self, addr: SocketAddr, round: u64) {
        let mut c = Conn::connect(addr).expect("connect for verification");
        // Every acknowledged delete is gone.
        for id in std::mem::take(&mut self.deleted) {
            if c.yaml(&format!("stats-job {id}")).unwrap().is_some() {
                self.violations
                    .push(format!("round {round}: deleted job {id} is back"));
            }
        }
        // Every known job.
        let ids: Vec<u64> = self.jobs.keys().copied().collect();
        let mut present = 0u64;
        for id in ids {
            let j = &self.jobs[&id];
            self.jobs_checked += 1;
            match check_job(&mut c, j).unwrap() {
                Ok(Some(d)) => {
                    present += 1;
                    let j = self.jobs.get_mut(&id).unwrap();
                    j.dur = d;
                    j.alt = None;
                    j.reset_live();
                }
                Ok(None) => {
                    self.jobs.remove(&id);
                }
                Err(v) => {
                    self.violations.push(format!("round {round}: {v}"));
                    // Resynchronize the model with the server so later
                    // rounds keep checking meaningfully.
                    self.jobs.remove(&id);
                    if c.yaml(&format!("stats-job {id}")).unwrap().is_some() {
                        let _ = c.cmd(&format!("delete {id}"));
                    }
                }
            }
        }
        // Puts whose reply was lost: probe unknown ids.
        let pending = std::mem::take(&mut self.pending_puts);
        let max_known = self.known.last().copied().unwrap_or(0);
        let top = max_known + pending.len() as u64 + 8;
        let mut used = vec![false; pending.len()];
        for id in 1..=top {
            if self.known.contains(&id) {
                continue;
            }
            let Some(s) = c.yaml(&format!("stats-job {id}")).unwrap() else {
                if id <= max_known {
                    // A gap that will never be filled (ids below the
                    // highest one on disk are not reused).
                    self.known.insert(id);
                }
                continue;
            };
            let body = c.peek(id).unwrap().unwrap_or_default();
            let tube = s.get("tube").cloned().unwrap_or_default();
            let pri: u32 = s
                .get("pri")
                .and_then(|v| v.parse().ok())
                .unwrap_or(u32::MAX);
            let state = s.get("state").cloned().unwrap_or_default();
            let found = pending
                .iter()
                .enumerate()
                .position(|(k, p)| !used[k] && p.body == body && p.tube == tube && p.pri == pri);
            match found {
                Some(k) if state == "ready" => {
                    used[k] = true;
                    present += 1;
                    self.unknown_adopted += 1;
                    self.known.insert(id);
                    self.jobs.insert(
                        id,
                        Job {
                            id,
                            tube,
                            body,
                            dur: Durable { pri, st: St::Ready },
                            alt: None,
                            live: Live::Ready,
                            live_pri: pri,
                        },
                    );
                }
                _ => {
                    self.violations.push(format!(
                        "round {round}: unexpected job {id} ({tube}, {state}, pri {pri}, {} bytes)",
                        body.len()
                    ));
                    let _ = c.cmd(&format!("delete {id}"));
                }
            }
        }
        // Nothing else is on the server.
        let total = c.total_jobs().unwrap();
        if total != present {
            self.violations.push(format!(
                "round {round}: server holds {total} jobs, model accounts for {present}"
            ));
        }
    }
}

struct ModeReport {
    mode: &'static str,
    rounds: u64,
    counts: Counts,
    jobs_checked: u64,
    unknown_adopted: u64,
    max_disk: u64,
    max_files: u64,
    violations: Vec<String>,
    elapsed: Duration,
}

/// Runs `rounds` kill/restart rounds with the given fsync flag.
fn torture(
    mode: &'static str,
    flag: &[&'static str],
    rounds: u64,
    max_run_ms: u64,
    seed: u64,
) -> ModeReport {
    const WORKERS: usize = 4;
    let t0 = Instant::now();
    let dir = tempfile::tempdir().unwrap();
    let bin = dir.path().join("wal");
    let mut rng = Rng::new(seed);
    let args_for = |round: u64| -> Vec<&'static str> {
        let mut a: Vec<&'static str> = flag.to_vec();
        a.push("-s");
        a.push(if round.is_multiple_of(2) {
            "65536"
        } else {
            "262144"
        });
        a
    };
    let mut server = start(&bin, &args_for(0));
    let mut model = Model::new();
    let mut counts = Counts::default();
    let mut max_disk = 0;
    let mut max_files = 0;
    for round in 0..rounds {
        // Hand out the jobs by id.
        let mut owned: Vec<Vec<Job>> = vec![Vec::new(); WORKERS];
        for (id, j) in std::mem::take(&mut model.jobs) {
            owned[(id % WORKERS as u64) as usize].push(j);
        }
        let stop = Arc::new(AtomicBool::new(false));
        let handles: Vec<_> = owned
            .into_iter()
            .enumerate()
            .map(|(idx, jobs)| {
                let w = Worker {
                    idx,
                    rng: Rng::new(rng.next()),
                    jobs,
                    target: 150 + (rng.below(150) as usize),
                    counts: Counts::default(),
                    pending_puts: Vec::new(),
                    deleted: Vec::new(),
                    violations: Vec::new(),
                    seq: 0,
                    round,
                };
                let addr = server.addr;
                let stop = stop.clone();
                std::thread::spawn(move || w.run(addr, stop))
            })
            .collect();
        let run_ms = match rng.below(100) {
            0..10 => rng.range(0, 30),
            10..80 => rng.range(30, max_run_ms / 2),
            _ => rng.range(max_run_ms / 2, max_run_ms),
        };
        std::thread::sleep(Duration::from_millis(run_ms));
        server.stop(Signal::SIGKILL);
        stop.store(true, Ordering::Relaxed);
        for h in handles {
            let w = h.join().unwrap();
            counts.add(&w.counts);
            for v in w.violations {
                model
                    .violations
                    .push(format!("round {round}: worker {}: {v}", w.idx));
            }
            model.pending_puts.extend(w.pending_puts);
            model.deleted.extend(w.deleted);
            for j in w.jobs {
                model.known.insert(j.id);
                model.jobs.insert(j.id, j);
            }
        }
        for id in &model.deleted {
            model.known.insert(*id);
        }
        let (bytes, files) = binlog_usage(&bin);
        max_disk = max_disk.max(bytes);
        max_files = max_files.max(files);
        drop(server);
        server = start(&bin, &args_for(round + 1));
        model.verify(server.addr, round);
        if !model.violations.is_empty() && model.violations.len() > 50 {
            break;
        }
    }
    drop(server);
    ModeReport {
        mode,
        rounds,
        counts,
        jobs_checked: model.jobs_checked,
        unknown_adopted: model.unknown_adopted,
        max_disk,
        max_files,
        violations: model.violations,
        elapsed: t0.elapsed(),
    }
}

fn modes() -> Vec<(&'static str, Vec<&'static str>)> {
    let all: [(&'static str, Vec<&'static str>); 3] =
        [("F", vec!["-F"]), ("default", vec![]), ("f0", vec!["-f0"])];
    let wanted =
        std::env::var("BSTK_DURABILITY_MODES").unwrap_or_else(|_| "F,default,f0".to_owned());
    let wanted: Vec<&str> = wanted.split(',').map(str::trim).collect();
    all.into_iter()
        .filter(|(m, _)| wanted.contains(m))
        .collect()
}

fn run_torture(default_rounds: u64, max_run_ms: u64) {
    let rounds = env_u64("BSTK_DURABILITY_ROUNDS", default_rounds);
    let seed = seed();
    eprintln!("durability: seed {seed} (BSTK_DURABILITY_SEED), {rounds} rounds per mode");
    // The modes run concurrently, each on its own server and directory.
    let handles: Vec<_> = modes()
        .into_iter()
        .enumerate()
        .map(|(k, (mode, flag))| {
            let s = seed.wrapping_add(k as u64 * 0x1000_0000);
            std::thread::spawn(move || torture(mode, &flag, rounds, max_run_ms, s))
        })
        .collect();
    let reports: Vec<ModeReport> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let mut failed = false;
    eprintln!(
        "| mode | rounds | acked ops | inserted | deleted | buried | released+delay | kicked | unjournaled acks | in flight at kill | jobs checked | lost-reply puts found | max binlog bytes | max files | violations | time |"
    );
    eprintln!("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|");
    for r in &reports {
        let c = &r.counts;
        eprintln!(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {:.0} s |",
            r.mode,
            r.rounds,
            c.acked(),
            c.inserted,
            c.deleted,
            c.buried,
            c.released_delay,
            c.kicked,
            c.unjournaled,
            c.in_flight,
            r.jobs_checked,
            r.unknown_adopted,
            r.max_disk,
            r.max_files,
            r.violations.len(),
            r.elapsed.as_secs_f64()
        );
        for v in r.violations.iter().take(20) {
            eprintln!("  {} violation: {v}", r.mode);
            failed = true;
        }
        failed |= !r.violations.is_empty();
    }
    assert!(!failed, "durability violations (seed {seed})");
}

/// Always-on: a few rounds per fsync mode (see the module docs).
#[test]
fn durability_torture_short() {
    run_torture(4, 400);
}

/// ≥ 100 rounds per fsync mode (see the module docs for the command).
#[test]
#[ignore = "long-running; run explicitly (see the module docs)"]
fn durability_torture_full() {
    run_torture(100, 1500);
}

// ---------------------------------------------------------------------
// Compaction churn
// ---------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
struct Steady {
    tube: String,
    body: Vec<u8>,
    pri: u32,
    /// "ready", "buried" or "delayed".
    state: &'static str,
}

const STEADY_JOBS: usize = 1000;
const STEADY_DELAY: u32 = 1_000_000;

/// Puts one steady job in `state` and returns its id.
fn put_steady(c: &mut Conn, rng: &mut Rng, n: u64, state: &'static str) -> (u64, Steady) {
    let tube = format!("steady-{}", n % 7);
    assert_eq!(
        c.cmd(&format!("use {tube}")).unwrap(),
        format!("USING {tube}")
    );
    let len = rng.range(16, 600) as usize;
    let mut body = format!("steady-{n}:").into_bytes();
    while body.len() < len {
        body.push(b'a' + (rng.below(26) as u8));
    }
    let pri = rng.below(10_000) as u32;
    let delay = if state == "delayed" { STEADY_DELAY } else { 0 };
    c.send(&Conn::put_msg(pri, delay, TTR, &body)).unwrap();
    let r = c.line().unwrap();
    let id: u64 = r.strip_prefix("INSERTED ").unwrap().parse().unwrap();
    let mut pri_now = pri;
    if state == "buried" {
        let r = c.cmd(&format!("reserve-job {id}")).unwrap();
        assert!(r.starts_with("RESERVED"), "{r}");
        c.body(body.len()).unwrap();
        pri_now = rng.below(10_000) as u32;
        assert_eq!(c.cmd(&format!("bury {id} {pri_now}")).unwrap(), "BURIED");
    }
    (
        id,
        Steady {
            tube,
            body,
            pri: pri_now,
            state,
        },
    )
}

/// Put + delete pairs, pipelined in batches, on one connection. Every
/// `replace_every` batches it also replaces one steady job (through
/// `steady`).
fn churn_conn(addr: SocketAddr, pairs: u64, body_len: usize, tube: String, done: Arc<AtomicU64>) {
    const BATCH: u64 = 64;
    let mut c = Conn::connect(addr).unwrap();
    assert_eq!(
        c.cmd(&format!("use {tube}")).unwrap(),
        format!("USING {tube}")
    );
    let body = vec![b'c'; body_len];
    let put = Conn::put_msg(1, 0, TTR, &body);
    let mut left = pairs;
    let mut ids = Vec::with_capacity(BATCH as usize);
    let mut msg = Vec::new();
    while left > 0 {
        let n = left.min(BATCH);
        msg.clear();
        for _ in 0..n {
            msg.extend_from_slice(&put);
        }
        c.send(&msg).unwrap();
        ids.clear();
        for _ in 0..n {
            let r = c.line().unwrap();
            ids.push(r.strip_prefix("INSERTED ").unwrap().parse::<u64>().unwrap());
        }
        msg.clear();
        for id in &ids {
            msg.extend_from_slice(format!("delete {id}\r\n").as_bytes());
        }
        c.send(&msg).unwrap();
        for _ in 0..n {
            assert_eq!(c.line().unwrap(), "DELETED");
        }
        left -= n;
        done.fetch_add(n, Ordering::Relaxed);
    }
}

/// Samples the binlog directory size until `stop`.
fn sampler(dir: PathBuf, stop: Arc<AtomicBool>) -> std::thread::JoinHandle<(u64, u64)> {
    std::thread::spawn(move || {
        let mut max = (0, 0);
        while !stop.load(Ordering::Relaxed) {
            let (b, f) = binlog_usage(&dir);
            max = (max.0.max(b), max.1.max(f));
            std::thread::sleep(Duration::from_millis(5));
        }
        max
    })
}

struct ChurnPhase {
    max_bytes: u64,
    max_files: u64,
    pairs: u64,
    elapsed: Duration,
}

/// One churn phase: `pairs` put + delete pairs over 4 connections while
/// the main connection replaces steady jobs.
fn churn_phase(
    server: &Server,
    dir: &Path,
    steady: &mut BTreeMap<u64, Steady>,
    rng: &mut Rng,
    next_n: &mut u64,
    pairs: u64,
) -> ChurnPhase {
    const CONNS: u64 = 4;
    let t0 = Instant::now();
    let stop = Arc::new(AtomicBool::new(false));
    let samp = sampler(dir.to_path_buf(), stop.clone());
    let done = Arc::new(AtomicU64::new(0));
    let handles: Vec<_> = (0..CONNS)
        .map(|k| {
            let addr = server.addr;
            let done = done.clone();
            let n = pairs / CONNS + u64::from(k < pairs % CONNS);
            let len = [16, 100, 1000, 4000][k as usize % 4];
            std::thread::spawn(move || churn_conn(addr, n, len, format!("churn-{k}"), done))
        })
        .collect();
    // Replace one steady job per ~500 churned pairs.
    let mut c = Conn::connect(server.addr).unwrap();
    let mut replaced = 0u64;
    while done.load(Ordering::Relaxed) < pairs {
        let due = done.load(Ordering::Relaxed) / 500;
        if replaced < due {
            let ids: Vec<u64> = steady.keys().copied().collect();
            let victim = ids[rng.below(ids.len() as u64) as usize];
            assert_eq!(c.cmd(&format!("delete {victim}")).unwrap(), "DELETED");
            steady.remove(&victim);
            let state = ["ready", "ready", "buried", "delayed"][rng.below(4) as usize];
            *next_n += 1;
            let (id, s) = put_steady(&mut c, rng, *next_n, state);
            steady.insert(id, s);
            replaced += 1;
        } else {
            std::thread::sleep(Duration::from_millis(1));
        }
        if handles.iter().all(|h| h.is_finished()) {
            break;
        }
    }
    for h in handles {
        h.join().unwrap();
    }
    stop.store(true, Ordering::Relaxed);
    let (max_bytes, max_files) = samp.join().unwrap();
    ChurnPhase {
        max_bytes,
        max_files,
        pairs,
        elapsed: t0.elapsed(),
    }
}

/// Checks that the server holds exactly `steady`.
fn check_steady(server: &Server, steady: &BTreeMap<u64, Steady>, when: &str) {
    let mut c = Conn::connect(server.addr).unwrap();
    for (id, s) in steady {
        let st = c
            .yaml(&format!("stats-job {id}"))
            .unwrap()
            .unwrap_or_else(|| panic!("{when}: steady job {id} lost"));
        assert_eq!(st["tube"], s.tube, "{when}: job {id} tube");
        assert_eq!(st["state"], s.state, "{when}: job {id} state");
        assert_eq!(st["pri"], s.pri.to_string(), "{when}: job {id} pri");
        assert_eq!(
            c.peek(*id).unwrap().as_deref(),
            Some(s.body.as_slice()),
            "{when}: job {id} body"
        );
    }
    let stats = c.yaml("stats").unwrap().unwrap();
    let count = |state: &str| {
        steady
            .values()
            .filter(|s| s.state == state)
            .count()
            .to_string()
    };
    assert_eq!(stats["current-jobs-ready"], count("ready"), "{when}: ready");
    assert_eq!(
        stats["current-jobs-buried"],
        count("buried"),
        "{when}: buried"
    );
    assert_eq!(
        stats["current-jobs-delayed"],
        count("delayed"),
        "{when}: delayed"
    );
    assert_eq!(stats["current-jobs-reserved"], "0", "{when}: reserved");
}

fn compaction_churn(pairs: u64) {
    const SEG: u64 = 65536;
    let seed = seed();
    eprintln!("churn: seed {seed}, {pairs} put+delete pairs, -s {SEG}");
    let mut rng = Rng::new(seed);
    let dir = tempfile::tempdir().unwrap();
    let bin = dir.path().join("wal");
    let args = ["-s", "65536"];
    let mut server = start(&bin, &args);
    let mut steady = BTreeMap::new();
    let mut c = Conn::connect(server.addr).unwrap();
    let mut next_n = 0u64;
    for n in 0..STEADY_JOBS {
        let state = match n % 10 {
            0..6 => "ready",
            6..8 => "buried",
            _ => "delayed",
        };
        next_n += 1;
        let (id, s) = put_steady(&mut c, &mut rng, next_n, state);
        steady.insert(id, s);
    }
    drop(c);
    let live_bytes: u64 = steady
        .values()
        .map(|s| (s.body.len() + s.tube.len() + 64) as u64)
        .sum();
    let (setup_bytes, _) = binlog_usage(&bin);

    let p1 = churn_phase(&server, &bin, &mut steady, &mut rng, &mut next_n, pairs);
    let records_migrated = Conn::connect(server.addr)
        .unwrap()
        .yaml("stats")
        .unwrap()
        .unwrap()["binlog-records-migrated"]
        .clone();
    assert_eq!(server.stop(Signal::SIGTERM).code(), Some(0));
    let (after_term, files_term) = binlog_usage(&bin);
    drop(server);
    let server2 = start(&bin, &args);
    check_steady(&server2, &steady, "after SIGTERM restart");

    let p2 = churn_phase(
        &server2,
        &bin,
        &mut steady,
        &mut rng,
        &mut next_n,
        (pairs / 10).max(2000),
    );
    let mut server2 = server2;
    server2.stop(Signal::SIGKILL);
    let (after_kill, files_kill) = binlog_usage(&bin);
    drop(server2);
    let server3 = start(&bin, &args);
    check_steady(&server3, &steady, "after SIGKILL restart");

    let max_bytes = p1.max_bytes.max(p2.max_bytes);
    eprintln!(
        "churn: steady set {} jobs (~{live_bytes} bytes of records), after setup {setup_bytes} bytes on disk",
        steady.len()
    );
    for (name, p) in [("phase 1", &p1), ("phase 2", &p2)] {
        eprintln!(
            "churn: {name}: {} pairs in {:.1} s ({:.0} ops/s), max binlog {} bytes in {} files",
            p.pairs,
            p.elapsed.as_secs_f64(),
            (2 * p.pairs) as f64 / p.elapsed.as_secs_f64(),
            p.max_bytes,
            p.max_files
        );
    }
    eprintln!(
        "churn: records migrated in phase 1: {records_migrated}; after SIGTERM {after_term} bytes / {files_term} files; after SIGKILL {after_kill} bytes / {files_kill} files"
    );
    // Bounded: compaction keeps (allocated - live) / live < 2, plus the
    // current segment, the spare and segments in transit.
    let bound = 6 * live_bytes + 8 * SEG;
    assert!(
        max_bytes <= bound,
        "binlog grew to {max_bytes} bytes (bound {bound}); live ~{live_bytes}"
    );
}

/// Always-on: a short churn (see the module docs).
#[test]
fn compaction_churn_short() {
    compaction_churn(env_u64("BSTK_CHURN_OPS", 20_000));
}

/// 1M put + delete pairs (see the module docs for the command).
#[test]
#[ignore = "long-running; run explicitly (see the module docs)"]
fn compaction_churn_full() {
    compaction_churn(env_u64("BSTK_CHURN_OPS", 1_000_000));
}
