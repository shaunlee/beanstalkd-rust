//! The engine actor: the sole owner and caller of the `Engine` (and, with
//! `-b`, of the write-ahead log). Connection tasks talk to it through
//! `EngineHandle`, an unbounded channel of `EngineMsg` (sending never
//! blocks); replies go back over each connection's tokio unbounded channel.
//!
//! Where it runs depends on `-b`:
//! - With `-b`, on a dedicated OS thread fed by a std channel and woken by
//!   `recv_timeout` at the earliest of the engine's next deadline and the
//!   next interval fsync, so blocking file I/O and fsync never run on a
//!   tokio worker.
//! - Without `-b` it does no I/O at all, so it runs as a tokio task (a
//!   `select!` over its channel and one re-armed timer), as before P1.
//!   Measured on the P0 benchmarks, the OS thread costs 9-17% of
//!   throughput without `-b`: every command then needs two cross-thread
//!   wake-ups (worker -> actor thread -> worker), while a task is usually
//!   woken on the same worker as the connection that sent the command.
//!
//! Both loops run the same per-message step (`Actor::on_message` /
//! `Actor::on_timer`).
//!
//! Time discipline (docs/PLAN.md §4.2.1, docs/DESIGN.md §4.1): `now` for
//! every engine call is the wall clock captured once at startup plus the
//! monotonic time elapsed since then (`Clock`), read fresh right before
//! that call. After every call we call `tick(now)` again; this resolves the
//! DEADLINE_SOON-after-handle case documented in docs/COMPAT.md (engine
//! item 5). Messages are processed one at a time, each followed by its own
//! tick.
//!
//! Write before reply (docs/PLAN.md §4.2.4): after every engine call and
//! its tick, the journal entries they produced are appended to the WAL
//! (and, with `-f0`, fsynced by `append`) before any of their replies is
//! released. Any WAL error is fatal: the pending replies are dropped and
//! the process exits (fail-stop, docs/COMPAT.md D5).

use std::collections::HashMap;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::{mpsc as tokio_mpsc, oneshot};

use bstk_engine::{BinlogStats, ConnId, Engine, JournalEntry, Nanos, Outbox};
use bstk_proto::{Command, MAX_TUBE_NAME_LEN, PutRejection, Response};
use bstk_store::{SyncPolicy, Wal, WalError};

/// Exit status after a WAL write, fsync or compaction error (fail-stop).
pub const EXIT_WAL_FAILURE: i32 = 20;

/// Upper bound on one wait, so a far-away deadline (e.g. a `u32::MAX`
/// second delay) never has to be represented as an `Instant`. Waking up
/// early is harmless: `tick` returns at once when nothing is due.
const MAX_WAIT: Duration = Duration::from_secs(3600);

/// A message sent by a connection task (or the signal handler) to the
/// engine actor.
pub enum EngineMsg {
    /// A new connection was accepted. Must be sent before that connection's
    /// first `Command`, on the same channel, so ordering is preserved.
    Connect {
        conn: ConnId,
        reply_tx: tokio_mpsc::UnboundedSender<Response>,
    },
    /// One fully-decoded command, sent only after the previous command on
    /// this connection has received its reply.
    Command { conn: ConnId, cmd: Command },
    /// A put command line was accepted; its body is still to come
    /// (`Frame::PutStarted`). Produces no reply.
    PutStarted { conn: ConnId, too_big: bool },
    /// A `put` rejected by the codec during framing (`Frame::PutRejected`).
    PutRejected { conn: ConnId, why: PutRejection },
    /// The connection's socket reached EOF while a reply was outstanding
    /// (or has already reached EOF and another command was just
    /// dispatched). A no-op unless the connection is currently blocked in
    /// reserve.
    HalfClose { conn: ConnId },
    /// The connection is gone; release its jobs and forget it. Sent from a
    /// drop guard so this fires on every exit path.
    Disconnect { conn: ConnId },
    /// SIGUSR1: enter (or, in principle, leave) drain mode.
    SetDraining(bool),
    /// SIGINT / SIGTERM: sync the WAL (unless `-F`), acknowledge on `done`
    /// and stop. Messages queued behind it are never processed.
    Shutdown { done: oneshot::Sender<()> },
}

/// Handle used by connection tasks and the signal handlers to reach the
/// engine actor. Cheap to clone; sending never blocks (unbounded), so a
/// slow or wedged connection can never stall the engine, and it can be
/// called from async code (including `Drop`).
#[derive(Clone)]
pub enum EngineHandle {
    /// The actor runs on its own OS thread (`-b`).
    Thread(mpsc::Sender<EngineMsg>),
    /// The actor runs as a tokio task (no `-b`).
    Task(tokio_mpsc::UnboundedSender<EngineMsg>),
}

/// The actor has stopped; the message was not delivered.
#[derive(Debug)]
pub struct EngineGone;

impl EngineHandle {
    pub fn send(&self, msg: EngineMsg) -> Result<(), EngineGone> {
        match self {
            EngineHandle::Thread(tx) => tx.send(msg).map_err(|_| EngineGone),
            EngineHandle::Task(tx) => tx.send(msg).map_err(|_| EngineGone),
        }
    }
}

/// The server's time source: wall-clock nanoseconds since the Unix epoch,
/// captured once at startup, plus monotonic time elapsed since then. Time
/// never goes backwards while the process runs, and job times written to
/// the binlog stay meaningful across restarts.
#[derive(Debug, Clone, Copy)]
pub struct Clock {
    anchor: Nanos,
    start: Instant,
}

impl Clock {
    pub fn start() -> Clock {
        // A clock set before 1970 gives 0 (the engine only needs a start
        // point); nanoseconds fit in u64 until the year 2554.
        let anchor = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX));
        Clock {
            anchor,
            start: Instant::now(),
        }
    }

    pub fn now(&self) -> Nanos {
        let elapsed = u64::try_from(self.start.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.anchor.saturating_add(elapsed)
    }

    /// The `Instant` at which `now()` reaches `at`, or at most `MAX_WAIT`
    /// from now: a far deadline (a `u32::MAX` second delay or TTR on top
    /// of a wall-clock epoch) is never converted to an `Instant`, which
    /// could overflow. Waking up early is harmless.
    fn instant_at(&self, at: Nanos) -> Instant {
        let now = Instant::now();
        let cap = now + MAX_WAIT;
        self.start
            .checked_add(Duration::from_nanos(at.saturating_sub(self.anchor)))
            .map_or(cap, |t| t.min(cap))
    }
}

/// What the actor needs from the write-ahead log; implemented by
/// `bstk_store::Wal` (see its docs for the contract) and by a fake in the
/// unit tests below.
pub trait Log: Send + 'static {
    fn reserve_put(&mut self, tube_len: usize, body_len: usize) -> bool;
    fn append(&mut self, entries: &[JournalEntry]) -> Result<(), WalError>;
    fn sync_if_due(&mut self, now: Instant) -> Result<(), WalError>;
    fn maintain(&mut self) -> Result<(), WalError>;
    fn stats(&self) -> BinlogStats;
}

impl Log for Wal {
    fn reserve_put(&mut self, tube_len: usize, body_len: usize) -> bool {
        Wal::reserve_put(self, tube_len, body_len)
    }
    fn append(&mut self, entries: &[JournalEntry]) -> Result<(), WalError> {
        Wal::append(self, entries)
    }
    fn sync_if_due(&mut self, now: Instant) -> Result<(), WalError> {
        Wal::sync_if_due(self, now)
    }
    fn maintain(&mut self) -> Result<(), WalError> {
        Wal::maintain(self)
    }
    fn stats(&self) -> BinlogStats {
        Wal::stats(self)
    }
}

/// The WAL plus the actor's bookkeeping around it.
struct Binlog<L> {
    log: L,
    policy: SyncPolicy,
    /// A put reservation was made since the last `append`; the next
    /// `append` must run (even with no entries) to consume or release it.
    reserved: bool,
    /// `records_written` as of the last write, to detect new writes
    /// (compaction moves included).
    records_written: u64,
    /// With `SyncPolicy::Interval`: when to call `sync_if_due` if no
    /// further write does it first (last write + interval).
    sync_at: Option<Instant>,
}

/// The engine, its optional WAL and the reply channels. Owned by the actor
/// thread; every method runs one complete engine step.
pub struct Actor<L> {
    clock: Clock,
    engine: Engine,
    binlog: Option<Binlog<L>>,
    conns: HashMap<ConnId, tokio_mpsc::UnboundedSender<Response>>,
    outbox: Outbox,
    journal: Vec<JournalEntry>,
    /// Mirrors the engine's drain mode (a draining put makes no
    /// reservation, as in prot.c where `drain_mode` is checked before
    /// `walresvput`).
    draining: bool,
}

impl<L: Log> Actor<L> {
    /// `engine` must have been created with `EngineConfig::journal` equal
    /// to `log.is_some()`. Pushes the log's stats into the engine.
    pub fn new(clock: Clock, mut engine: Engine, log: Option<(L, SyncPolicy)>) -> Self {
        let binlog = log.map(|(log, policy)| {
            let stats = log.stats();
            engine.set_binlog_stats(stats);
            Binlog {
                log,
                policy,
                reserved: false,
                records_written: stats.records_written,
                sync_at: None,
            }
        });
        Actor {
            clock,
            engine,
            binlog,
            conns: HashMap::new(),
            outbox: Vec::new(),
            journal: Vec::new(),
            draining: false,
        }
    }

    /// Starts the actor: on its own OS thread with a WAL, else as a task
    /// on `runtime`.
    pub fn spawn(self, runtime: &tokio::runtime::Handle) -> std::io::Result<EngineHandle> {
        if self.binlog.is_some() {
            let (tx, rx) = mpsc::channel();
            std::thread::Builder::new()
                .name("engine".to_owned())
                .spawn(move || self.run(rx))?;
            Ok(EngineHandle::Thread(tx))
        } else {
            let (tx, rx) = tokio_mpsc::unbounded_channel();
            runtime.spawn(self.run_task(rx));
            Ok(EngineHandle::Task(tx))
        }
    }

    /// The task loop (no WAL). The timer is re-armed only when the
    /// engine's next deadline changes (most messages leave it unchanged).
    async fn run_task(mut self, mut rx: tokio_mpsc::UnboundedReceiver<EngineMsg>) {
        let sleep = tokio::time::sleep(Duration::ZERO);
        tokio::pin!(sleep);
        let mut armed: Option<Nanos> = None;
        loop {
            let deadline = self.engine.next_deadline();
            if deadline != armed {
                if let Some(at) = deadline {
                    sleep
                        .as_mut()
                        .reset(tokio::time::Instant::from_std(self.clock.instant_at(at)));
                }
                armed = deadline;
            }
            let res = tokio::select! {
                msg = rx.recv() => match msg {
                    Some(EngineMsg::Shutdown { done }) => {
                        if let Err(e) = self.shutdown() {
                            fail_stop(&e);
                        }
                        let _ = done.send(());
                        return;
                    }
                    Some(msg) => self.on_message(msg),
                    // Every sender is gone: nothing left to serve.
                    None => return,
                },
                () = &mut sleep, if armed.is_some() => {
                    // Re-arm on the next iteration even if the deadline
                    // is unchanged (it may have been capped).
                    armed = None;
                    self.on_timer()
                }
            };
            if let Err(e) = res {
                fail_stop(&e);
            }
        }
    }

    fn run(mut self, rx: mpsc::Receiver<EngineMsg>) {
        loop {
            let received = self.recv(&rx);
            let res = match received {
                Ok(EngineMsg::Shutdown { done }) => {
                    let res = self.shutdown();
                    if let Err(e) = res {
                        fail_stop(&e);
                    }
                    let _ = done.send(());
                    return;
                }
                Ok(msg) => self.on_message(msg),
                Err(RecvTimeoutError::Timeout) => self.on_timer(),
                // Every sender is gone: nothing left to serve.
                Err(RecvTimeoutError::Disconnected) => return,
            };
            if let Err(e) = res {
                fail_stop(&e);
            }
        }
    }

    /// Waits for the next message, or times out when the engine's next
    /// deadline or the interval fsync is due.
    fn recv(&self, rx: &mpsc::Receiver<EngineMsg>) -> Result<EngineMsg, RecvTimeoutError> {
        match self.next_wait() {
            Some(wait) => rx.recv_timeout(wait),
            None => rx.recv().map_err(|_| RecvTimeoutError::Disconnected),
        }
    }

    /// How long to wait for the next message: until the engine's next
    /// deadline or the next interval fsync, whichever is first; `None`
    /// means no timer.
    fn next_wait(&self) -> Option<Duration> {
        let engine = self
            .engine
            .next_deadline()
            .map(|at| Duration::from_nanos(at.saturating_sub(self.clock.now())).min(MAX_WAIT));
        let sync = self
            .binlog
            .as_ref()
            .and_then(|b| b.sync_at)
            .map(|at| at.saturating_duration_since(Instant::now()).min(MAX_WAIT));
        match (engine, sync) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    /// Runs one message through the engine, then tick, persist, deliver.
    pub fn on_message(&mut self, msg: EngineMsg) -> Result<(), WalError> {
        let now = self.clock.now();
        self.outbox.clear();
        match msg {
            EngineMsg::Connect { conn, reply_tx } => {
                self.conns.insert(conn, reply_tx);
                self.engine.connect(now, conn);
            }
            EngineMsg::Command { conn, cmd } => self.command(now, conn, cmd),
            EngineMsg::PutStarted { conn, too_big } => {
                self.engine.put_started(now, conn, too_big);
            }
            EngineMsg::PutRejected { conn, why } => {
                self.engine.put_rejected(now, conn, why, &mut self.outbox);
            }
            EngineMsg::HalfClose { conn } => self.engine.half_close(now, conn, &mut self.outbox),
            EngineMsg::Disconnect { conn } => {
                self.engine.disconnect(now, conn, &mut self.outbox);
                self.conns.remove(&conn);
            }
            EngineMsg::SetDraining(on) => {
                self.draining = on;
                self.engine.set_draining(on);
            }
            // Handled by `run`; kept total so a stray one is harmless.
            EngineMsg::Shutdown { done } => {
                let _ = done.send(());
            }
        }
        self.finish(now)
    }

    fn command(&mut self, now: Nanos, conn: ConnId, cmd: Command) {
        if let Command::Put { body, .. } = &cmd
            && !self.draining
            && let Some(b) = self.binlog.as_mut()
        {
            // The actor does not know the connection's used tube, so it
            // reserves for the longest name; `append` releases the excess.
            if !b.log.reserve_put(MAX_TUBE_NAME_LEN, body.len()) {
                self.engine
                    .put_rejected(now, conn, PutRejection::OutOfMemory, &mut self.outbox);
                return;
            }
            b.reserved = true;
        }
        self.engine.handle(now, conn, cmd, &mut self.outbox);
    }

    /// The engine's deadline (or the interval fsync) is due.
    pub fn on_timer(&mut self) -> Result<(), WalError> {
        let now = self.clock.now();
        self.outbox.clear();
        // `finish` ticks.
        if let Some(b) = self.binlog.as_mut()
            && let Some(at) = b.sync_at
        {
            let t = Instant::now();
            if at <= t {
                b.sync_at = None;
                b.log.sync_if_due(t)?;
            }
        }
        self.finish(now)
    }

    /// Tick after the engine call, write its journal, then release its
    /// replies. On error the replies are not delivered.
    fn finish(&mut self, now: Nanos) -> Result<(), WalError> {
        self.engine.tick(now, &mut self.outbox);
        self.persist()?;
        deliver(&self.conns, &mut self.outbox);
        Ok(())
    }

    fn persist(&mut self) -> Result<(), WalError> {
        let Some(b) = self.binlog.as_mut() else {
            return Ok(());
        };
        self.engine.take_journal(&mut self.journal);
        if self.journal.is_empty() && !b.reserved {
            return Ok(());
        }
        b.reserved = false;
        // Also runs with no entries: that releases the put reservation.
        b.log.append(&self.journal)?;
        self.journal.clear();
        b.log.maintain()?;
        let stats = b.log.stats();
        self.engine.set_binlog_stats(stats);
        if stats.records_written != b.records_written {
            b.records_written = stats.records_written;
            if let SyncPolicy::Interval(every) = b.policy {
                let t = Instant::now();
                b.log.sync_if_due(t)?;
                b.sync_at = t.checked_add(every);
            }
        }
        Ok(())
    }

    /// Graceful shutdown: make every write so far durable unless `-F`.
    fn shutdown(&mut self) -> Result<(), WalError> {
        let Some(b) = self.binlog.as_mut() else {
            return Ok(());
        };
        match b.policy {
            // `append` already fsynced every write.
            SyncPolicy::Always | SyncPolicy::Never => Ok(()),
            // The store has no unconditional sync; a time one interval
            // ahead always passes its rate limit (it still skips the fsync
            // when nothing is unsynced).
            SyncPolicy::Interval(every) => {
                let now = Instant::now();
                b.log.sync_if_due(now.checked_add(every).unwrap_or(now))
            }
        }
    }
}

fn fail_stop(e: &WalError) -> ! {
    tracing::error!("binlog failure, exiting without sending pending replies: {e}");
    eprintln!("beanstalkd-rs: binlog failure: {e}");
    std::process::exit(EXIT_WAL_FAILURE);
}

/// Delivers (and removes) every reply in `outbox`, in order, to each
/// connection's reply channel. A missing or closed receiver (connection
/// already gone) is silently ignored: the engine must never block on, or
/// fail because of, a single connection.
fn deliver(conns: &HashMap<ConnId, tokio_mpsc::UnboundedSender<Response>>, outbox: &mut Outbox) {
    for (conn, resp) in outbox.drain(..) {
        if let Some(tx) = conns.get(&conn) {
            let _ = tx.send(resp);
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use std::sync::{Arc, Mutex};

    use bstk_engine::{EngineConfig, StaticSysInfo};
    use bytes::Bytes;

    #[derive(Debug, Clone, PartialEq)]
    enum Ev {
        Reserve(usize, usize),
        Append(usize),
        Sync,
        Maintain,
    }

    #[derive(Clone, Default)]
    struct FakeLog {
        events: Arc<Mutex<Vec<Ev>>>,
        full: bool,
        fail_append: bool,
        written: u64,
    }

    impl Log for FakeLog {
        fn reserve_put(&mut self, tube_len: usize, body_len: usize) -> bool {
            self.events
                .lock()
                .unwrap()
                .push(Ev::Reserve(tube_len, body_len));
            !self.full
        }
        fn append(&mut self, entries: &[JournalEntry]) -> Result<(), WalError> {
            self.events.lock().unwrap().push(Ev::Append(entries.len()));
            if self.fail_append {
                return Err(WalError::Io(std::io::Error::other("disk on fire")));
            }
            self.written += entries.len() as u64;
            Ok(())
        }
        fn sync_if_due(&mut self, _now: Instant) -> Result<(), WalError> {
            self.events.lock().unwrap().push(Ev::Sync);
            Ok(())
        }
        fn maintain(&mut self) -> Result<(), WalError> {
            self.events.lock().unwrap().push(Ev::Maintain);
            Ok(())
        }
        fn stats(&self) -> BinlogStats {
            BinlogStats {
                records_written: self.written,
                ..BinlogStats::default()
            }
        }
    }

    struct Harness {
        actor: Actor<FakeLog>,
        events: Arc<Mutex<Vec<Ev>>>,
        rx: tokio_mpsc::UnboundedReceiver<Response>,
    }

    fn harness(log: FakeLog, journal: bool, policy: SyncPolicy) -> Harness {
        let clock = Clock::start();
        let cfg = EngineConfig {
            journal,
            ..EngineConfig::default()
        };
        let engine = Engine::new(clock.now(), cfg, Box::new(StaticSysInfo::default()));
        let events = log.events.clone();
        let mut actor = Actor::new(clock, engine, Some((log, policy)));
        let (reply_tx, rx) = tokio_mpsc::unbounded_channel();
        actor
            .on_message(EngineMsg::Connect { conn: 1, reply_tx })
            .unwrap();
        Harness { actor, events, rx }
    }

    fn put(body: &'static [u8]) -> EngineMsg {
        EngineMsg::Command {
            conn: 1,
            cmd: Command::Put {
                pri: 0,
                delay: 0,
                ttr: 60,
                body: Bytes::from_static(body),
            },
        }
    }

    fn events(h: &Harness) -> Vec<Ev> {
        std::mem::take(&mut *h.events.lock().unwrap())
    }

    #[test]
    fn put_reserves_for_the_longest_tube_then_appends_before_replying() {
        let mut h = harness(FakeLog::default(), true, SyncPolicy::Never);
        h.actor.on_message(put(b"hello")).unwrap();
        assert_eq!(
            events(&h),
            vec![
                Ev::Reserve(MAX_TUBE_NAME_LEN, 5),
                Ev::Append(1),
                Ev::Maintain
            ]
        );
        assert_eq!(h.rx.try_recv().unwrap(), Response::Inserted(1));
    }

    #[test]
    fn failed_reservation_replies_out_of_memory_without_writing() {
        let log = FakeLog {
            full: true,
            ..FakeLog::default()
        };
        let mut h = harness(log, true, SyncPolicy::Never);
        h.actor.on_message(put(b"hello")).unwrap();
        assert_eq!(events(&h), vec![Ev::Reserve(MAX_TUBE_NAME_LEN, 5)]);
        assert_eq!(h.rx.try_recv().unwrap(), Response::OutOfMemory);
        // The id was consumed at put time, as in the reference.
        h.actor
            .on_message(EngineMsg::Command {
                conn: 1,
                cmd: Command::StatsJob(1),
            })
            .unwrap();
        assert_eq!(h.rx.try_recv().unwrap(), Response::NotFound);
    }

    #[test]
    fn reservation_is_released_by_an_empty_append() {
        // With journaling off the engine writes nothing, but the
        // reservation still has to be consumed by an `append`.
        let mut h = harness(FakeLog::default(), false, SyncPolicy::Never);
        h.actor.on_message(put(b"x")).unwrap();
        assert_eq!(
            events(&h),
            vec![
                Ev::Reserve(MAX_TUBE_NAME_LEN, 1),
                Ev::Append(0),
                Ev::Maintain
            ]
        );
        assert_eq!(h.rx.try_recv().unwrap(), Response::Inserted(1));
    }

    #[test]
    fn draining_put_makes_no_reservation() {
        let mut h = harness(FakeLog::default(), true, SyncPolicy::Never);
        h.actor.on_message(EngineMsg::SetDraining(true)).unwrap();
        h.actor.on_message(put(b"x")).unwrap();
        assert_eq!(events(&h), vec![]);
        assert_eq!(h.rx.try_recv().unwrap(), Response::Draining);
    }

    #[test]
    fn unjournaled_commands_do_not_touch_the_log() {
        let mut h = harness(FakeLog::default(), true, SyncPolicy::Never);
        h.actor
            .on_message(EngineMsg::Command {
                conn: 1,
                cmd: Command::Stats,
            })
            .unwrap();
        assert_eq!(events(&h), vec![]);
        assert!(h.rx.try_recv().is_ok());
    }

    #[test]
    fn append_error_is_returned_and_the_reply_is_withheld() {
        let log = FakeLog {
            fail_append: true,
            ..FakeLog::default()
        };
        let mut h = harness(log, true, SyncPolicy::Never);
        assert!(h.actor.on_message(put(b"x")).is_err());
        assert!(h.rx.try_recv().is_err());
    }

    #[test]
    fn interval_policy_syncs_after_writes_and_arms_the_timer() {
        let every = Duration::from_millis(20);
        let mut h = harness(FakeLog::default(), true, SyncPolicy::Interval(every));
        assert_eq!(h.actor.next_wait(), None);
        h.actor.on_message(put(b"x")).unwrap();
        assert_eq!(
            events(&h),
            vec![
                Ev::Reserve(MAX_TUBE_NAME_LEN, 1),
                Ev::Append(1),
                Ev::Maintain,
                Ev::Sync
            ]
        );
        let wait = h.actor.next_wait().unwrap();
        assert!(wait <= every, "{wait:?}");
        std::thread::sleep(every);
        h.actor.on_timer().unwrap();
        assert_eq!(events(&h), vec![Ev::Sync]);
        assert_eq!(h.actor.next_wait(), None);
        // Shutdown forces one more sync attempt.
        h.actor.shutdown().unwrap();
        assert_eq!(events(&h), vec![Ev::Sync]);
    }

    #[test]
    fn huge_deadlines_do_not_overflow_the_wait() {
        let mut h = harness(FakeLog::default(), true, SyncPolicy::Never);
        h.actor
            .on_message(EngineMsg::Command {
                conn: 1,
                cmd: Command::Put {
                    pri: 0,
                    delay: u32::MAX,
                    ttr: u32::MAX,
                    body: Bytes::from_static(b"x"),
                },
            })
            .unwrap();
        assert_eq!(h.rx.try_recv().unwrap(), Response::Inserted(1));
        assert_eq!(h.actor.next_wait(), Some(MAX_WAIT));
        h.actor.on_timer().unwrap();
    }

    #[test]
    fn far_deadlines_convert_to_a_capped_instant() {
        let clock = Clock::start();
        let before = Instant::now();
        let far = clock.instant_at(u64::MAX);
        assert!(far > before && far <= Instant::now() + MAX_WAIT);
        let past = clock.instant_at(0);
        assert!(past <= Instant::now());
        let soon = clock.instant_at(clock.now() + 1_000_000);
        assert!(soon <= Instant::now() + Duration::from_millis(1));
    }

    #[test]
    fn clock_is_wall_anchored_and_monotonic() {
        let clock = Clock::start();
        let wall = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let a = clock.now();
        let b = clock.now();
        assert!(a <= b);
        assert!(a.abs_diff(wall) < 5 * 1_000_000_000);
    }
}
