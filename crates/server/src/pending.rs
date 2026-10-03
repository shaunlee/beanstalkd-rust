//! Pending TLS connections and their server-side counters (P2 security
//! review, finding F1).
//!
//! A TLS connection is *pending* while its handshake runs and, on `auth =
//! "token"` listeners, until the client has authenticated. Pending connections
//! are invisible to the engine (never in `stats`), so they are counted here
//! across all TLS listeners and capped at `server.max_pending_connections`;
//! past the cap new TLS connections are closed at once, so unauthenticated
//! clients cannot use up the process's file descriptors (which would take
//! down every listener, HTTP included). Plaintext listeners never touch any
//! of this.
//!
//! The counters are exported by the HTTP listener (`/metrics`, the
//! `"server_rs"` object of `/admin`), never through `stats`, which stays
//! byte-identical to the reference.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use crate::metrics::ServerRsStats;

/// At most one "limit reached" log line per this interval.
const REJECT_LOG_INTERVAL: Duration = Duration::from_secs(1);

/// Server-side counters shared by the TLS accept loops, the TLS
/// connection tasks and the HTTP listener.
#[derive(Debug)]
pub struct ServerCounters {
    max_pending: usize,
    pending: AtomicUsize,
    pending_rejected: AtomicU64,
    auth_timeouts: AtomicU64,
    auth_failures: AtomicU64,
    reject_log: Mutex<RejectLog>,
}

#[derive(Debug, Default)]
struct RejectLog {
    last: Option<Instant>,
    unreported: u64,
}

impl RejectLog {
    /// Records one rejection at `now`; returns how many rejections the
    /// caller should report now (this one included), or `None` while the
    /// last line is less than [`REJECT_LOG_INTERVAL`] old.
    fn note(&mut self, now: Instant) -> Option<u64> {
        self.unreported = self.unreported.saturating_add(1);
        let due = self
            .last
            .is_none_or(|t| now.saturating_duration_since(t) >= REJECT_LOG_INTERVAL);
        if !due {
            return None;
        }
        self.last = Some(now);
        Some(std::mem::take(&mut self.unreported))
    }
}

impl ServerCounters {
    /// `max_pending` is `server.max_pending_connections` (at least 1).
    pub fn new(max_pending: usize) -> Arc<ServerCounters> {
        Arc::new(ServerCounters {
            max_pending,
            pending: AtomicUsize::new(0),
            pending_rejected: AtomicU64::new(0),
            auth_timeouts: AtomicU64::new(0),
            auth_failures: AtomicU64::new(0),
            reject_log: Mutex::new(RejectLog::default()),
        })
    }

    /// Counts a newly accepted TLS connection as pending, or, when the cap
    /// is reached, counts (and, rate-limited, logs) a rejection and
    /// returns `None`: the caller must close the connection.
    pub fn try_acquire(self: &Arc<Self>) -> Option<PendingGuard> {
        let acquired = self
            .pending
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < self.max_pending).then_some(n + 1)
            })
            .is_ok();
        if acquired {
            return Some(PendingGuard {
                counters: Arc::clone(self),
            });
        }
        self.pending_rejected.fetch_add(1, Ordering::Relaxed);
        let report = self
            .reject_log
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .note(Instant::now());
        if let Some(n) = report {
            tracing::info!(
                rejected = n,
                limit = self.max_pending,
                "too many pending TLS connections (server.max_pending_connections): \
                 closed {n} new connection(s)"
            );
        }
        None
    }

    pub fn auth_timed_out(&self) {
        self.auth_timeouts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn auth_failed(&self) {
        self.auth_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub fn sample(&self) -> ServerRsStats {
        ServerRsStats {
            pending_connections: self.pending.load(Ordering::Acquire) as u64,
            pending_rejected: self.pending_rejected.load(Ordering::Relaxed),
            auth_timeouts: self.auth_timeouts.load(Ordering::Relaxed),
            auth_failures: self.auth_failures.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug)]
pub struct PendingGuard {
    counters: Arc<ServerCounters>,
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        self.counters.pending.fetch_sub(1, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cap_rejects_and_recovers() {
        let c = ServerCounters::new(2);
        let a = c.try_acquire().expect("first");
        let b = c.try_acquire().expect("second");
        assert_eq!(c.sample().pending_connections, 2);
        assert!(c.try_acquire().is_none());
        assert!(c.try_acquire().is_none());
        assert_eq!(c.sample().pending_rejected, 2);
        assert_eq!(c.sample().pending_connections, 2);
        drop(a);
        assert_eq!(c.sample().pending_connections, 1);
        let a = c.try_acquire().expect("room again");
        drop((a, b));
        assert_eq!(c.sample().pending_connections, 0);
        assert_eq!(c.sample().pending_rejected, 2);
    }

    #[test]
    fn cap_holds_under_concurrency() {
        let c = ServerCounters::new(8);
        let held: Vec<_> = std::thread::scope(|s| {
            let handles: Vec<_> = (0..32).map(|_| s.spawn(|| c.try_acquire())).collect();
            handles
                .into_iter()
                .filter_map(|h| h.join().ok().flatten())
                .collect()
        });
        assert_eq!(held.len(), 8);
        let s = c.sample();
        assert_eq!((s.pending_connections, s.pending_rejected), (8, 24));
        drop(held);
        assert_eq!(c.sample().pending_connections, 0);
    }

    #[test]
    fn auth_counters() {
        let c = ServerCounters::new(1);
        c.auth_failed();
        c.auth_failed();
        c.auth_timed_out();
        let s = c.sample();
        assert_eq!((s.auth_failures, s.auth_timeouts), (2, 1));
    }

    #[test]
    fn reject_log_is_rate_limited_and_counts() {
        let mut log = RejectLog::default();
        let t0 = Instant::now();
        assert_eq!(log.note(t0), Some(1));
        for i in 1..=10 {
            assert_eq!(log.note(t0 + Duration::from_millis(i * 50)), None);
        }
        assert_eq!(log.note(t0 + Duration::from_secs(1)), Some(11));
        assert_eq!(log.note(t0 + Duration::from_millis(1500)), None);
        assert_eq!(log.note(t0 + Duration::from_millis(2100)), Some(2));
    }
}
