//! Chaos testing of cluster mode (P3-T6, docs/PLAN.md §6.5, §6.6).
//!
//! - [`history`], [`checker`]: client histories and their verification;
//! - [`workload`]: the client operation mix shared by both harnesses;
//! - [`inproc`]: the in-process harness (simulated network, real storage);
//! - [`mp`], [`proxy`], [`client`]: the multi-process harness (real
//!   `beanstalkd-rs` processes, a pausable TCP proxy per directed cluster
//!   link, a small beanstalk client).

pub mod checker;
pub mod client;
pub mod history;
pub mod inproc;
pub mod mp;
pub mod proxy;
pub mod workload;

/// Runs `f` over `items` on up to `jobs` threads, returning the results in
/// item order.
pub fn parallel<T: Send, R: Send>(items: Vec<T>, jobs: usize, f: impl Fn(T) -> R + Sync) -> Vec<R> {
    let n = items.len();
    let queue = std::sync::Mutex::new(items.into_iter().enumerate().collect::<Vec<_>>());
    let results = std::sync::Mutex::new(Vec::with_capacity(n));
    std::thread::scope(|s| {
        for _ in 0..jobs.max(1) {
            s.spawn(|| {
                loop {
                    let next = queue.lock().unwrap_or_else(|e| e.into_inner()).pop();
                    let Some((i, item)) = next else { return };
                    let r = f(item);
                    results
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .push((i, r));
                }
            });
        }
    });
    let mut v = results.into_inner().unwrap_or_else(|e| e.into_inner());
    v.sort_by_key(|(i, _)| *i);
    v.into_iter().map(|(_, r)| r).collect()
}
