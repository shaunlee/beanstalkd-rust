//! Latency recording: raw nanosecond samples per operation kind, merged
//! and sorted at the end (simple and exact; memory is 8 bytes per op).

use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Put,
    Reserve,
    Delete,
}

impl Op {
    pub const ALL: [Op; 3] = [Op::Put, Op::Reserve, Op::Delete];

    pub fn name(self) -> &'static str {
        match self {
            Op::Put => "put",
            Op::Reserve => "reserve",
            Op::Delete => "delete",
        }
    }

    fn index(self) -> usize {
        match self {
            Op::Put => 0,
            Op::Reserve => 1,
            Op::Delete => 2,
        }
    }
}

/// Per-connection samples, merged into one after the run.
#[derive(Debug, Default)]
pub struct Recorder {
    samples: [Vec<u64>; 3],
}

impl Recorder {
    pub fn record(&mut self, op: Op, d: Duration) {
        let nanos = u64::try_from(d.as_nanos()).unwrap_or(u64::MAX);
        self.samples[op.index()].push(nanos);
    }

    pub fn merge(&mut self, other: Recorder) {
        for (dst, src) in self.samples.iter_mut().zip(other.samples) {
            dst.extend(src);
        }
    }

    pub fn count(&self, op: Op) -> usize {
        self.samples[op.index()].len()
    }

    pub fn total(&self) -> usize {
        self.samples.iter().map(Vec::len).sum()
    }

    /// Sorts the samples and returns the summary for `op`.
    pub fn summary(&mut self, op: Op) -> Option<Summary> {
        let v = &mut self.samples[op.index()];
        if v.is_empty() {
            return None;
        }
        v.sort_unstable();
        Some(Summary {
            count: v.len(),
            p50: percentile(v, 500),
            p99: percentile(v, 990),
            p999: percentile(v, 999),
            max: v[v.len() - 1],
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Summary {
    pub count: usize,
    pub p50: u64,
    pub p99: u64,
    pub p999: u64,
    pub max: u64,
}

/// Nearest-rank percentile of an ascending, non-empty slice, with the
/// percentile given in per mille (990 = p99) to keep the math exact.
fn percentile(sorted: &[u64], per_mille: usize) -> u64 {
    let n = sorted.len();
    let rank = (per_mille * n).div_ceil(1000);
    sorted[rank.clamp(1, n) - 1]
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn percentiles_use_nearest_rank() {
        let v: Vec<u64> = (1..=1000).collect();
        assert_eq!(percentile(&v, 500), 500);
        assert_eq!(percentile(&v, 990), 990);
        assert_eq!(percentile(&v, 999), 999);
        assert_eq!(percentile(&[5], 999), 5);
    }

    #[test]
    fn merge_and_summary() {
        let mut a = Recorder::default();
        let mut b = Recorder::default();
        a.record(Op::Put, Duration::from_nanos(30));
        b.record(Op::Put, Duration::from_nanos(10));
        b.record(Op::Delete, Duration::from_nanos(20));
        a.merge(b);
        assert_eq!(a.count(Op::Put), 2);
        assert_eq!(a.total(), 3);
        let s = a.summary(Op::Put).unwrap();
        assert_eq!((s.p50, s.max), (10, 30));
        assert!(a.summary(Op::Reserve).is_none());
    }
}
