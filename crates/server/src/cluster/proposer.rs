//! Leader-side proposal batching (P3-FD).
//!
//! openraft 0.9 appends (and syncs) each proposal on its own, so the
//! leader proposes connection inputs in [`Op::Batch`] entries: the cluster
//! actor (this node's own inputs) and the forward handler (other owners'
//! inputs) hand their items to one proposer task, in order, and the
//! proposer turns everything queued into as few entries as the bounds
//! allow ([`bstk_raft::split_batches`]: at most
//! [`bstk_raft::MAX_PROPOSAL_ITEMS`] items and
//! [`bstk_raft::MAX_PROPOSAL_BYTES`] per entry, a single larger item alone).
//!
//! # Proposal rounds
//!
//! At most [`MAX_INFLIGHT`] batches are outstanding (proposed, and not yet
//! applied on this node or refused); further items wait in the proposer's
//! queue and go into the next batch. So while the Raft core is busy with a
//! log write, inputs accumulate instead of becoming one entry each. An
//! outstanding batch stops counting after [`INFLIGHT_WAIT`] (a proposal
//! whose result never arrives, for example after losing leadership) or
//! when the view (leader, term) changes.
//!
//! # Order and resends
//!
//! The queue is FIFO over all submissions, and each submission keeps its
//! items' order, so every owner's items reach the log in the order the
//! owner sent them, which the state machine's dedup rule and the actor's
//! resend rules (see [`super::actor`]) rely on. Items are proposed whether
//! or not this node still leads: openraft refuses proposals on a
//! non-leader, and the owners resend whatever is not applied (as before
//! P3-FD, where a forward answered `Accepted` was not guaranteed to
//! commit either).

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use futures::stream::FuturesUnordered;
use tokio::sync::mpsc;

use bstk_engine::EngineInput;
use bstk_raft::{MAX_PROPOSAL_BYTES, MAX_PROPOSAL_ITEMS, Op, proposal_item_size};

use super::Core;

/// Most batches outstanding at once (see the module docs). Measured on
/// one machine (P3-FD, 3 nodes, put-reserve-delete, 100 connections via
/// the leader): 1 gives about 133k ops/s, 2 about 120k, 4 about 90-120k,
/// and 64 (effectively unbounded) 35k, as nearly every batch then holds
/// one input again.
pub const MAX_INFLIGHT: usize = 1;

/// An outstanding batch stops counting after this long.
pub const INFLIGHT_WAIT: Duration = Duration::from_millis(500);

/// Items `(seq, input)` handed to the proposer, in order.
pub type Items = Vec<(u64, EngineInput)>;

/// The proposer's queue (FIFO over all submissions).
#[derive(Default)]
struct Queue {
    items: VecDeque<(u64, EngineInput)>,
}

impl Queue {
    fn extend(&mut self, items: Items) {
        self.items.extend(items);
    }

    fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// The next batch: at most `MAX_PROPOSAL_ITEMS` items and
    /// `MAX_PROPOSAL_BYTES`, unless the first item alone is larger.
    fn next_batch(&mut self) -> Items {
        let mut out = Vec::new();
        let mut bytes = 0;
        while let Some((_, input)) = self.items.front() {
            let size = proposal_item_size(input);
            if !out.is_empty()
                && (out.len() >= MAX_PROPOSAL_ITEMS || bytes + size > MAX_PROPOSAL_BYTES)
            {
                break;
            }
            bytes += size;
            if let Some(item) = self.items.pop_front() {
                out.push(item);
            }
        }
        out
    }
}

type Outstanding = Pin<Box<dyn Future<Output = ()> + Send>>;

/// Runs the proposer until every submitter is gone or Raft stops.
pub async fn run(core: Arc<Core>, mut rx: mpsc::UnboundedReceiver<Items>) {
    let mut metrics = core.raft.metrics();
    let mut view = core.view();
    let mut queue = Queue::default();
    let mut outstanding: FuturesUnordered<Outstanding> = FuturesUnordered::new();
    loop {
        tokio::select! {
            items = rx.recv() => {
                let Some(items) = items else { return };
                queue.extend(items);
                while let Ok(items) = rx.try_recv() {
                    queue.extend(items);
                }
            }
            Some(()) = outstanding.next(), if !outstanding.is_empty() => {}
            changed = metrics.changed() => {
                if changed.is_err() {
                    return;
                }
                let v = core.view();
                if v != view {
                    view = v;
                    outstanding.clear();
                }
            }
        }
        while outstanding.len() < MAX_INFLIGHT && !queue.is_empty() {
            let batch = queue.next_batch();
            match core.propose_tracked(Op::Batch(batch)).await {
                Ok(done) => outstanding.push(Box::pin(async move {
                    let _ = tokio::time::timeout(INFLIGHT_WAIT, done).await;
                })),
                // Raft has stopped.
                Err(_) => return,
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use bstk_proto::Command;

    fn put(conn: u64, len: usize) -> EngineInput {
        EngineInput::Command {
            conn,
            cmd: Command::Put {
                pri: 0,
                delay: 0,
                ttr: 1,
                body: bytes::Bytes::from(vec![b'x'; len]),
            },
        }
    }

    #[test]
    fn batches_respect_the_item_and_byte_bounds_in_order() {
        let mut q = Queue::default();
        q.extend((1..=2500).map(|i| (i, EngineInput::Connect(i))).collect());
        let sizes: Vec<usize> = std::iter::from_fn(|| {
            let b = q.next_batch();
            (!b.is_empty()).then_some(b.len())
        })
        .collect();
        assert_eq!(sizes, [1024, 1024, 452]);

        // Bytes: 300 KiB bodies, three per MiB; a 3 MiB body goes alone.
        let mut q = Queue::default();
        let mut items: Items = (1..=4).map(|i| (i, put(1, 300 << 10))).collect();
        items.push((5, put(1, 3 << 20)));
        items.push((6, put(1, 10)));
        q.extend(items);
        let batches: Vec<Vec<u64>> = std::iter::from_fn(|| {
            let b = q.next_batch();
            (!b.is_empty()).then(|| b.iter().map(|(s, _)| *s).collect())
        })
        .collect();
        assert_eq!(batches, [vec![1, 2, 3], vec![4], vec![5], vec![6]]);
    }
}
