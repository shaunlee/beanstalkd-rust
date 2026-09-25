//! The client workload shared by both harnesses: a small random mix of
//! put / reserve / delete / release / bury / kick / touch / peek with short
//! TTRs, chosen per connection from what it holds.
//!
//! Rules the checker relies on (see [`crate::checker`]):
//! - every put body is unique in the run (`<run>-<conn>-<n>`);
//! - one command in flight per connection;
//! - after a command without a reply, the connection is closed and never
//!   used again (the harnesses record the close);
//! - `reserve` without a timeout is rare, and every client has an op
//!   timeout longer than any reserve timeout.

use std::collections::BTreeSet;

use bstk_raft::sim::SimRng;

use crate::history::{Cmd, ConnKey, JobId, Reply};

/// Workload shape.
#[derive(Debug, Clone)]
pub struct WorkloadConfig {
    /// Maximum number of puts per run (keeps per-job histories short).
    pub max_puts: u64,
    /// TTR range, seconds.
    pub ttr: (u32, u32),
    /// Longest `reserve-with-timeout`, seconds.
    pub max_reserve_timeout: u32,
}

impl Default for WorkloadConfig {
    fn default() -> Self {
        WorkloadConfig {
            max_puts: 60,
            ttr: (1, 3),
            max_reserve_timeout: 2,
        }
    }
}

/// Job ids seen by any client of the run (targets for peek, kick-job,
/// deletes of jobs held by others, and the final verification).
#[derive(Debug, Default)]
pub struct Known {
    pub ids: BTreeSet<JobId>,
    pub puts: u64,
}

/// One connection's workload state.
#[derive(Debug)]
pub struct ClientState {
    run: u64,
    conn: ConnKey,
    n: u64,
    /// Jobs this connection holds (as far as it knows).
    held: Vec<JobId>,
}

impl ClientState {
    pub fn new(run: u64, conn: ConnKey) -> ClientState {
        ClientState {
            run,
            conn,
            n: 0,
            held: Vec::new(),
        }
    }

    /// The next command. `known` is shared by the run's clients.
    pub fn next(&mut self, r: &mut SimRng, cfg: &WorkloadConfig, known: &mut Known) -> Cmd {
        let pick_known = |r: &mut SimRng, known: &Known| -> Option<JobId> {
            if known.ids.is_empty() {
                return None;
            }
            // Prefer recent jobs.
            let n = known.ids.len() as u64;
            let back = r.range(0, n.min(12) - 1) as usize;
            known.ids.iter().rev().nth(back).copied()
        };
        loop {
            let roll = r.range(0, 99);
            let held = (!self.held.is_empty())
                .then(|| self.held[r.range(0, self.held.len() as u64 - 1) as usize]);
            let cmd = match roll {
                0..=24 => {
                    if known.puts >= cfg.max_puts {
                        continue;
                    }
                    known.puts += 1;
                    self.n += 1;
                    let delay = if r.range(0, 9) == 0 { 1 } else { 0 };
                    Some(Cmd::Put {
                        pri: r.range(0, 3) as u32,
                        delay,
                        ttr: r.range(u64::from(cfg.ttr.0), u64::from(cfg.ttr.1)) as u32,
                        body: format!("{}-{}-{}", self.run, self.conn, self.n).into_bytes(),
                    })
                }
                25..=44 => {
                    if self.held.len() >= 2 {
                        continue;
                    }
                    Some(Cmd::ReserveWithTimeout(
                        r.range(0, u64::from(cfg.max_reserve_timeout)) as u32,
                    ))
                }
                45..=46 => {
                    if self.held.len() >= 2 {
                        continue;
                    }
                    Some(Cmd::Reserve)
                }
                47..=62 => held.map(Cmd::Delete),
                63..=70 => held.map(|id| Cmd::Release {
                    id,
                    pri: r.range(0, 3) as u32,
                    delay: if r.range(0, 4) == 0 { 1 } else { 0 },
                }),
                71..=76 => held.map(|id| Cmd::Bury { id, pri: 1 }),
                77..=82 => held.map(Cmd::Touch),
                83..=86 => pick_known(r, known).map(Cmd::KickJob),
                87..=88 => Some(Cmd::Kick(r.range(1, 3) as u32)),
                89..=92 => pick_known(r, known).map(Cmd::Peek),
                93..=95 => pick_known(r, known).map(Cmd::StatsJob),
                _ => pick_known(r, known).map(Cmd::Delete),
            };
            if let Some(c) = cmd {
                return c;
            }
        }
    }

    /// Learns from a reply.
    pub fn observe(&mut self, cmd: &Cmd, reply: &Reply, known: &mut Known) {
        match (cmd, reply) {
            (_, Reply::Inserted(id) | Reply::BuriedId(id)) => {
                known.ids.insert(*id);
            }
            (_, Reply::Reserved { id, .. }) => {
                known.ids.insert(*id);
                if !self.held.contains(id) {
                    self.held.push(*id);
                }
            }
            (
                Cmd::Delete(id) | Cmd::Release { id, .. } | Cmd::Bury { id, .. },
                Reply::Deleted | Reply::Released | Reply::Buried | Reply::NotFound,
            )
            | (Cmd::Touch(id), Reply::NotFound) => {
                self.held.retain(|h| h != id);
            }
            _ => {}
        }
    }
}
