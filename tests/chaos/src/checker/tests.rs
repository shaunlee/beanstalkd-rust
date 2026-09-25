//! Hand-made histories: valid ones, each violation kind, and the edge
//! cases around TTR, delays, kicks, disconnects and unacknowledged
//! operations.

#![allow(clippy::unwrap_used)]

use std::time::Duration;

use super::{CheckConfig, ViolationKind, check};
use crate::history::{Cmd, History, JobStateName, Reply};

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

/// A history builder; times in milliseconds.
#[derive(Default)]
struct B {
    h: History,
}

impl B {
    fn new() -> B {
        B::default()
    }

    fn conn(&mut self, c: u64) {
        if !self.h.conns.contains_key(&c) {
            self.h.open_conn(c, Duration::ZERO);
        }
    }

    fn op(&mut self, c: u64, cmd: Cmd, send: u64, reply: Option<(u64, Reply)>) -> &mut B {
        self.conn(c);
        let i = self.h.begin(c, cmd, ms(send));
        if let Some((t, r)) = reply {
            self.h.finish(i, ms(t), r);
        }
        self
    }

    fn put(&mut self, c: u64, body: &str, ttr: u32, delay: u32, at: (u64, u64), id: u64) -> &mut B {
        self.op(
            c,
            put_cmd(body, ttr, delay),
            at.0,
            Some((at.1, Reply::Inserted(id))),
        )
    }

    fn reserve(&mut self, c: u64, at: (u64, u64), id: u64, body: &str) -> &mut B {
        self.op(
            c,
            Cmd::ReserveWithTimeout(5),
            at.0,
            Some((
                at.1,
                Reply::Reserved {
                    id,
                    body: body.as_bytes().to_vec(),
                },
            )),
        )
    }

    fn close(&mut self, c: u64, at: u64) -> &mut B {
        self.h.close_conn(c, ms(at));
        self
    }

    fn check(&self, slack: u64) -> Vec<ViolationKind> {
        let r = check(
            &self.h,
            &CheckConfig {
                slack: ms(slack),
                ..CheckConfig::default()
            },
        );
        if !r.ok() {
            eprintln!("{}", r.summary());
        }
        r.violations.iter().map(|v| v.kind).collect()
    }
}

fn put_cmd(body: &str, ttr: u32, delay: u32) -> Cmd {
    Cmd::Put {
        pri: 0,
        delay,
        ttr,
        body: body.as_bytes().to_vec(),
    }
}

fn found(id: u64, body: &str) -> Reply {
    Reply::Found {
        id,
        body: body.as_bytes().to_vec(),
    }
}

use ViolationKind as V;

// ------------------------------------------------------------------ valid

#[test]
fn valid_put_reserve_delete() {
    let mut b = B::new();
    b.put(1, "a", 60, 0, (0, 10), 1)
        .reserve(2, (20, 30), 1, "a")
        .op(2, Cmd::Touch(1), 40, Some((50, Reply::Touched)))
        .op(3, Cmd::Delete(1), 45, Some((55, Reply::NotFound)))
        .op(2, Cmd::Delete(1), 60, Some((70, Reply::Deleted)))
        .op(3, Cmd::Peek(1), 80, Some((90, Reply::NotFound)));
    assert_eq!(b.check(0), vec![]);
}

#[test]
fn valid_overlapping_operations_in_either_order() {
    // The reserve and the delete overlap: the delete may come first.
    let mut b = B::new();
    b.put(1, "a", 60, 0, (0, 10), 1)
        .op(3, Cmd::Delete(1), 20, Some((100, Reply::Deleted)))
        .op(2, Cmd::Reserve, 30, None)
        .close(2, 200)
        .op(4, Cmd::Peek(1), 300, Some((310, Reply::NotFound)));
    assert_eq!(b.check(0), vec![]);
    // A reserve concurrent with a delete that got NOT_FOUND: the reserve
    // came first.
    let mut b = B::new();
    b.put(1, "a", 60, 0, (0, 10), 1)
        .reserve(2, (20, 100), 1, "a")
        .op(3, Cmd::Delete(1), 30, Some((40, Reply::NotFound)));
    assert_eq!(b.check(0), vec![]);
}

#[test]
fn valid_release_bury_kick_cycle() {
    let mut b = B::new();
    b.put(1, "a", 60, 0, (0, 10), 1)
        .reserve(2, (20, 30), 1, "a")
        .op(
            2,
            Cmd::Release {
                id: 1,
                pri: 0,
                delay: 0,
            },
            40,
            Some((50, Reply::Released)),
        )
        .reserve(3, (60, 70), 1, "a")
        .op(
            2,
            Cmd::Bury { id: 1, pri: 0 },
            80,
            Some((90, Reply::NotFound)),
        )
        .op(
            3,
            Cmd::Bury { id: 1, pri: 0 },
            100,
            Some((110, Reply::Buried)),
        )
        .op(
            4,
            Cmd::StatsJob(1),
            120,
            Some((
                130,
                Reply::JobStats {
                    id: 1,
                    state: JobStateName::Buried,
                },
            )),
        )
        .op(4, Cmd::KickJob(1), 140, Some((150, Reply::KickedJob)))
        .op(4, Cmd::KickJob(1), 160, Some((170, Reply::NotFound)))
        .op(4, Cmd::Peek(1), 180, Some((190, found(1, "a"))));
    assert_eq!(b.check(0), vec![]);
}

// ----------------------------------------------------------- violations

#[test]
fn lost_job() {
    let mut b = B::new();
    b.put(1, "a", 60, 0, (0, 10), 1)
        .op(2, Cmd::Peek(1), 100, Some((110, Reply::NotFound)));
    assert_eq!(b.check(0), vec![V::LostJob]);
}

#[test]
fn missing_job_explained_by_an_unacknowledged_delete() {
    let mut b = B::new();
    b.put(1, "a", 60, 0, (0, 10), 1)
        .op(2, Cmd::Delete(1), 20, None)
        .close(2, 30)
        .op(3, Cmd::Peek(1), 100, Some((110, Reply::NotFound)));
    assert_eq!(b.check(0), vec![]);
}

#[test]
fn resurrected_job() {
    let mut b = B::new();
    b.put(1, "a", 60, 0, (0, 10), 1)
        .op(2, Cmd::Delete(1), 20, Some((30, Reply::Deleted)))
        .op(3, Cmd::Peek(1), 40, Some((50, found(1, "a"))));
    assert_eq!(b.check(0), vec![V::Resurrected]);
    let mut b = B::new();
    b.put(1, "a", 60, 0, (0, 10), 1)
        .op(2, Cmd::Delete(1), 20, Some((30, Reply::Deleted)))
        .reserve(3, (40, 50), 1, "a");
    assert_eq!(b.check(0), vec![V::Resurrected]);
}

#[test]
fn duplicate_id() {
    let mut b = B::new();
    b.put(1, "a", 60, 0, (0, 10), 1)
        .put(2, "b", 60, 0, (5, 15), 1);
    assert!(b.check(0).contains(&V::DuplicateId));
}

#[test]
fn ids_must_increase_in_commit_order() {
    let mut b = B::new();
    b.put(1, "a", 60, 0, (0, 10), 5)
        .put(2, "b", 60, 0, (20, 30), 3);
    assert_eq!(b.check(0), vec![V::IdOrder]);
    // Concurrent puts may get ids in either order.
    let mut b = B::new();
    b.put(1, "a", 60, 0, (0, 30), 5)
        .put(2, "b", 60, 0, (20, 40), 3);
    assert_eq!(b.check(0), vec![]);
}

#[test]
fn exclusive_holding() {
    let mut b = B::new();
    b.put(1, "a", 60, 0, (0, 10), 1)
        .reserve(2, (100, 110), 1, "a")
        .reserve(3, (200, 210), 1, "a")
        .op(2, Cmd::Delete(1), 300, Some((310, Reply::Deleted)));
    assert_eq!(b.check(0), vec![V::ExclusiveHolding]);
}

#[test]
fn duplicate_job_from_one_put() {
    let mut b = B::new();
    b.put(1, "a", 60, 0, (0, 10), 1)
        .reserve(2, (20, 30), 2, "a");
    assert_eq!(b.check(0), vec![V::DuplicateJob]);
}

#[test]
fn not_found_without_explanation() {
    let mut b = B::new();
    b.put(1, "a", 60, 0, (0, 10), 1)
        .op(2, Cmd::Delete(1), 20, Some((30, Reply::NotFound)));
    assert_eq!(b.check(0), vec![V::Inconsistent]);
}

#[test]
fn stats_job_state_must_match() {
    let mut b = B::new();
    b.put(1, "a", 60, 0, (0, 10), 1).op(
        2,
        Cmd::StatsJob(1),
        20,
        Some((
            30,
            Reply::JobStats {
                id: 1,
                state: JobStateName::Buried,
            },
        )),
    );
    assert_eq!(b.check(0), vec![V::Inconsistent]);
}

#[test]
fn unexpected_reply_and_harness_rules() {
    let mut b = B::new();
    b.put(1, "a", 60, 0, (0, 10), 1).op(
        2,
        Cmd::Delete(1),
        20,
        Some((30, Reply::Other("INTERNAL_ERROR".into()))),
    );
    assert!(b.check(0).contains(&V::UnexpectedReply));
    let mut b = B::new();
    b.put(1, "a", 60, 0, (0, 10), 1)
        .op(2, Cmd::Peek(1), 20, None)
        .op(2, Cmd::Peek(1), 30, Some((40, found(1, "a"))));
    assert_eq!(b.check(0), vec![V::Harness]);
    let mut b = B::new();
    b.put(1, "a", 60, 0, (0, 10), 1)
        .put(2, "a", 60, 0, (20, 30), 2);
    assert!(b.check(0).contains(&V::Harness));
}

#[test]
fn reply_for_a_job_no_put_created() {
    let mut b = B::new();
    b.op(1, Cmd::Delete(9), 0, Some((10, Reply::Deleted)));
    assert_eq!(b.check(0), vec![V::Inconsistent]);
    let mut b = B::new();
    b.op(
        1,
        Cmd::Reserve,
        0,
        Some((
            10,
            Reply::Reserved {
                id: 9,
                body: b"zz".to_vec(),
            },
        )),
    );
    assert_eq!(b.check(0), vec![V::Inconsistent]);
}

// ------------------------------------------------------------------ TTR

#[test]
fn ttr_expiry_allows_a_new_reservation() {
    let mut b = B::new();
    b.put(1, "a", 1, 0, (0, 10), 1)
        .reserve(2, (100, 110), 1, "a")
        .reserve(3, (1200, 1210), 1, "a")
        .op(2, Cmd::Delete(1), 1300, Some((1310, Reply::NotFound)))
        .op(3, Cmd::Delete(1), 1400, Some((1410, Reply::Deleted)));
    assert_eq!(b.check(0), vec![]);
}

#[test]
fn late_expiry_is_allowed() {
    // The TTR passed, but the holder acts before any expiry was applied.
    let mut b = B::new();
    b.put(1, "a", 1, 0, (0, 10), 1)
        .reserve(2, (100, 110), 1, "a")
        .op(2, Cmd::Delete(1), 5000, Some((5010, Reply::Deleted)));
    assert_eq!(b.check(0), vec![]);
}

#[test]
fn ttr_boundary() {
    // Reserved at point >= 100, so the TTR (1 s) ends at >= 1100: another
    // reservation acknowledged at 1099 is too early, at 1100 it is fine.
    let early = |reply: u64, slack: u64| {
        let mut b = B::new();
        b.put(1, "a", 1, 0, (0, 10), 1)
            .reserve(2, (100, 110), 1, "a")
            .reserve(3, (900, reply), 1, "a");
        b.check(slack)
    };
    assert_eq!(early(1099, 0), vec![V::Inconsistent]);
    assert_eq!(early(1100, 0), vec![]);
    // The slack widens both intervals: (100 − 50) + 1000 <= 1000 + 50.
    assert_eq!(early(1000, 50), vec![]);
    assert_eq!(early(999, 50), vec![V::Inconsistent]);
}

#[test]
fn ttr_zero_counts_as_one_second() {
    let mut b = B::new();
    b.put(1, "a", 0, 0, (0, 10), 1)
        .reserve(2, (100, 110), 1, "a")
        .reserve(3, (500, 510), 1, "a");
    assert_eq!(b.check(0), vec![V::Inconsistent]);
}

#[test]
fn touch_extends_the_reservation() {
    let run = |second: u64| {
        let mut b = B::new();
        b.put(1, "a", 1, 0, (0, 10), 1)
            .reserve(2, (100, 110), 1, "a")
            .op(2, Cmd::Touch(1), 900, Some((910, Reply::Touched)))
            .reserve(3, (second, second + 10), 1, "a");
        b.check(0)
    };
    assert_eq!(run(1500), vec![V::Inconsistent]);
    assert_eq!(run(1900), vec![]);
}

#[test]
fn holder_cannot_act_after_expiry_and_new_reservation() {
    let mut b = B::new();
    b.put(1, "a", 1, 0, (0, 10), 1)
        .reserve(2, (100, 110), 1, "a")
        .reserve(3, (1200, 1210), 1, "a")
        .op(2, Cmd::Touch(1), 1300, Some((1310, Reply::Touched)));
    assert_eq!(b.check(0), vec![V::ExclusiveHolding]);
}

// ---------------------------------------------------------- disconnects

#[test]
fn disconnect_releases_the_reservation() {
    let mut b = B::new();
    b.put(1, "a", 60, 0, (0, 10), 1)
        .reserve(2, (100, 110), 1, "a")
        .close(2, 150)
        .reserve(3, (200, 210), 1, "a");
    assert_eq!(b.check(0), vec![]);
    // Without the close the job stays reserved for 60 s.
    let mut b = B::new();
    b.put(1, "a", 60, 0, (0, 10), 1)
        .reserve(2, (100, 110), 1, "a")
        .reserve(3, (200, 210), 1, "a");
    assert_eq!(b.check(0), vec![V::Inconsistent]);
}

#[test]
fn disconnect_release_after_the_client_saw_the_close() {
    // After a node crash, the release happens when the cluster drops the
    // node's connections, long after the client saw the reset.
    let mut b = B::new();
    b.put(1, "a", 60, 0, (0, 10), 1)
        .reserve(2, (100, 110), 1, "a")
        .close(2, 150)
        .reserve(3, (140, 20_000), 1, "a");
    assert_eq!(b.check(0), vec![]);
}

#[test]
fn disconnect_not_before_the_last_acknowledged_operation() {
    // Conn 2 was still alive at 300 (another job's op was answered), so it
    // could not have been disconnected before conn 3's reserve ended.
    let mut b = B::new();
    b.put(1, "a", 60, 0, (0, 10), 1)
        .reserve(2, (100, 110), 1, "a")
        .reserve(3, (200, 210), 1, "a")
        .op(2, Cmd::Delete(99), 300, Some((310, Reply::NotFound)))
        .close(2, 400);
    assert_eq!(b.check(0), vec![V::Inconsistent]);
    // If conn 3's reserve ended after 300, the disconnect explains it.
    let mut b = B::new();
    b.put(1, "a", 60, 0, (0, 10), 1)
        .reserve(2, (100, 110), 1, "a")
        .reserve(3, (200, 500), 1, "a")
        .op(2, Cmd::Delete(99), 300, Some((310, Reply::NotFound)))
        .close(2, 400);
    assert_eq!(b.check(0), vec![]);
}

// --------------------------------------------------- unacknowledged ops

#[test]
fn unacknowledged_put_identified_by_body() {
    let mut b = B::new();
    b.op(1, put_cmd("a", 60, 0), 0, None)
        .close(1, 50)
        .reserve(2, (100, 110), 7, "a")
        .op(2, Cmd::Delete(7), 120, Some((130, Reply::Deleted)));
    assert_eq!(b.check(0), vec![]);
    // Reserved before the put was even sent: impossible.
    let mut b = B::new();
    b.op(1, put_cmd("a", 60, 0), 200, None)
        .close(1, 250)
        .reserve(2, (100, 110), 7, "a");
    assert_eq!(b.check(0), vec![V::Inconsistent]);
}

#[test]
fn unacknowledged_reserve_explains_not_found() {
    let mut b = B::new();
    b.put(1, "a", 60, 0, (0, 10), 1)
        .op(2, Cmd::Reserve, 20, None)
        .close(2, 30)
        .op(3, Cmd::Delete(1), 40, Some((50, Reply::NotFound)))
        .op(3, Cmd::Delete(1), 60, Some((70, Reply::Deleted)));
    assert_eq!(b.check(0), vec![]);
}

#[test]
fn unacknowledged_release_may_or_may_not_have_happened() {
    for later in [Reply::Deleted, Reply::NotFound] {
        let mut b = B::new();
        b.put(1, "a", 60, 0, (0, 10), 1)
            .reserve(2, (20, 30), 1, "a")
            .op(
                2,
                Cmd::Release {
                    id: 1,
                    pri: 0,
                    delay: 0,
                },
                40,
                None,
            )
            .op(3, Cmd::Delete(1), 50, Some((60, later)));
        // Without a close, NOT_FOUND means the release did not happen yet.
        assert_eq!(b.check(0), vec![]);
    }
}

// ---------------------------------------------------- delays and kicks

#[test]
fn delayed_job_not_reservable_before_its_delay() {
    let mut b = B::new();
    b.put(1, "a", 60, 2, (0, 10), 1)
        .reserve(2, (500, 510), 1, "a");
    assert_eq!(b.check(0), vec![V::Inconsistent]);
    let mut b = B::new();
    b.put(1, "a", 60, 2, (0, 10), 1)
        .reserve(2, (500, 2100), 1, "a");
    assert_eq!(b.check(0), vec![]);
    let mut b = B::new();
    b.put(1, "a", 60, 2, (0, 10), 1)
        .op(3, Cmd::KickJob(1), 100, Some((110, Reply::KickedJob)))
        .reserve(2, (500, 510), 1, "a");
    assert_eq!(b.check(0), vec![]);
}

#[test]
fn released_with_delay() {
    let run = |at: u64| {
        let mut b = B::new();
        b.put(1, "a", 60, 0, (0, 10), 1)
            .reserve(2, (20, 30), 1, "a")
            .op(
                2,
                Cmd::Release {
                    id: 1,
                    pri: 0,
                    delay: 1,
                },
                100,
                Some((110, Reply::Released)),
            )
            .reserve(3, (at, at + 10), 1, "a");
        b.check(0)
    };
    assert_eq!(run(500), vec![V::Inconsistent]);
    assert_eq!(run(1100), vec![]);
}

#[test]
fn bulk_kick_may_have_kicked_the_job() {
    let run = |kicked: Option<u64>| {
        let mut b = B::new();
        b.put(1, "a", 60, 0, (0, 10), 1)
            .reserve(2, (20, 30), 1, "a")
            .op(
                2,
                Cmd::Bury { id: 1, pri: 0 },
                40,
                Some((50, Reply::Buried)),
            );
        if let Some(n) = kicked {
            b.op(3, Cmd::Kick(10), 60, Some((70, Reply::Kicked(n))));
        }
        b.reserve(4, (100, 110), 1, "a");
        b.check(0)
    };
    assert_eq!(run(Some(1)), vec![]);
    assert_eq!(run(Some(0)), vec![V::Inconsistent]);
    assert_eq!(run(None), vec![V::Inconsistent]);
}

#[test]
fn many_unacknowledged_reserves_stay_fast() {
    // Twenty lost reserves in every job's history; the search must stay
    // small both when the history is valid and when it is not.
    for bad in [false, true] {
        let mut b = B::new();
        for j in 0..10u64 {
            b.put(1, &format!("j{j}"), 60, 0, (j * 10, j * 10 + 5), j + 1);
        }
        for c in 0..20u64 {
            b.op(100 + c, Cmd::Reserve, 200 + c, None)
                .close(100 + c, 300);
        }
        for j in 0..10u64 {
            b.op(
                2,
                Cmd::Peek(j + 1),
                1000 + j * 10,
                Some((1005 + j * 10, found(j + 1, &format!("j{j}")))),
            );
        }
        if bad {
            b.op(3, Cmd::Delete(1), 2000, Some((2010, Reply::Deleted)));
            b.op(3, Cmd::Peek(1), 2100, Some((2110, found(1, "j0"))));
        }
        let r = check(&b.h, &CheckConfig::default());
        assert_eq!(r.ok(), !bad, "{}", r.summary());
        assert!(r.states < 200_000, "{} states", r.states);
    }
}

#[test]
fn blocked_reserve_woken_by_a_later_put() {
    // The reservation happens at the put's point (>= 500), so its TTR ends
    // at >= 1500 even though the reserve was sent at 100.
    let run = |third: Option<u64>| {
        let mut b = B::new();
        b.op(
            2,
            Cmd::ReserveWithTimeout(10),
            100,
            Some((
                512,
                Reply::Reserved {
                    id: 5,
                    body: b"a".to_vec(),
                },
            )),
        )
        .put(1, "a", 1, 0, (500, 510), 5);
        if let Some(t) = third {
            b.reserve(3, (t, t + 10), 5, "a");
        }
        b.check(0)
    };
    assert_eq!(run(None), vec![]);
    assert_eq!(run(Some(1500)), vec![]);
    assert_eq!(run(Some(1300)), vec![V::Inconsistent]);
}

#[test]
fn one_bulk_kick_may_serve_several_jobs() {
    let mut b = B::new();
    b.put(1, "a", 60, 0, (0, 10), 1)
        .put(1, "b", 60, 0, (20, 30), 2)
        .reserve(2, (40, 50), 1, "a")
        .op(
            2,
            Cmd::Bury { id: 1, pri: 0 },
            60,
            Some((70, Reply::Buried)),
        )
        .reserve(2, (80, 90), 2, "b")
        .op(
            2,
            Cmd::Bury { id: 2, pri: 0 },
            100,
            Some((110, Reply::Buried)),
        )
        .op(3, Cmd::Kick(10), 200, Some((210, Reply::Kicked(2))))
        .reserve(4, (300, 310), 1, "a")
        .reserve(4, (320, 330), 2, "b");
    assert_eq!(b.check(0), vec![]);
}

#[test]
fn holder_acts_after_its_ttr_while_others_saw_it_reserved() {
    let mut b = B::new();
    b.put(1, "a", 1, 0, (0, 10), 1)
        .reserve(2, (100, 110), 1, "a")
        .op(3, Cmd::Delete(1), 1500, Some((1510, Reply::NotFound)))
        .op(2, Cmd::Delete(1), 2000, Some((2010, Reply::Deleted)));
    assert_eq!(b.check(0), vec![]);
}
