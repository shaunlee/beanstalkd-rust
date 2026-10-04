//! The commands against scripted admin endpoints (plaintext): redirects,
//! conflicts, refusals, polling and the timeout paths, which a real cluster
//! cannot be made to produce on demand. The real thing is
//! `tests/cluster.rs` (`cli_*`).

#![allow(clippy::unwrap_used)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bstk_raft::NodeId;
use bstk_raft::status::{MembershipView, NodeStatus, NodeStatusEx};
use bstk_raft::wire::{
    self, AdminRequest, AdminResponse, ClientMsg, PROTOCOL_VERSION, ServerHello, ServerMsg,
};
use openraft::{CommittedLeaderId, LogId};
use tokio::net::TcpListener;

use super::args::{ClusterCmd, Settings};
use super::drive::{Fail, Operator, Role};
use super::link::Auth;
use super::render;

type Script = Box<dyn FnMut(&AdminRequest) -> AdminResponse + Send>;

/// A node that speaks the admin channel and answers by `script`.
struct Stub {
    addr: String,
    seen: Arc<Mutex<Vec<AdminRequest>>>,
}

impl Stub {
    async fn start(
        node: NodeId,
        script: impl FnMut(&AdminRequest) -> AdminResponse + Send + 'static,
    ) -> Stub {
        Self::start_with(node, None, Box::new(script)).await
    }

    /// `reject`: answer the hello with this refusal.
    async fn start_with(node: NodeId, reject: Option<&'static str>, script: Script) -> Stub {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let script = Arc::new(Mutex::new(script));
        let log = seen.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut io, _)) = listener.accept().await else {
                    return;
                };
                let (script, log) = (script.clone(), log.clone());
                tokio::spawn(async move {
                    let max = wire::DEFAULT_MAX_FRAME;
                    let Ok(Some(ClientMsg::AdminHello(_))) = wire::read_frame(&mut io, max).await
                    else {
                        return;
                    };
                    let hello = match reject {
                        Some(reason) => ServerHello::Rejected {
                            reason: reason.into(),
                        },
                        None => ServerHello::Accepted {
                            version: PROTOCOL_VERSION,
                            node_id: node,
                            max_job_size: 0,
                        },
                    };
                    let f = wire::encode(&ServerMsg::Hello(hello), max).unwrap();
                    if wire::write_frame(&mut io, &f).await.is_err() || reject.is_some() {
                        return;
                    }
                    while let Ok(Some(ClientMsg::Admin { id, body })) =
                        wire::read_frame::<_, ClientMsg>(&mut io, max).await
                    {
                        log.lock().unwrap().push(body.clone());
                        let answer = (script.lock().unwrap())(&body);
                        let f = wire::encode(&ServerMsg::Admin { id, body: answer }, max).unwrap();
                        if wire::write_frame(&mut io, &f).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        Stub { addr, seen }
    }

    fn changes(&self) -> Vec<AdminRequest> {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.is_change())
            .cloned()
            .collect()
    }
}

fn lid(index: u64) -> Option<LogId<NodeId>> {
    Some(LogId::new(CommittedLeaderId::new(1, 1), index))
}

struct V {
    leader: Option<NodeId>,
    index: u64,
    committed: bool,
    configs: Vec<Vec<NodeId>>,
    nodes: Vec<(NodeId, String)>,
    applied: u64,
}

impl V {
    fn build(&self) -> NodeStatusEx {
        NodeStatusEx {
            status: NodeStatus::default(),
            raft_running: true,
            rejoining: false,
            term: 3,
            leader: self.leader,
            last_applied: lid(self.applied),
            highest_member: self.nodes.iter().map(|n| n.0).max().unwrap_or(0),
            membership: MembershipView {
                log_id: lid(self.index),
                committed: self.committed,
                configs: self
                    .configs
                    .iter()
                    .map(|c| c.iter().copied().collect::<BTreeSet<_>>())
                    .collect(),
                nodes: self.nodes.iter().cloned().collect::<BTreeMap<_, _>>(),
            },
        }
    }

    fn answer(&self) -> AdminResponse {
        AdminResponse::Membership(Box::new(self.build()))
    }
}

fn nodes(addrs: &[(NodeId, &str)]) -> Vec<(NodeId, String)> {
    addrs.iter().map(|(i, a)| (*i, (*a).to_owned())).collect()
}

fn settings(seeds: Vec<String>, timeout: Duration) -> Settings {
    Settings {
        seeds,
        auth: Auth::Plain,
        timeout,
        json: false,
    }
}

fn add(id: NodeId, addr: &str) -> ClusterCmd {
    ClusterCmd::Add {
        id,
        addr: addr.into(),
    }
}

const SECS: Duration = Duration::from_secs(1);

#[tokio::test]
async fn status_reports_roles_lag_and_unreachable_nodes() {
    let closed = {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        l.local_addr().unwrap().to_string()
    };
    // Node 1 leads (applied 100), 2 lags (90), 3 is a learner that does not
    // answer. The scripts read the addresses once all are known.
    let addrs: Arc<Mutex<Vec<(NodeId, String)>>> = Arc::default();
    let script = |applied| {
        let addrs = addrs.clone();
        move |_: &AdminRequest| {
            V {
                leader: Some(1),
                index: 7,
                committed: true,
                configs: vec![vec![1, 2]],
                nodes: addrs.lock().unwrap().clone(),
                applied,
            }
            .answer()
        }
    };
    let leader = Stub::start(1, script(100)).await;
    let follower = Stub::start(2, script(90)).await;
    *addrs.lock().unwrap() = nodes(&[(1, &leader.addr), (2, &follower.addr), (3, &closed)]);

    // Asked through the follower: its view names the leader, whose own view
    // is the one used.
    let st = settings(vec![follower.addr.clone()], 5 * SECS);
    let report = Operator::new(&st).status().await.unwrap();
    assert_eq!(report.view.leader, Some(1));
    assert_eq!(report.view.last_applied, lid(100));
    let by_id = |id| report.nodes.iter().find(|n| n.id == id).unwrap();
    assert_eq!(by_id(1).role, Role::Leader);
    assert_eq!(by_id(2).role, Role::Voter);
    assert_eq!(by_id(3).role, Role::Learner);
    assert!(by_id(3).state.is_err());
    assert_eq!(report.lag(by_id(2)), Some(10));
    assert_eq!(report.lag(by_id(1)), Some(0));
    assert_eq!(report.lag(by_id(3)), None);
    let text = render::status_text(&report);
    assert!(text.contains("leader: node 1"), "{text}");
    assert!(text.contains("voters:   1 2"), "{text}");
    assert!(text.contains("learners: 3"), "{text}");
    assert!(text.contains("unreachable"), "{text}");
    let json = render::status_json(&report);
    assert_eq!(json["leader"], 1);
    assert_eq!(json["nodes"][1]["lag"], 10);
    assert_eq!(json["nodes"][2]["reachable"], false);
    assert_eq!(json["membership"]["learners"][0], 3);
}

/// A request to a follower is redirected; the leader's own membership (not
/// the follower's, possibly stale) is the one the change is based on; the
/// change is `Started` and the tool waits through the joint step.
#[tokio::test]
async fn change_follows_the_leader_and_waits_for_the_final_membership() {
    let polls = Arc::new(Mutex::new(0u32));
    let polls2 = polls.clone();
    let leader = Stub::start(1, move |req| match req {
        AdminRequest::Membership => {
            let mut n = polls2.lock().unwrap();
            *n += 1;
            // Poll 1: before; 2: still before; 3: a joint step with the
            // learner (not final); 4+: final.
            let (index, joint) = match *n {
                1 | 2 => (10, false),
                3 => (11, true),
                _ => (12, false),
            };
            let mut ns = nodes(&[(1, "127.0.0.1:1"), (2, "127.0.0.1:2"), (3, "127.0.0.1:3")]);
            if index > 10 {
                ns.push((4, "127.0.0.1:4".into()));
            }
            V {
                leader: Some(1),
                index,
                committed: true,
                configs: if joint {
                    vec![vec![1, 2, 3], vec![1, 2, 3]]
                } else {
                    vec![vec![1, 2, 3]]
                },
                nodes: ns,
                applied: 50,
            }
            .answer()
        }
        _ => AdminResponse::Started {
            note: Some("mind the even count".into()),
        },
    })
    .await;
    let leader_addr = leader.addr.clone();
    let follower = Stub::start(2, move |req| match req {
        // The follower's view is behind (index 9).
        AdminRequest::Membership => V {
            leader: Some(1),
            index: 9,
            committed: true,
            configs: vec![vec![1, 2, 3]],
            nodes: nodes(&[(1, "127.0.0.1:1"), (2, "127.0.0.1:2"), (3, "127.0.0.1:3")]),
            applied: 40,
        }
        .answer(),
        _ => AdminResponse::NotLeader {
            leader: Some(1),
            addr: Some(leader_addr.clone()),
        },
    })
    .await;
    let st = settings(vec![follower.addr.clone()], 10 * SECS);
    let done = Operator::new(&st)
        .change(&add(4, "127.0.0.1:4"))
        .await
        .unwrap();
    assert_eq!(done.command, "add");
    assert_eq!(done.note.as_deref(), Some("mind the even count"));
    let view = done.view.as_ref().unwrap();
    assert_eq!(view.membership.log_id, lid(12));
    assert_eq!(view.membership.learners(), [4].into());
    let sent = leader.changes();
    assert_eq!(sent.len(), 1, "{sent:?}");
    let AdminRequest::AddLearner { id: 4, expect, .. } = &sent[0] else {
        panic!("{sent:?}");
    };
    assert_eq!(
        *expect,
        lid(10),
        "based on the leader's view, not the follower's"
    );
    let text = render::changed_text(&done);
    assert!(text.contains("learners: 4"), "{text}");
    assert!(text.contains("NOTE: mind the even count"), "{text}");
}

#[tokio::test]
async fn not_leader_is_followed_to_the_leader() {
    let leader = Stub::start(1, |req| match req {
        AdminRequest::Membership => V {
            leader: Some(1),
            index: 5,
            committed: true,
            configs: vec![vec![1, 2, 3]],
            nodes: nodes(&[(1, "127.0.0.1:1"), (2, "127.0.0.1:2"), (3, "127.0.0.1:3")]),
            applied: 5,
        }
        .answer(),
        _ => AdminResponse::Done {
            log_id: lid(5),
            note: None,
        },
    })
    .await;
    let leader_addr = leader.addr.clone();
    // A follower that cannot name the leader in its view (it has none yet)
    // but does in the answer to the change.
    let follower = Stub::start(2, move |req| match req {
        AdminRequest::Membership => V {
            leader: None,
            index: 4,
            committed: true,
            configs: vec![vec![1, 2, 3]],
            nodes: nodes(&[(1, "127.0.0.1:1"), (2, "127.0.0.1:2"), (3, "127.0.0.1:3")]),
            applied: 4,
        }
        .answer(),
        _ => AdminResponse::NotLeader {
            leader: Some(1),
            addr: Some(leader_addr.clone()),
        },
    })
    .await;
    let st = settings(vec![follower.addr.clone()], 5 * SECS);
    let cmd = ClusterCmd::Promote { id: 3, force: true };
    let done = Operator::new(&st).change(&cmd).await.unwrap();
    assert_eq!(done.log_id, lid(5));
    assert_eq!(follower.changes().len(), 1);
    let sent = leader.changes();
    assert_eq!(sent.len(), 1, "{sent:?}");
    assert_eq!(
        sent[0],
        AdminRequest::Promote {
            ids: [3].into(),
            expect: lid(5),
            force: true
        }
    );
}

#[tokio::test]
async fn redirect_loops_are_bounded() {
    let b_addr: Arc<Mutex<String>> = Arc::default();
    let a_addr: Arc<Mutex<String>> = Arc::default();
    let view = |leader| V {
        leader,
        index: 5,
        committed: true,
        configs: vec![vec![1, 2, 3]],
        nodes: nodes(&[(1, "127.0.0.1:1"), (2, "127.0.0.1:2"), (3, "127.0.0.1:3")]),
        applied: 5,
    };
    let a = Stub::start(1, {
        let b = b_addr.clone();
        move |req| match req {
            AdminRequest::Membership => view(None).answer(),
            _ => AdminResponse::NotLeader {
                leader: Some(2),
                addr: Some(b.lock().unwrap().clone()),
            },
        }
    })
    .await;
    let b = Stub::start(2, {
        let a = a_addr.clone();
        move |req| match req {
            AdminRequest::Membership => view(None).answer(),
            _ => AdminResponse::NotLeader {
                leader: Some(1),
                addr: Some(a.lock().unwrap().clone()),
            },
        }
    })
    .await;
    *a_addr.lock().unwrap() = a.addr.clone();
    *b_addr.lock().unwrap() = b.addr.clone();
    let st = settings(vec![a.addr.clone()], 20 * SECS);
    let started = Instant::now();
    let e = Operator::new(&st)
        .change(&add(4, "127.0.0.1:4"))
        .await
        .err()
        .unwrap();
    assert!(
        matches!(&e, Fail::Unreachable(m) if m.contains("redirects")),
        "{e:?}"
    );
    assert_eq!(e.exit_code(), 3);
    assert!(started.elapsed() < 10 * SECS);
    assert_eq!(a.changes().len() + b.changes().len(), 9);
}

#[tokio::test]
async fn a_cluster_without_a_leader_is_retried_until_the_timeout() {
    let a = Stub::start(1, |req| match req {
        AdminRequest::Membership => V {
            leader: None,
            index: 5,
            committed: true,
            configs: vec![vec![1, 2, 3]],
            nodes: nodes(&[(1, "127.0.0.1:1"), (2, "127.0.0.1:2"), (3, "127.0.0.1:3")]),
            applied: 5,
        }
        .answer(),
        _ => AdminResponse::NotLeader {
            leader: None,
            addr: None,
        },
    })
    .await;
    let st = settings(vec![a.addr.clone()], 2 * SECS);
    let started = Instant::now();
    let e = Operator::new(&st)
        .change(&add(4, "127.0.0.1:4"))
        .await
        .err()
        .unwrap();
    assert_eq!(e.exit_code(), 3, "{e:?}");
    assert!(started.elapsed() < 6 * SECS);
    assert!(a.changes().len() >= 2, "it must ask again");
}

#[tokio::test]
async fn a_conflict_is_reported_with_the_current_membership_and_not_retried() {
    let a = Stub::start(1, |req| match req {
        AdminRequest::Membership => V {
            leader: Some(1),
            index: 8,
            committed: true,
            configs: vec![vec![1, 2, 3]],
            nodes: nodes(&[(1, "127.0.0.1:1"), (2, "127.0.0.1:2"), (3, "127.0.0.1:3")]),
            applied: 8,
        }
        .answer(),
        _ => AdminResponse::Conflict { current: lid(8) },
    })
    .await;
    let st = settings(vec![a.addr.clone()], 5 * SECS);
    let e = Operator::new(&st)
        .change(&ClusterCmd::Remove {
            id: 3,
            force: false,
        })
        .await
        .err()
        .unwrap();
    assert_eq!(e.exit_code(), 1);
    assert!(matches!(&e, Fail::Conflict { current, view: Some(_), .. } if *current == lid(8)));
    assert_eq!(a.changes().len(), 1, "no automatic retry");
    let text = render::fail_text(&e);
    assert!(text.contains("conflict"), "{text}");
    assert!(text.contains("nothing was changed"), "{text}");
    assert!(text.contains("voters:   1 2 3"), "{text}");
    let json = render::fail_json(&e);
    assert_eq!(
        (
            json["ok"].clone(),
            json["kind"].clone(),
            json["exit"].clone()
        ),
        (false.into(), "conflict".into(), 1.into())
    );
}

#[tokio::test]
async fn refusals_and_unsupported_exit_one() {
    for (answer, text) in [
        (
            AdminResponse::Refused {
                reason: "fewer than 3 voters".into(),
            },
            "fewer than 3 voters",
        ),
        (AdminResponse::Unsupported, "does not support"),
    ] {
        let a = Stub::start(1, move |req| match req {
            AdminRequest::Membership => V {
                leader: Some(1),
                index: 8,
                committed: true,
                configs: vec![vec![1, 2, 3]],
                nodes: nodes(&[(1, "127.0.0.1:1"), (2, "127.0.0.1:2"), (3, "127.0.0.1:3")]),
                applied: 8,
            }
            .answer(),
            _ => answer.clone(),
        })
        .await;
        let st = settings(vec![a.addr.clone()], 5 * SECS);
        let e = Operator::new(&st)
            .change(&ClusterCmd::Remove {
                id: 3,
                force: false,
            })
            .await
            .err()
            .unwrap();
        assert_eq!(e.exit_code(), 1);
        assert!(render::fail_text(&e).contains(text), "{e:?}");
    }
}

/// `Started` that never shows a new membership: the tool gives up at the
/// timeout with its own exit status and says the change may still complete.
#[tokio::test]
async fn a_started_change_that_is_not_seen_to_complete_times_out() {
    let a = Stub::start(1, |req| match req {
        AdminRequest::Membership => V {
            leader: Some(1),
            index: 8,
            committed: true,
            configs: vec![vec![1, 2, 3]],
            nodes: nodes(&[(1, "127.0.0.1:1"), (2, "127.0.0.1:2"), (3, "127.0.0.1:3")]),
            applied: 8,
        }
        .answer(),
        _ => AdminResponse::Started {
            note: Some("stop the removed node".into()),
        },
    })
    .await;
    let st = settings(vec![a.addr.clone()], Duration::from_millis(1200));
    let started = Instant::now();
    let e = Operator::new(&st)
        .change(&ClusterCmd::Remove {
            id: 3,
            force: false,
        })
        .await
        .err()
        .unwrap();
    assert_eq!(e.exit_code(), 4, "{e:?}");
    assert!(started.elapsed() < 5 * SECS);
    let text = render::fail_text(&e);
    assert!(text.contains("may still complete"), "{text}");
    assert!(text.contains("cluster status"), "{text}");
    assert!(text.contains("NOTE: stop the removed node"), "{text}");
}

/// A final membership that is not what was asked for (another operator's
/// change got there first) is not reported as success.
#[tokio::test]
async fn a_changed_membership_without_the_requested_effect_is_refused() {
    let polls = Arc::new(Mutex::new(0));
    let a = Stub::start(1, move |req| match req {
        AdminRequest::Membership => {
            let mut n = polls.lock().unwrap();
            *n += 1;
            V {
                leader: Some(1),
                index: if *n == 1 { 8 } else { 9 },
                committed: true,
                configs: vec![vec![1, 2, 3]],
                nodes: nodes(&[(1, "127.0.0.1:1"), (2, "127.0.0.1:2"), (3, "127.0.0.1:3")]),
                applied: 8,
            }
            .answer()
        }
        _ => AdminResponse::Started { note: None },
    })
    .await;
    let st = settings(vec![a.addr.clone()], 5 * SECS);
    let e = Operator::new(&st)
        .change(&add(4, "127.0.0.1:4"))
        .await
        .err()
        .unwrap();
    assert_eq!(e.exit_code(), 1, "{e:?}");
    assert!(render::fail_text(&e).contains("not as requested"));
}

#[tokio::test]
async fn unreachable_nodes_exit_three_and_a_rejected_hello_exits_one() {
    let closed = {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        l.local_addr().unwrap().to_string()
    };
    let st = settings(vec![closed.clone()], 2 * SECS);
    let e = Operator::new(&st).status().await.err().unwrap();
    assert_eq!(e.exit_code(), 3, "{e:?}");
    let e = Operator::new(&st)
        .change(&add(4, "127.0.0.1:4"))
        .await
        .err()
        .unwrap();
    assert_eq!(e.exit_code(), 3, "{e:?}");

    let rejecting = Stub::start_with(
        1,
        Some("hello rejected"),
        Box::new(|_| AdminResponse::Unsupported),
    )
    .await;
    let st = settings(vec![closed, rejecting.addr.clone()], 2 * SECS);
    let e = Operator::new(&st).status().await.err().unwrap();
    assert_eq!(e.exit_code(), 1, "{e:?}");
    assert!(render::fail_text(&e).contains("hello rejected"), "{e:?}");
}
