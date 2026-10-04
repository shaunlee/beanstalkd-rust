//! The commands: find a node, follow the leader, send a change, wait for it.

use std::time::{Duration, Instant};

use bstk_raft::NodeId;
use bstk_raft::status::{MembershipView, NodeStatusEx};
use bstk_raft::wire::{AdminRequest, AdminResponse};
use futures::future::join_all;
use openraft::LogId;

use super::args::{ClusterCmd, Settings};
use super::link::{Auth, Link, LinkError};

/// Redirects (`NotLeader`) followed before giving up.
const MAX_HOPS: u32 = 8;
/// Longest wait for one connect, hello or answer; the command's timeout
/// bounds the sum.
const CONNECT_CAP: Duration = Duration::from_secs(5);
const CALL_CAP: Duration = Duration::from_secs(10);
/// Per-node bound of the probes `status` makes.
const PROBE_CAP: Duration = Duration::from_secs(3);
const POLL_EVERY: Duration = Duration::from_millis(300);
/// Pause before asking again while an election is in flight or the leader
/// named is unreachable.
const RETRY_EVERY: Duration = Duration::from_millis(500);
/// How long to wait for the leader's membership to show a change it
/// reported `Done` (it is the leader's own view, so this is short).
const SHOW_CAP: Duration = Duration::from_secs(5);

/// Why a command did not succeed; `exit_code` maps it.
#[derive(Debug)]
pub enum Fail {
    /// The cluster said no (a guardrail, a rejected identity, an answer the
    /// tool does not understand). Nothing was changed.
    Refused(String),
    /// The membership is not the one the request was based on; nothing was
    /// changed. `view`: what the node reports now.
    Conflict {
        expected: Option<LogId<NodeId>>,
        current: Option<LogId<NodeId>>,
        view: Option<Box<NodeStatusEx>>,
    },
    /// No node (or no leader) could be reached; nothing was changed.
    Unreachable(String),
    /// The change was accepted or sent, but not seen to complete in time.
    Timeout { msg: String, note: Option<String> },
}

impl Fail {
    pub fn exit_code(&self) -> u8 {
        match self {
            Fail::Refused(_) | Fail::Conflict { .. } => 1,
            Fail::Unreachable(_) => 3,
            Fail::Timeout { .. } => 4,
        }
    }
}

pub struct Changed {
    pub command: &'static str,
    /// The membership after the change, if it could be read.
    pub view: Option<NodeStatusEx>,
    pub log_id: Option<LogId<NodeId>>,
    pub note: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Leader,
    Voter,
    Learner,
}

pub struct NodeReport {
    pub id: NodeId,
    pub addr: String,
    pub role: Role,
    /// The node's own view, or why it could not be asked.
    pub state: Result<NodeStatusEx, String>,
}

pub struct StatusReport {
    pub view: NodeStatusEx,
    pub nodes: Vec<NodeReport>,
}

impl StatusReport {
    /// Entries `node` is behind the leader's last applied entry; `None`
    /// when either is unknown.
    pub fn lag(&self, node: &NodeReport) -> Option<u64> {
        let leader = self.view.leader?;
        let applied = |id| {
            self.nodes
                .iter()
                .find(|n| n.id == id)
                .and_then(|n| n.state.as_ref().ok())
                .and_then(|s| s.last_applied)
                .map(|l| l.index)
        };
        let top = applied(leader)?;
        Some(top.saturating_sub(node.state.as_ref().ok()?.last_applied?.index))
    }
}

/// A requested change and the membership it must produce.
enum Effect {
    Add(NodeId, String),
    Promote(NodeId, bool),
    Remove(NodeId, bool),
    SetAddr(NodeId, String, bool),
}

impl Effect {
    fn of(cmd: &ClusterCmd) -> Option<Effect> {
        Some(match cmd {
            ClusterCmd::Status => return None,
            ClusterCmd::Add { id, addr } => Effect::Add(*id, addr.clone()),
            ClusterCmd::Promote { id, force } => Effect::Promote(*id, *force),
            ClusterCmd::Remove { id, force } => Effect::Remove(*id, *force),
            ClusterCmd::SetAddr { id, addr, force } => Effect::SetAddr(*id, addr.clone(), *force),
        })
    }

    fn name(&self) -> &'static str {
        match self {
            Effect::Add(..) => "add",
            Effect::Promote(..) => "promote",
            Effect::Remove(..) => "remove",
            Effect::SetAddr(..) => "set-addr",
        }
    }

    fn request(&self, expect: Option<LogId<NodeId>>) -> AdminRequest {
        match self {
            Effect::Add(id, addr) => AdminRequest::AddLearner {
                id: *id,
                addr: addr.clone(),
                expect,
            },
            Effect::Promote(id, force) => AdminRequest::Promote {
                ids: [*id].into(),
                expect,
                force: *force,
            },
            Effect::Remove(id, force) => AdminRequest::Remove {
                id: *id,
                expect,
                force: *force,
            },
            Effect::SetAddr(id, addr, force) => AdminRequest::SetAddr {
                id: *id,
                addr: addr.clone(),
                expect,
                force: *force,
            },
        }
    }

    /// Whether `m` is the membership the request asks for.
    fn holds(&self, m: &MembershipView) -> bool {
        match self {
            Effect::Add(id, addr) | Effect::SetAddr(id, addr, _) => {
                m.nodes.get(id).is_some_and(|a| a == addr)
            }
            Effect::Promote(id, _) => m.voters().contains(id),
            Effect::Remove(id, _) => !m.is_member(*id),
        }
    }
}

/// A membership that differs from the one a change was based on and is
/// final: committed, and not in the middle of a joint-consensus step.
fn settled(m: &MembershipView, expect: Option<LogId<NodeId>>) -> bool {
    m.log_id != expect && m.committed && !m.is_joint()
}

pub struct Operator<'a> {
    st: &'a Settings,
    deadline: Instant,
    /// Addresses to ask: the seeds, then the members' addresses learned.
    candidates: Vec<String>,
    /// Nodes that said they know no leader; asked last.
    cold: Vec<String>,
    link: Option<Link>,
}

impl<'a> Operator<'a> {
    pub fn new(st: &'a Settings) -> Self {
        Operator {
            st,
            deadline: Instant::now() + st.timeout,
            candidates: st.seeds.clone(),
            cold: Vec::new(),
            link: None,
        }
    }

    fn progress(&self, msg: &str) {
        if !self.st.json {
            eprintln!("{msg}");
        }
    }

    fn remaining(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }

    fn expired(&self) -> bool {
        self.remaining().is_zero()
    }

    /// The time one step may take: at most `cap`, never past the deadline,
    /// never zero (a zero timeout fails at once, which reads as a hang).
    fn budget(&self, cap: Duration) -> Duration {
        self.remaining().min(cap).max(Duration::from_millis(1))
    }

    fn learn(&mut self, v: &NodeStatusEx) {
        for addr in v.membership.nodes.values() {
            if !self.candidates.contains(addr) {
                self.candidates.push(addr.clone());
            }
        }
    }

    /// Connects to the first candidate that answers: `prefer` first, nodes
    /// in `avoid` and those that know no leader last.
    async fn connect_any(&mut self, prefer: Option<&str>, avoid: &[String]) -> Result<(), Fail> {
        let mut order: Vec<String> = prefer.map(str::to_owned).into_iter().collect();
        let is_last = |a: &String| avoid.contains(a) || self.cold.contains(a);
        order.extend(
            self.candidates
                .iter()
                .filter(|a| !is_last(a) && Some(a.as_str()) != prefer)
                .cloned(),
        );
        order.extend(
            self.candidates
                .iter()
                .filter(|a| is_last(a) && Some(a.as_str()) != prefer)
                .cloned(),
        );
        let mut errors = Vec::new();
        let mut rejected = false;
        for addr in order {
            if self.expired() {
                break;
            }
            match Link::open(&addr, &self.st.auth, None, self.budget(CONNECT_CAP)).await {
                Ok(l) => {
                    self.link = Some(l);
                    return Ok(());
                }
                Err(LinkError::Rejected(m)) => {
                    rejected = true;
                    errors.push(m);
                }
                Err(e) => errors.push(e.to_string()),
            }
        }
        if errors.is_empty() {
            return Err(Fail::Unreachable(format!(
                "timed out after {:?} before a node answered",
                self.st.timeout
            )));
        }
        let msg = errors.join("\n");
        Err(if rejected {
            Fail::Refused(msg)
        } else {
            Fail::Unreachable(msg)
        })
    }

    /// The membership view of the node connected, or of the first one that
    /// answers.
    async fn fetch(&mut self) -> Result<NodeStatusEx, Fail> {
        let mut bad: Vec<String> = Vec::new();
        let mut last = String::from("no node answered");
        for _ in 0..=self.candidates.len() {
            if self.link.is_none() {
                self.connect_any(None, &bad).await?;
            }
            let budget = self.budget(CALL_CAP);
            let Some(link) = self.link.as_mut() else {
                continue;
            };
            match link.call(AdminRequest::Membership, budget).await {
                Ok(AdminResponse::Membership(v)) => {
                    self.learn(&v);
                    return Ok(*v);
                }
                Ok(other) => {
                    return Err(Fail::Refused(format!(
                        "{}: unexpected answer to a membership request: {other:?}",
                        link.addr
                    )));
                }
                Err(e) => {
                    bad.push(link.addr.clone());
                    last = e.to_string();
                    self.link = None;
                }
            }
        }
        Err(Fail::Unreachable(last))
    }

    /// Like [`Self::fetch`], from the leader when the node asked names one
    /// whose address is known (a follower may be behind).
    async fn fetch_from_leader(&mut self) -> Result<NodeStatusEx, Fail> {
        let view = self.fetch().await?;
        let current = self.link.as_ref().map(|l| (l.addr.clone(), l.node));
        let leader = view
            .leader
            .filter(|l| current.as_ref().is_none_or(|(_, node)| node != l))
            .and_then(|l| view.membership.nodes.get(&l).map(|a| (l, a.clone())));
        if let Some((id, addr)) = leader
            && !self.expired()
            && let Ok(l) =
                Link::open(&addr, &self.st.auth, Some(id), self.budget(CONNECT_CAP)).await
        {
            self.link = Some(l);
            if let Ok(v) = self.fetch().await {
                return Ok(v);
            }
        }
        Ok(view)
    }

    /// Polls until `done` accepts the membership shown, following the
    /// leader, or the deadline passes (`None`).
    async fn wait_view(
        &mut self,
        until: Instant,
        done: impl Fn(&MembershipView) -> bool,
    ) -> Option<NodeStatusEx> {
        let saved = self.deadline;
        self.deadline = until.min(saved);
        let found = loop {
            if let Ok(v) = self.fetch_from_leader().await
                && done(&v.membership)
            {
                break Some(v);
            }
            if self.expired() {
                break None;
            }
            tokio::time::sleep(POLL_EVERY.min(self.remaining())).await;
        };
        self.deadline = saved;
        found
    }

    pub async fn status(&mut self) -> Result<StatusReport, Fail> {
        let view = self.fetch_from_leader().await?;
        // Free the admin slot before asking every node.
        self.link = None;
        let auth = &self.st.auth;
        let budget = self.budget(PROBE_CAP);
        let shown = &view;
        let voters = view.membership.voters();
        let voters = &voters;
        let asks = view.membership.nodes.iter().map(|(&id, addr)| async move {
            let state = probe(auth, id, addr, budget).await;
            let role = if shown.leader == Some(id) {
                Role::Leader
            } else if voters.contains(&id) {
                Role::Voter
            } else {
                Role::Learner
            };
            NodeReport {
                id,
                addr: addr.clone(),
                role,
                state,
            }
        });
        let nodes = join_all(asks).await;
        Ok(StatusReport { view, nodes })
    }

    pub async fn change(&mut self, cmd: &ClusterCmd) -> Result<Changed, Fail> {
        let Some(effect) = Effect::of(cmd) else {
            return Err(Fail::Refused("status is not a change".into()));
        };
        let mut view = self.fetch().await?;
        let mut hops = 0;
        loop {
            let expect = view.membership.log_id;
            let req = effect.request(expect);
            let budget = self.budget(CALL_CAP);
            let Some(link) = self.link.as_mut() else {
                view = self.fetch().await?;
                continue;
            };
            let from = link.addr.clone();
            let answer = link.call(req, budget).await.map_err(|e| Fail::Timeout {
                msg: format!(
                    "{e}; the request was sent, so the change may have been applied: check with \
                     `beanstalkd-rs cluster status`"
                ),
                note: None,
            })?;
            match answer {
                AdminResponse::Done { log_id, note } => {
                    let shown = self
                        .wait_view(Instant::now() + SHOW_CAP, |m| {
                            log_id.is_none() || m.log_id == log_id
                        })
                        .await;
                    return Ok(Changed {
                        command: effect.name(),
                        view: shown,
                        log_id,
                        note,
                    });
                }
                AdminResponse::Started { note } => {
                    self.progress(&format!(
                        "change accepted by {from}; waiting for it to complete (up to {:?})",
                        self.remaining()
                    ));
                    return match self.wait_view(self.deadline, |m| settled(m, expect)).await {
                        Some(v) if effect.holds(&v.membership) => Ok(Changed {
                            command: effect.name(),
                            log_id: v.membership.log_id,
                            view: Some(v),
                            note,
                        }),
                        Some(v) => Err(Fail::Refused(format!(
                            "the membership changed (log id {:?}), but not as requested; another \
                             change may have run, or this one did not take effect: check with \
                             `beanstalkd-rs cluster status`",
                            v.membership.log_id
                        ))),
                        None => Err(Fail::Timeout {
                            msg: format!(
                                "the change was accepted by {from} but not seen to complete within \
                                 {:?}; it may still complete (check with \
                                 `beanstalkd-rs cluster status`)",
                                self.st.timeout
                            ),
                            note,
                        }),
                    };
                }
                AdminResponse::NotLeader { leader, addr } => {
                    hops += 1;
                    if hops > MAX_HOPS {
                        return Err(Fail::Unreachable(format!(
                            "no leader found after {MAX_HOPS} redirects (last asked: {from})"
                        )));
                    }
                    match (leader, addr) {
                        (Some(id), Some(addr)) => {
                            self.progress(&format!(
                                "{from} is not the leader; following to node {id} at {addr}"
                            ));
                            self.cold.clear();
                            match Link::open(
                                &addr,
                                &self.st.auth,
                                Some(id),
                                self.budget(CONNECT_CAP),
                            )
                            .await
                            {
                                Ok(l) => self.link = Some(l),
                                Err(LinkError::Rejected(m)) => return Err(Fail::Refused(m)),
                                Err(e) => {
                                    self.progress(&format!("cannot reach the leader: {e}"));
                                    self.link = None;
                                    self.pause().await?;
                                }
                            }
                        }
                        _ => {
                            self.progress(&format!("{from} knows no leader yet; trying again"));
                            self.cold.push(from);
                            self.link = None;
                            self.pause().await?;
                        }
                    }
                    view = self.fetch().await?;
                }
                AdminResponse::Conflict { current } => {
                    let shown = self.fetch().await.ok().map(Box::new);
                    return Err(Fail::Conflict {
                        expected: expect,
                        current,
                        view: shown,
                    });
                }
                AdminResponse::Refused { reason } => return Err(Fail::Refused(reason)),
                AdminResponse::Unsupported => {
                    return Err(Fail::Refused(format!(
                        "{from} does not support membership changes"
                    )));
                }
                AdminResponse::Membership(_) => {
                    return Err(Fail::Refused(format!(
                        "{from}: unexpected answer to a change request"
                    )));
                }
            }
        }
    }

    async fn pause(&self) -> Result<(), Fail> {
        if self.remaining() <= RETRY_EVERY {
            return Err(Fail::Unreachable(format!(
                "no leader reachable within {:?}",
                self.st.timeout
            )));
        }
        tokio::time::sleep(RETRY_EVERY).await;
        Ok(())
    }
}

/// One node's own view, or why it could not be asked.
async fn probe(
    auth: &Auth,
    id: NodeId,
    addr: &str,
    budget: Duration,
) -> Result<NodeStatusEx, String> {
    let mut link = Link::open(addr, auth, Some(id), budget)
        .await
        .map_err(|e| e.to_string())?;
    match link.call(AdminRequest::Membership, budget).await {
        Ok(AdminResponse::Membership(v)) => Ok(*v),
        Ok(other) => Err(format!("unexpected answer: {other:?}")),
        Err(e) => Err(e.to_string()),
    }
}
