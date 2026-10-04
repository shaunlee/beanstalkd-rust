//! Text and JSON output of the commands.

use std::collections::BTreeSet;
use std::fmt::Write as _;

use bstk_raft::NodeId;
use bstk_raft::status::{MembershipView, NodeStatusEx};
use openraft::LogId;
use serde_json::{Value, json};

use super::drive::{Changed, Fail, NodeReport, Role, StatusReport};

pub fn log_id_text(id: Option<LogId<NodeId>>) -> String {
    id.map_or_else(
        || "none".to_owned(),
        |l| format!("term {} index {}", l.leader_id.term, l.index),
    )
}

fn log_id_json(id: Option<LogId<NodeId>>) -> Value {
    id.map_or(
        Value::Null,
        |l| json!({"term": l.leader_id.term, "index": l.index}),
    )
}

fn ids(set: &BTreeSet<NodeId>) -> String {
    if set.is_empty() {
        return "-".to_owned();
    }
    set.iter().map(u64::to_string).collect::<Vec<_>>().join(" ")
}

/// The membership in one or two lines.
fn membership_text(m: &MembershipView) -> String {
    let mut s = String::new();
    let state = if m.committed {
        "committed"
    } else {
        "not committed yet"
    };
    let _ = writeln!(s, "membership ({}, {state})", log_id_text(m.log_id));
    if m.is_joint() {
        let sets: Vec<String> = m.configs.iter().map(ids).collect();
        let _ = writeln!(s, "  voters:   joint ({})", sets.join(" -> "));
    } else {
        let _ = writeln!(s, "  voters:   {}", ids(&m.voters()));
    }
    let _ = writeln!(s, "  learners: {}", ids(&m.learners()));
    s
}

fn membership_json(m: &MembershipView) -> Value {
    json!({
        "log_id": log_id_json(m.log_id),
        "committed": m.committed,
        "joint": m.is_joint(),
        "voter_sets": m.configs.iter().map(|c| c.iter().collect::<Vec<_>>()).collect::<Vec<_>>(),
        "voters": m.voters(),
        "learners": m.learners(),
        "nodes": m.nodes.iter().map(|(id, a)| (id.to_string(), json!(a))).collect::<serde_json::Map<_, _>>(),
    })
}

fn role_text(r: Role) -> &'static str {
    match r {
        Role::Leader => "leader",
        Role::Voter => "voter",
        Role::Learner => "learner",
    }
}

fn node_state(report: &StatusReport, n: &NodeReport) -> (String, String, String) {
    match &n.state {
        Err(e) => (
            "-".into(),
            "-".into(),
            format!("unreachable: {}", e.lines().next().unwrap_or("")),
        ),
        Ok(s) => {
            let applied = s.last_applied.map_or("-".into(), |l| l.index.to_string());
            let lag = report.lag(n).map_or("-".into(), |l| l.to_string());
            let state = if !s.raft_running {
                "starting (Raft not running)"
            } else if s.rejoining {
                "rejoining"
            } else {
                "ok"
            };
            (applied, lag, state.into())
        }
    }
}

pub fn status_text(r: &StatusReport) -> String {
    let v: &NodeStatusEx = &r.view;
    let mut s = String::new();
    let _ = write!(s, "{}", membership_text(&v.membership));
    match v.leader {
        Some(l) => {
            let addr = v.membership.nodes.get(&l).map_or("?", String::as_str);
            let _ = writeln!(s, "leader: node {l} ({addr}), term {}", v.term);
        }
        None => {
            let _ = writeln!(s, "leader: none known, term {}", v.term);
        }
    }
    let _ = writeln!(s, "highest member id ever: {}", v.highest_member);
    if v.membership.is_joint() {
        let _ = writeln!(
            s,
            "a joint configuration is in effect: a change is in flight or was interrupted"
        );
    }
    let rows: Vec<[String; 6]> = r
        .nodes
        .iter()
        .map(|n| {
            let (applied, lag, state) = node_state(r, n);
            [
                n.id.to_string(),
                role_text(n.role).into(),
                n.addr.clone(),
                applied,
                lag,
                state,
            ]
        })
        .collect();
    let head = ["ID", "ROLE", "ADDRESS", "APPLIED", "LAG", "STATE"].map(String::from);
    let mut width = [0usize; 6];
    for row in std::iter::once(&head).chain(&rows) {
        for (w, cell) in width.iter_mut().zip(row) {
            *w = (*w).max(cell.len());
        }
    }
    s.push('\n');
    for row in std::iter::once(&head).chain(&rows) {
        let line: Vec<String> = row
            .iter()
            .zip(width)
            .map(|(c, w)| format!("{c:<w$}"))
            .collect();
        let _ = writeln!(s, "{}", line.join("  ").trim_end());
    }
    s
}

pub fn status_json(r: &StatusReport) -> Value {
    let v = &r.view;
    json!({
        "ok": true,
        "command": "status",
        "leader": v.leader,
        "term": v.term,
        "highest_member": v.highest_member,
        "membership": membership_json(&v.membership),
        "nodes": r.nodes.iter().map(|n| {
            let mut o = json!({
                "id": n.id,
                "addr": n.addr,
                "role": role_text(n.role),
            });
            match &n.state {
                Ok(s) => {
                    o["reachable"] = json!(true);
                    o["raft_running"] = json!(s.raft_running);
                    o["rejoining"] = json!(s.rejoining);
                    o["last_applied"] = s.last_applied.map_or(Value::Null, |l| json!(l.index));
                    o["lag"] = r.lag(n).map_or(Value::Null, |l| json!(l));
                }
                Err(e) => {
                    o["reachable"] = json!(false);
                    o["error"] = json!(e);
                }
            }
            o
        }).collect::<Vec<_>>(),
    })
}

fn note_text(note: &str) -> String {
    format!("\nNOTE: {note}\n")
}

pub fn changed_text(c: &Changed) -> String {
    let mut s = format!("{}: done\n", c.command);
    match &c.view {
        Some(v) => s.push_str(&membership_text(&v.membership)),
        None => {
            let _ = writeln!(
                s,
                "membership log id: {} (its members could not be read back; run `status`)",
                log_id_text(c.log_id)
            );
        }
    }
    if let Some(n) = &c.note {
        s.push_str(&note_text(n));
    }
    s
}

pub fn changed_json(c: &Changed) -> Value {
    json!({
        "ok": true,
        "command": c.command,
        "log_id": log_id_json(c.log_id),
        "membership": c.view.as_ref().map(|v| membership_json(&v.membership)),
        "note": c.note,
    })
}

pub fn fail_text(f: &Fail) -> String {
    match f {
        Fail::Refused(m) => format!("refused: {m}\n"),
        Fail::Unreachable(m) => format!("cannot reach the cluster: {m}\n"),
        Fail::Conflict {
            expected,
            current,
            view,
        } => {
            let mut s = format!(
                "conflict: the membership changed while this request was being made (it was based \
                 on {}, it is now {}); nothing was changed. Check `beanstalkd-rs cluster status` \
                 and run the command again if it is still needed.\n",
                log_id_text(*expected),
                log_id_text(*current)
            );
            if let Some(v) = view {
                s.push('\n');
                s.push_str(&membership_text(&v.membership));
            }
            s
        }
        Fail::Timeout { msg, note } => {
            let mut s = format!("timed out: {msg}\n");
            if let Some(n) = note {
                s.push_str(&note_text(n));
            }
            s
        }
    }
}

pub fn fail_json(f: &Fail) -> Value {
    let (kind, error) = match f {
        Fail::Refused(m) => ("refused", m.clone()),
        Fail::Unreachable(m) => ("unreachable", m.clone()),
        Fail::Conflict { .. } => (
            "conflict",
            "the membership changed while the request was being made; nothing was changed".into(),
        ),
        Fail::Timeout { msg, .. } => ("timeout", msg.clone()),
    };
    let mut o = json!({"ok": false, "exit": f.exit_code(), "kind": kind, "error": error});
    match f {
        Fail::Conflict {
            expected,
            current,
            view,
        } => {
            o["expected"] = log_id_json(*expected);
            o["current"] = log_id_json(*current);
            o["membership"] = view
                .as_ref()
                .map_or(Value::Null, |v| membership_json(&v.membership));
        }
        Fail::Timeout { note, .. } => o["note"] = json!(note),
        _ => {}
    }
    o
}
