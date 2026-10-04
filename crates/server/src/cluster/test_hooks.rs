//! Test-only membership changes (feature `test-hooks`, never enabled in a
//! shipped build; docs/DESIGN.md §8, "Membership-driven networking"). The
//! integration tests that predate the admin executor (P6-T4) drive
//! membership changes through a command file, which only a build with this
//! feature reads; it skips the executor's guardrails (only the id check is
//! kept), so its tests decide which of the rejoin argument's assumptions to
//! keep (one breaks A2 on purpose, to test the rejoin re-check). It holds the
//! executor's lock like a change does.
//!
//! Protocol: the test writes one line to `<data_dir>/test-membership.cmd`
//! (on the leader); [`run`] polls for it, removes it, runs it on this node's
//! Raft and writes `ok <log index>` or `err <message>` to
//! `<data_dir>/test-membership.out` (renamed into place, so a reader never
//! sees a partial answer). Commands:
//!
//! - `add-learner <id> <addr>`: `Raft::add_learner` (not blocking);
//! - `change-membership <id>,<id>,...`: `Raft::change_membership` to these
//!   voters (removed nodes are not kept as learners);
//! - `set-nodes <id>=<addr>,...`: `ChangeMembers::SetNodes` (address change).
//!
//! On a rejoining node, the file `<data_dir>/test-hold-rejoin` (see
//! [`rejoin_held`]) keeps it from asking for its `DropNode`, so a test can
//! change the membership while it is still in rejoin mode.
//!
//! On the leader, the file `<data_dir>/test-hold-change` (see
//! [`hold_change`]) holds an admin membership change after its checks and
//! before openraft runs it, so a test can cut links or kill the leader
//! between the two (P6-T4).

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use bstk_raft::NodeId;
use openraft::{BasicNode, ChangeMembers};

use super::Core;

pub const CMD_FILE: &str = "test-membership.cmd";
pub const OUT_FILE: &str = "test-membership.out";
pub const HOLD_REJOIN_FILE: &str = "test-hold-rejoin";
pub const HOLD_CHANGE_FILE: &str = "test-hold-change";

/// Whether the test holds this node in rejoin mode (see the module docs).
pub fn rejoin_held(data_dir: &Path) -> bool {
    data_dir.join(HOLD_REJOIN_FILE).exists()
}

/// Waits while the test holds admin membership changes (see the module
/// docs).
pub async fn hold_change(data_dir: &Path) {
    let path = data_dir.join(HOLD_CHANGE_FILE);
    if !path.exists() {
        return;
    }
    tracing::warn!("test hook: holding the membership change");
    while path.exists() {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tracing::warn!("test hook: membership change released");
}

fn parse_id(s: &str) -> Result<NodeId, String> {
    s.trim()
        .parse()
        .map_err(|e| format!("bad node id {s:?}: {e}"))
}

/// Runs one command (see the module docs); returns the log index of the
/// (last) membership entry.
async fn execute(core: &Core, line: &str) -> Result<u64, String> {
    // As the admin executor does: `admin::finish_joint` must not take this
    // change's joint step for a leftover.
    let _lock = core.admin_lock.lock().await;
    let mut words = line.split_whitespace();
    let cmd = words.next().unwrap_or_default();
    let resp = match cmd {
        "add-learner" => {
            let id = parse_id(words.next().ok_or("add-learner: missing id")?)?;
            let addr = words.next().ok_or("add-learner: missing address")?;
            // Ids only ever grow (PLAN §9.3); a lower id would make a node's
            // removal check misread it as removed.
            let highest = core.state.highest_member();
            if id <= highest {
                return Err(format!("add-learner: id {id} is not above {highest}"));
            }
            core.raft
                .add_learner(id, BasicNode::new(addr), false)
                .await
                .map_err(|e| e.to_string())?
        }
        "change-membership" => {
            let ids = words.next().ok_or("change-membership: missing ids")?;
            let voters = ids
                .split(',')
                .map(parse_id)
                .collect::<Result<BTreeSet<_>, _>>()?;
            core.raft
                .change_membership(voters, false)
                .await
                .map_err(|e| e.to_string())?
        }
        "set-nodes" => {
            let list = words.next().ok_or("set-nodes: missing nodes")?;
            let mut nodes = BTreeMap::new();
            for item in list.split(',') {
                let (id, addr) = item
                    .split_once('=')
                    .ok_or_else(|| format!("set-nodes: bad item {item:?}"))?;
                nodes.insert(parse_id(id)?, BasicNode::new(addr));
            }
            core.raft
                .change_membership(ChangeMembers::SetNodes(nodes), false)
                .await
                .map_err(|e| e.to_string())?
        }
        other => return Err(format!("unknown command {other:?}")),
    };
    Ok(resp.log_id.index)
}

fn write_answer(dir: &Path, answer: &str) {
    let tmp = dir.join(format!("{OUT_FILE}.tmp"));
    if std::fs::write(&tmp, answer).is_ok() {
        let _ = std::fs::rename(&tmp, dir.join(OUT_FILE));
    }
}

/// Polls `data_dir` for a command file until the task is aborted.
pub async fn run(core: Arc<Core>, data_dir: PathBuf) {
    let cmd_path = data_dir.join(CMD_FILE);
    loop {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let Ok(line) = std::fs::read_to_string(&cmd_path) else {
            continue;
        };
        let _ = std::fs::remove_file(&cmd_path);
        let line = line.trim().to_string();
        tracing::warn!(command = %line, "test hook: membership change");
        let answer = match execute(&core, &line).await {
            Ok(index) => format!("ok {index}"),
            Err(e) => format!("err {e}"),
        };
        tracing::warn!(command = %line, %answer, "test hook: membership change done");
        write_answer(&data_dir, &answer);
    }
}
