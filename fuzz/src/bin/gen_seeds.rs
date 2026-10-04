//! Writes small seed corpora for the fuzz targets into `seeds/<target>/`
//! (run from `fuzz/`: `cargo run --bin gen_seeds`). Seeds are valid inputs
//! from the project's own encoders and test cases, so the fuzzer starts
//! past the outer checks.

use std::path::{Path, PathBuf};

use std::collections::BTreeMap;

use bstk_engine::{EngineInput, JobRecord, JournalEntry, RecordState};
use bstk_proto::{Command, TubeName};
use bstk_raft::forward::ControlRequest;
use bstk_raft::storage::state_machine::fuzzing;
use bstk_raft::status::{MembershipView, NodeStatus, NodeStatusEx};
use bstk_raft::wire::{
    self, AdminHello, AdminRequest, AdminResponse, ClientMsg, Hello, RpcRequest, RpcResponse,
    ServerHello, ServerMsg, WireError,
};
use bstk_raft::{ForwardRequest, ForwardResponse, Op, Request};
use bstk_store::{SyncPolicy, Wal, WalOptions};
use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest, VoteResponse};
use openraft::{
    BasicNode, CommittedLeaderId, Entry, EntryPayload, LogId, Membership, SnapshotMeta,
    StoredMembership, Vote,
};

fn write(target: &str, name: &str, bytes: &[u8]) {
    let dir = PathBuf::from("seeds").join(target);
    std::fs::create_dir_all(&dir).expect("corpus dir");
    std::fs::write(dir.join(name), bytes).expect("write seed");
}

/// The `send` strings of a compat case, concatenated (`\r`, `\n`, `\\`,
/// `\"`, `\t`, `\0` and `\xNN` escapes).
fn sends(case: &str) -> Vec<u8> {
    let mut out = Vec::new();
    for line in case.lines() {
        let Some(pos) = line.find(" send \"") else {
            continue;
        };
        let s = &line[pos + 7..];
        let Some(end) = s.rfind('"') else { continue };
        let b = &s.as_bytes()[..end];
        let mut i = 0;
        while i < b.len() {
            if b[i] == b'\\' && i + 1 < b.len() {
                match b[i + 1] {
                    b'r' => out.push(b'\r'),
                    b'n' => out.push(b'\n'),
                    b't' => out.push(b'\t'),
                    b'0' => out.push(0),
                    b'x' if i + 3 < b.len() => {
                        let h = std::str::from_utf8(&b[i + 2..i + 4]).unwrap_or("00");
                        out.push(u8::from_str_radix(h, 16).unwrap_or(0));
                        i += 2;
                    }
                    c => out.push(c),
                }
                i += 2;
            } else {
                out.push(b[i]);
                i += 1;
            }
        }
    }
    out
}

fn proto(cases: &Path) {
    let mut names: Vec<_> = std::fs::read_dir(cases)
        .expect("compat cases")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "bt"))
        .collect();
    names.sort();
    for (i, p) in names.iter().enumerate() {
        let stream = sends(&std::fs::read_to_string(p).expect("case"));
        if stream.is_empty() || stream.len() > 4096 {
            continue;
        }
        // Default -z with put-started frames, as the server decodes; the
        // fuzzer mutates the flags and chunk sizes itself.
        let mut seed = vec![0b1010, 3, 1, 7, 64];
        seed.extend_from_slice(&stream);
        write("proto_decode", &format!("compat-{i:03}"), &seed);
    }
}

fn record(id: u64, state: RecordState) -> JobRecord {
    JobRecord {
        id,
        pri: 10,
        delay: 0,
        ttr: 60,
        created_at: 1_000,
        deadline_at: 0,
        state,
        reserve_ct: 0,
        timeout_ct: 0,
        release_ct: 0,
        bury_ct: 0,
        kick_ct: 0,
    }
}

fn wal() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let opts = WalOptions {
        dir: tmp.path().to_path_buf(),
        file_size: 4096,
        sync: SyncPolicy::Never,
    };
    let (mut wal, _) = Wal::open(opts.clone()).expect("open");
    let tube = TubeName::new("seed").expect("tube");
    let mut entries = Vec::new();
    for id in 1..=6 {
        entries.push(JournalEntry::Put {
            record: record(id, RecordState::Ready),
            tube: if id % 2 == 0 {
                tube.clone()
            } else {
                TubeName::default_tube()
            },
            body: bytes::Bytes::from(vec![b'a' + id as u8; 40 * id as usize]),
        });
    }
    entries.push(JournalEntry::Update(record(2, RecordState::Buried)));
    entries.push(JournalEntry::Delete(3));
    entries.push(JournalEntry::Update(record(4, RecordState::Delayed)));
    for chunk in entries.chunks(3) {
        assert!(wal.reserve_put(4, 240));
        wal.append(chunk).expect("append");
    }
    drop(wal);

    let mut segs: Vec<(u64, Vec<u8>)> = std::fs::read_dir(tmp.path())
        .expect("dir")
        .filter_map(|e| {
            let e = e.ok()?;
            let n: u64 = e
                .file_name()
                .to_str()?
                .strip_prefix("binlog.")?
                .parse()
                .ok()?;
            Some((n, std::fs::read(e.path()).ok()?))
        })
        .collect();
    segs.sort();
    // Trailing zeros (the preallocated rest) only slow the fuzzer down.
    let trimmed: Vec<(u64, Vec<u8>)> = segs
        .into_iter()
        .map(|(n, mut b)| {
            while b.len() > 16 && b.last() == Some(&0) {
                b.pop();
            }
            (n, b)
        })
        .collect();
    let file = |flags: u8, index: u64, body: &[u8], out: &mut Vec<u8>| {
        out.push(flags);
        out.push((index - 1) as u8);
        out.extend_from_slice(&(body.len() as u16).to_le_bytes());
        out.extend_from_slice(body);
    };
    let mut all = Vec::new();
    for (n, b) in &trimmed {
        file(0, *n, b, &mut all);
    }
    write("wal_read", "segments", &all);
    // The same records with the header and CRCs left to the harness.
    let mut fixed = Vec::new();
    for (n, b) in &trimmed {
        file(3, *n, &b[16..], &mut fixed);
    }
    write("wal_read", "segments-fixup", &fixed);
    if let Some((n, b)) = trimmed.first() {
        let mut torn = Vec::new();
        file(0, *n, &b[..b.len() - 5], &mut torn);
        write("wal_read", "torn-tail", &torn);
    }
}

fn enc<T: serde::Serialize>(m: &T) -> Vec<u8> {
    wire::encode(m, wire::DEFAULT_MAX_FRAME).expect("encode")
}

fn raft() {
    let frame = |sel: u8, bytes: Vec<u8>| {
        let mut s = vec![sel];
        s.extend_from_slice(&bytes);
        s
    };
    let leader = CommittedLeaderId::new(3, 1);
    let input = |seq: u64| EngineInput::Command {
        conn: (1 << 48) | seq,
        cmd: Command::Put {
            pri: 1,
            delay: 0,
            ttr: 5,
            body: bytes::Bytes::from_static(b"body"),
        },
    };
    let mut members = BTreeMap::new();
    members.insert(1, BasicNode::new("127.0.0.1:7001"));
    members.insert(2, BasicNode::new("127.0.0.1:7002"));
    let membership = Membership::new(vec![[1, 2].into()], members);
    let entries = vec![
        Entry {
            log_id: LogId::new(leader, 11),
            payload: EntryPayload::Normal(Request {
                now: 42,
                op: Op::Conn {
                    seq: 1,
                    input: EngineInput::Connect(1 << 48),
                },
            }),
        },
        Entry {
            log_id: LogId::new(leader, 12),
            payload: EntryPayload::Normal(Request {
                now: 43,
                op: Op::Batch(vec![(2, input(2)), (3, EngineInput::Tick)]),
            }),
        },
        Entry {
            log_id: LogId::new(leader, 13),
            payload: EntryPayload::Membership(membership.clone()),
        },
        Entry {
            log_id: LogId::new(leader, 14),
            payload: EntryPayload::Blank,
        },
    ];
    let joint = Membership::new(
        vec![[1, 2].into(), [2, 3].into()],
        (1..=4)
            .map(|i| (i, BasicNode::new(format!("127.0.0.1:700{i}"))))
            .collect::<BTreeMap<_, _>>(),
    );
    let status_ex = NodeStatusEx {
        status: NodeStatus {
            vote: Some(Vote::new_committed(3, 1)),
            last_log_id: Some(LogId::new(leader, 14)),
            committed: Some(LogId::new(leader, 14)),
            has_state: true,
        },
        raft_running: true,
        rejoining: false,
        term: 3,
        leader: Some(1),
        last_applied: Some(LogId::new(leader, 14)),
        highest_member: 4,
        membership: MembershipView::new(Some(LogId::new(leader, 13)), &joint, false),
    };
    let meta = SnapshotMeta {
        last_log_id: Some(LogId::new(leader, 14)),
        last_membership: StoredMembership::new(Some(LogId::new(leader, 13)), membership),
        snapshot_id: "3-1-14-1".into(),
    };
    let requests = [
        ClientMsg::Hello(Hello {
            version: wire::PROTOCOL_VERSION,
            from: 1,
            to: 2,
            max_job_size: 65_535,
        }),
        ClientMsg::Request {
            id: 7,
            body: RpcRequest::AppendEntries(AppendEntriesRequest {
                vote: Vote::new_committed(3, 1),
                prev_log_id: Some(LogId::new(leader, 10)),
                entries,
                leader_commit: Some(LogId::new(leader, 9)),
            }),
        },
        ClientMsg::Request {
            id: 8,
            body: RpcRequest::Vote(VoteRequest::new(
                Vote::new(4, 2),
                Some(LogId::new(leader, 14)),
            )),
        },
        ClientMsg::Request {
            id: 9,
            body: RpcRequest::InstallSnapshot(InstallSnapshotRequest {
                vote: Vote::new_committed(3, 1),
                meta: meta.clone(),
                offset: 0,
                data: b"chunk".to_vec(),
                done: true,
            }),
        },
        ClientMsg::Request {
            id: 10,
            body: RpcRequest::Forward(ForwardRequest {
                from: 2,
                items: vec![
                    (1 << 48, 2, input(2)),
                    (1 << 48, 3, EngineInput::Disconnect(1 << 48)),
                ],
            }),
        },
        ClientMsg::Request {
            id: 11,
            body: RpcRequest::Control(ControlRequest {
                from: 2,
                op: Op::SetDraining(true),
            }),
        },
        ClientMsg::Request {
            id: 12,
            body: RpcRequest::Status,
        },
        // Protocol version 4 (P6-T2).
        ClientMsg::Request {
            id: 13,
            body: RpcRequest::StatusEx,
        },
        ClientMsg::AdminHello(AdminHello {
            version: wire::PROTOCOL_VERSION,
            to: Some(2),
        }),
        ClientMsg::Admin {
            id: 1,
            body: AdminRequest::Membership,
        },
        ClientMsg::Admin {
            id: 2,
            body: AdminRequest::AddLearner {
                id: 4,
                addr: "127.0.0.1:7004".into(),
                expect: Some(LogId::new(leader, 13)),
            },
        },
        ClientMsg::Admin {
            id: 3,
            body: AdminRequest::Promote {
                ids: [3, 4].into(),
                expect: Some(LogId::new(leader, 13)),
            },
        },
        ClientMsg::Admin {
            id: 4,
            body: AdminRequest::Remove {
                id: 1,
                expect: None,
            },
        },
        ClientMsg::Admin {
            id: 5,
            body: AdminRequest::SetAddr {
                id: 2,
                addr: "[::1]:7002".into(),
                expect: Some(LogId::new(leader, 13)),
            },
        },
    ];
    for (i, m) in requests.iter().enumerate() {
        write("raft_wire", &format!("client-{i}"), &frame(0, enc(m)));
    }
    let replies = [
        ServerMsg::Hello(ServerHello::Accepted {
            version: wire::PROTOCOL_VERSION,
            node_id: 2,
            max_job_size: 65_535,
        }),
        ServerMsg::Hello(ServerHello::Rejected {
            reason: "seed".into(),
        }),
        ServerMsg::Response {
            id: 8,
            body: RpcResponse::Vote(Ok(VoteResponse::new(Vote::new(4, 2), None, true))),
        },
        ServerMsg::Response {
            id: 10,
            body: RpcResponse::Forward(Ok(ForwardResponse::Accepted)),
        },
        // Protocol version 4 (P6-T2).
        ServerMsg::Response {
            id: 13,
            body: RpcResponse::StatusEx(Ok(Box::new(status_ex.clone()))),
        },
        ServerMsg::Response {
            id: 13,
            body: RpcResponse::StatusEx(Err(WireError::Rejected("seed".into()))),
        },
        ServerMsg::Admin {
            id: 1,
            body: AdminResponse::Membership(Box::new(status_ex)),
        },
        ServerMsg::Admin {
            id: 2,
            body: AdminResponse::Started,
        },
        ServerMsg::Admin {
            id: 3,
            body: AdminResponse::Done {
                log_id: Some(LogId::new(leader, 15)),
            },
        },
        ServerMsg::Admin {
            id: 4,
            body: AdminResponse::NotLeader {
                leader: Some(1),
                addr: Some("127.0.0.1:7001".into()),
            },
        },
        ServerMsg::Admin {
            id: 5,
            body: AdminResponse::Conflict {
                current: Some(LogId::new(leader, 13)),
            },
        },
        ServerMsg::Admin {
            id: 6,
            body: AdminResponse::Refused {
                reason: "seed".into(),
            },
        },
        ServerMsg::Admin {
            id: 7,
            body: AdminResponse::Unsupported,
        },
    ];
    for (i, m) in replies.iter().enumerate() {
        write("raft_wire", &format!("server-{i}"), &frame(1, enc(m)));
    }
    let meta = postcard::to_allocvec(&meta).expect("meta");
    let payload = fuzzing::sample_payload();
    fuzzing::restore_payload(&payload).expect("the sample payload restores");
    let mut seed = vec![0];
    seed.extend_from_slice(&payload);
    write("snapshot_decode", "payload", &seed);
    for version in [0u8, 1] {
        let mut seed = vec![1, version];
        seed.extend_from_slice(&(meta.len() as u16).to_le_bytes());
        seed.extend_from_slice(&meta);
        seed.extend_from_slice(&payload);
        write("snapshot_decode", &format!("file-v{}", 2 - version), &seed);
    }
}

fn main() {
    proto(Path::new("../tests/compat/cases"));
    wal();
    raft();
}
