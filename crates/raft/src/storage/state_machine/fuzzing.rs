//! Entry points for the `snapshot_decode` fuzz target (`fuzz/`, feature
//! `fuzzing`, never enabled by the server; docs/DESIGN.md §9.2).

use std::io;
use std::sync::Arc;

use bstk_engine::{Engine, EngineConfig, EngineInput, StaticSysInfo, SysInfo};
use bstk_proto::{Command, TubeName};

use super::{PAYLOAD_VERSION, SmMeta, SnapshotPayloadRef, SysFactory, restore_from};
use crate::{CONN_SEQ_BITS, NodeId, conn_id};

/// `-z` of the restoring node. Small, because the decoder's scratch buffer
/// is `-z` plus a margin and a large one only slows the fuzzer down.
pub const MAX_JOB_SIZE: u32 = 1024;

/// The restoring node.
pub const NODE: NodeId = 1;

fn cfg() -> EngineConfig {
    EngineConfig {
        max_job_size: MAX_JOB_SIZE,
        ..EngineConfig::default()
    }
}

fn sys() -> SysFactory {
    Arc::new(|| Box::new(StaticSysInfo::default()) as Box<dyn SysInfo>)
}

fn encode(meta: &SmMeta, engine: &Engine) -> Vec<u8> {
    let payload = SnapshotPayloadRef {
        version: PAYLOAD_VERSION,
        meta,
        engine: engine.state_view(),
    };
    postcard::to_allocvec(&payload).expect("encoding to memory cannot fail")
}

fn restore(payload: &[u8]) -> io::Result<(Engine, SmMeta)> {
    let (engine, meta, _) = restore_from(payload, payload.len() as u64, 0, &cfg(), &sys(), NODE)?;
    Ok((engine, meta))
}

/// Restores `payload` the way an installed snapshot is restored. When it is
/// accepted, panics unless re-encoding is stable and the restored engine
/// survives being driven: `import_state` must reject every state the
/// engine cannot run with.
pub fn restore_payload(payload: &[u8]) -> io::Result<()> {
    let (mut engine, meta) = restore(payload)?;
    let first = encode(&meta, &engine);
    let (again, meta2) = restore(&first).expect("a re-encoded snapshot must restore");
    assert!(
        encode(&meta2, &again) == first,
        "re-encoding a restored snapshot is not stable"
    );

    let mut out = Vec::new();
    let mut now = meta.last_now;
    engine.apply_input(now, EngineInput::Tick, &mut out);
    let tube = TubeName::new("fuzz").expect("valid tube name");
    for conn in engine.conn_ids() {
        for cmd in [
            Command::Stats,
            Command::ListTubesWatched,
            Command::Watch(tube.clone()),
            Command::ReserveWithTimeout(0),
            Command::PeekReady,
            Command::Kick(10),
        ] {
            engine.apply_input(now, EngineInput::Command { conn, cmd }, &mut out);
        }
    }
    // `highest_local` is unconstrained for a node without connections; the
    // server stops accepting once local numbers run out, so skip that case.
    let next_local = meta
        .highest_local
        .get(&NODE)
        .copied()
        .unwrap_or(0)
        .checked_add(1)
        .filter(|&l| l < 1 << CONN_SEQ_BITS);
    if let Some(local) = next_local {
        let fresh = conn_id(NODE, local);
        engine.apply_input(now, EngineInput::Connect(fresh), &mut out);
        for cmd in [
            Command::Put {
                pri: 1,
                delay: 0,
                ttr: 1,
                body: bytes::Bytes::from_static(b"x"),
            },
            Command::ListTubes,
            Command::Reserve,
        ] {
            engine.apply_input(now, EngineInput::Command { conn: fresh, cmd }, &mut out);
        }
    }
    // Far enough ahead for every TTR, delay and pause of a sane state to
    // expire; each deadline the engine reports is visited in order.
    let horizon = now.saturating_add(1 << 50);
    for _ in 0..64 {
        match engine.next_deadline() {
            Some(d) if d <= horizon => {
                now = now.max(d);
                engine.apply_input(now, EngineInput::Tick, &mut out);
            }
            _ => break,
        }
    }
    for conn in engine.conn_ids() {
        engine.apply_input(now, EngineInput::Disconnect(conn), &mut out);
    }
    engine.apply_input(now, EngineInput::Tick, &mut out);
    Ok(())
}

/// A valid payload with jobs in every state, waiting and reserving
/// connections and a paused tube (seed corpus).
pub fn sample_payload() -> Vec<u8> {
    let mut engine = Engine::new(1_000, cfg(), sys()());
    engine.set_local_conns(Some(super::local_conns(NODE)));
    let mut out = Vec::new();
    let t = |s: &str| TubeName::new(s).expect("valid tube name");
    let a = conn_id(NODE, 1);
    let b = conn_id(2, 1);
    let mut meta = SmMeta {
        started: true,
        last_now: 2_000,
        ..SmMeta::default()
    };
    for c in [a, b] {
        engine.apply_input(1_000, EngineInput::Connect(c), &mut out);
    }
    let put = |delay: u32, body: &'static [u8]| Command::Put {
        pri: 5,
        delay,
        ttr: 30,
        body: bytes::Bytes::from_static(body),
    };
    let script = [
        (a, Command::Use(t("jobs"))),
        (a, put(0, b"ready")),
        (a, put(0, b"reserved")),
        (a, put(0, b"buried")),
        (a, put(60, b"delayed")),
        (b, Command::Watch(t("jobs"))),
        (b, Command::Reserve),
        (b, Command::Reserve),
        (b, Command::Bury { id: 3, pri: 9 }),
        (
            a,
            Command::PauseTube {
                tube: t("paused"),
                delay: 100,
            },
        ),
        (b, Command::Watch(t("empty"))),
        (b, Command::Ignore(t("jobs"))),
        (b, Command::Ignore(t("default"))),
        (b, Command::Reserve),
    ];
    for (conn, cmd) in script {
        engine.apply_input(2_000, EngineInput::Command { conn, cmd }, &mut out);
    }
    meta.next_seq.insert(a, 9);
    meta.next_seq.insert(b, 9);
    meta.highest_local.insert(NODE, 1);
    meta.highest_local.insert(2, 1);
    encode(&meta, &engine)
}
