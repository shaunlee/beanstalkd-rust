//! Snapshot decoding. Mode 0 feeds the bytes to the payload decoder as an
//! installed snapshot's payload (`fuzzing::restore_payload`, which also
//! drives the restored engine). Mode 1 builds a `.snap` file with a correct
//! header and checksum around a fuzzed payload and meta, mode 2 writes the
//! bytes as a `.snap` file; both open it as a restarting node does
//! (`ClusterStateMachine::open`). Errors are fine; panics are not.
//!
//! Input: `mode`, then for mode 1 `flags` (bit 0: version 1 instead of 2;
//! bit 1: the largest sequence number in the file name), a little-endian
//! `u16` meta length, the meta and the payload.
#![no_main]

use std::sync::Arc;

use bstk_engine::{ConnId, EngineConfig, StaticSysInfo, SysInfo};
use bstk_proto::Response;
use bstk_raft::storage::state_machine::fuzzing::{self, MAX_JOB_SIZE, NODE};
use bstk_raft::storage::{ClusterStateMachine, ReplySink, SmOptions};
use libfuzzer_sys::fuzz_target;

struct NoSink;

impl ReplySink for NoSink {
    fn applied(&self, _: ConnId, _: u64) {}
    fn deliver(&self, _: ConnId, _: Response) {}
    fn closed(&self, _: ConnId) {}
}

fn snap_file(version: u8, meta: &[u8], payload: &[u8]) -> Vec<u8> {
    let mut lens = [0u8; 16];
    lens[..8].copy_from_slice(&(meta.len() as u64).to_le_bytes());
    lens[8..].copy_from_slice(&(payload.len() as u64).to_le_bytes());
    let v1 = version & 1 != 0;
    let crc = if v1 {
        crc32c::crc32c_append(crc32c::crc32c_append(crc32c::crc32c(&lens), meta), payload)
    } else {
        crc32c::crc32c_append(crc32c::crc32c_append(crc32c::crc32c(payload), meta), &lens)
    };
    let mut f = Vec::with_capacity(32 + meta.len() + payload.len());
    f.extend_from_slice(b"BSTKSNAP");
    f.extend_from_slice(&(if v1 { 1u32 } else { 2u32 }).to_le_bytes());
    f.extend_from_slice(&crc.to_le_bytes());
    f.extend_from_slice(&lens);
    let (a, b) = if v1 { (meta, payload) } else { (payload, meta) };
    f.extend_from_slice(a);
    f.extend_from_slice(b);
    f
}

fn open(file: &[u8], top: bool) {
    let dir = tempfile::tempdir().expect("temp dir");
    let seq = if top { u64::MAX } else { 1 };
    std::fs::write(dir.path().join(format!("{seq:020}.snap")), file).expect("write");
    let opts = SmOptions {
        node_id: NODE,
        engine: EngineConfig {
            max_job_size: MAX_JOB_SIZE,
            ..EngineConfig::default()
        },
        sys: Arc::new(|| Box::new(StaticSysInfo::default()) as Box<dyn SysInfo>),
        sink: Arc::new(NoSink),
    };
    let _ = ClusterStateMachine::open(dir.path(), opts);
}

fuzz_target!(|data: &[u8]| {
    match data {
        [0, payload @ ..] => {
            let _ = fuzzing::restore_payload(payload);
        }
        [1, version, m0, m1, rest @ ..] => {
            let meta_len = usize::from(u16::from_le_bytes([*m0, *m1])).min(rest.len());
            let (meta, payload) = rest.split_at(meta_len);
            open(&snap_file(*version, meta, payload), version & 2 != 0);
        }
        [2, file @ ..] => open(file, false),
        _ => {}
    }
});
