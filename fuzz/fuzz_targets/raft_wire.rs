//! Cluster wire decoding (`wire::decode` / `wire::read_frame`) of both
//! directions: any bytes must decode or fail without panicking or
//! allocating beyond the bounded limits, the two readers must agree, and
//! a decoded message must re-encode to bytes that decode to the same
//! encoding (the bounded deserializers accept what the encoder emits).
//!
//! Input: `selector`, then one frame (u32 big-endian length and payload).
#![no_main]

use std::sync::OnceLock;

use bstk_raft::wire::{self, ClientMsg, DEFAULT_MAX_FRAME, ServerMsg};
use libfuzzer_sys::fuzz_target;
use serde::Serialize;
use serde::de::DeserializeOwned;

fn runtime() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime")
    })
}

fn check<T: Serialize + DeserializeOwned>(frame: &[u8], max: usize) {
    let decoded = wire::decode::<T>(frame, max);
    let mut r = frame;
    let read = runtime().block_on(wire::read_frame::<_, T>(&mut r, max));
    match (&decoded, &read) {
        (Ok(Some(_)), Ok(Some(_))) | (Err(_), Err(_)) | (Ok(None), _) => {}
        _ => panic!("decode and read_frame disagree"),
    }
    let Ok(Some((msg, _))) = decoded else { return };
    let e1 = wire::encode(&msg, DEFAULT_MAX_FRAME).expect("a decoded message re-encodes");
    let (again, used) = wire::decode::<T>(&e1, DEFAULT_MAX_FRAME)
        .expect("a re-encoded message decodes")
        .expect("complete");
    assert_eq!(used, e1.len());
    let e2 = wire::encode(&again, DEFAULT_MAX_FRAME).expect("re-encode");
    assert_eq!(e1, e2, "encoding is not stable");
}

fuzz_target!(|data: &[u8]| {
    let [sel, frame @ ..] = data else { return };
    let max = if sel & 2 == 0 { DEFAULT_MAX_FRAME } else { 256 };
    if sel & 1 == 0 {
        check::<ClientMsg>(frame, max);
    } else {
        check::<ServerMsg>(frame, max);
    }
});
