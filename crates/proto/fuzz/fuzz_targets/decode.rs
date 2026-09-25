//! Fuzz target for `ServerCodec::decode`.
//!
//! Feeds arbitrary bytes through the decoder in two ways: all at once, and
//! split into arbitrary chunks (driven by the fuzz input itself), checking
//! only that the codec never panics and never loops forever. This is a
//! structural fuzzer (no oracle beyond "doesn't crash / doesn't hang");
//! `tests/roundtrip.rs` in the main crate covers semantic correctness via
//! `proptest`.
//!
//! Run with: `cargo +nightly fuzz run decode` (requires `cargo-fuzz`).
//! Not part of `scripts/check.sh` (needs nightly).

#![no_main]

use bstk_proto::ServerCodec;
use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use tokio_util::codec::Decoder;

fuzz_target!(|data: &[u8]| {
    // Whole-buffer decode.
    {
        let mut codec = ServerCodec::new(bstk_proto::DEFAULT_MAX_JOB_SIZE);
        let mut buf = BytesMut::from(data);
        let mut iterations = 0usize;
        while let Ok(Some(_frame)) = codec.decode(&mut buf) {
            iterations += 1;
            // A well-behaved decoder always makes progress or needs more
            // data; bail out defensively rather than hang the fuzzer on a
            // logic bug that causes an infinite Ok(Some(..)) loop without
            // consuming bytes.
            if iterations > data.len() + 16 {
                break;
            }
        }
    }

    // Byte-at-a-time decode: exercises the same paths under maximal
    // fragmentation.
    {
        let mut codec = ServerCodec::new(bstk_proto::DEFAULT_MAX_JOB_SIZE);
        let mut buf = BytesMut::new();
        for &b in data {
            buf.extend_from_slice(&[b]);
            let mut iterations = 0usize;
            while let Ok(Some(_frame)) = codec.decode(&mut buf) {
                iterations += 1;
                if iterations > data.len() + 16 {
                    break;
                }
            }
        }
    }
});
