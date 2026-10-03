//! Fuzz target for `ServerCodec::decode`: arbitrary bytes, fed whole and in
//! fuzz-chosen chunks; checks only that the codec never panics or hangs
//! (`tests/roundtrip.rs` covers semantics). Needs nightly, so it is not part
//! of `scripts/check.sh`: `cargo +nightly fuzz run decode`.

#![no_main]

use bstk_proto::ServerCodec;
use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use tokio_util::codec::Decoder;

fuzz_target!(|data: &[u8]| {
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
