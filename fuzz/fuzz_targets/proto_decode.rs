//! Client protocol framing (`ServerCodec`): any byte stream, delivered in
//! any chunking, must decode without panicking to the same frames as when
//! delivered at once (the codec's discard boundaries are defined to be
//! independent of how bytes arrive, as in prot.c).
//!
//! Input: `flags`, `n`, `n` chunk sizes, then the stream.
#![no_main]

use bstk_proto::{DEFAULT_MAX_JOB_SIZE, Frame, MAX_JOB_SIZE_LIMIT, ServerCodec};
use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use tokio_util::codec::Decoder;

fn codec(flags: u8) -> ServerCodec {
    let max = match flags & 3 {
        0 => 0,
        1 => 4,
        2 => DEFAULT_MAX_JOB_SIZE,
        _ => MAX_JOB_SIZE_LIMIT,
    };
    let mut c = ServerCodec::new(max);
    if flags & 4 != 0 {
        c = c.recognize_auth();
    }
    if flags & 8 != 0 {
        c = c.emit_put_started();
    }
    c
}

fn drain(c: &mut ServerCodec, buf: &mut BytesMut, frames: &mut Vec<Frame>) {
    while let Some(f) = c.decode(buf).expect("ServerCodec never fails") {
        frames.push(f);
    }
}

fuzz_target!(|data: &[u8]| {
    let [flags, n, rest @ ..] = data else { return };
    let n = usize::from(*n % 16).min(rest.len());
    let (sizes, stream) = rest.split_at(n);

    let mut whole = Vec::new();
    let mut c = codec(*flags);
    let mut whole_rest = BytesMut::from(stream);
    drain(&mut c, &mut whole_rest, &mut whole);

    let mut chunked = Vec::new();
    let mut c = codec(*flags);
    let mut buf = BytesMut::new();
    let mut pos = 0;
    let mut i = 0;
    while pos < stream.len() {
        let size = sizes
            .get(i % n.max(1))
            .map_or(1, |&s| usize::from(s).max(1));
        let end = (pos + size).min(stream.len());
        buf.extend_from_slice(&stream[pos..end]);
        drain(&mut c, &mut buf, &mut chunked);
        pos = end;
        i += 1;
    }

    assert_eq!(whole, chunked, "frames depend on how the stream was split");
    assert_eq!(
        whole_rest, buf,
        "buffered bytes depend on how the stream was split"
    );
});
