//! Property test: encoding an arbitrary valid `Command` the way a
//! well-behaved client would, then decoding it through `ServerCodec`,
//! must return the same `Command`.

#![allow(clippy::unwrap_used)]

#[path = "support.rs"]
mod support;

use bstk_proto::{Frame, ServerCodec};
use bytes::BytesMut;
use proptest::prelude::*;
use support::{arb_command, encode_command};
use tokio_util::codec::Decoder;

proptest! {
    #[test]
    fn command_roundtrips_through_codec(cmd in arb_command()) {
        let encoded = encode_command(&cmd);
        // Bodies in the generator are bounded to 2000 bytes; use a limit
        // comfortably above that so we never hit JOB_TOO_BIG here.
        let mut codec = ServerCodec::new(1 << 20);
        let mut buf = BytesMut::from(&encoded[..]);
        let frame = codec.decode(&mut buf).unwrap();
        match frame {
            Some(Frame::Command(decoded)) => prop_assert_eq!(decoded, cmd),
            other => prop_assert!(false, "expected a Command frame, got {other:?} for input {encoded:?}"),
        }
        // No leftover bytes after decoding exactly one encoded command.
        prop_assert_eq!(buf.len(), 0);
    }

    #[test]
    fn command_roundtrips_one_byte_at_a_time(cmd in arb_command()) {
        let encoded = encode_command(&cmd);
        let mut codec = ServerCodec::new(1 << 20);
        let mut buf = BytesMut::new();
        let mut frames = Vec::new();
        for &b in &encoded {
            buf.extend_from_slice(&[b]);
            while let Some(frame) = codec.decode(&mut buf).unwrap() {
                frames.push(frame);
            }
        }
        prop_assert_eq!(frames.len(), 1);
        match &frames[0] {
            Frame::Command(decoded) => prop_assert_eq!(decoded, &cmd),
            other => prop_assert!(false, "expected a Command frame, got {other:?}"),
        }
    }
}
