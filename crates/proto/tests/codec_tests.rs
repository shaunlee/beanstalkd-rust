//! `ServerCodec` decoder tests: fragmentation, pipelining, JOB_TOO_BIG body
//! discard, EXPECTED_CRLF, over-long lines, and the `scan_line_end`
//! single-`\r` quirk.

#![allow(clippy::unwrap_used)]

use bstk_proto::{Command, Frame, Response, ServerCodec};
use bytes::{Buf, BytesMut};
use tokio_util::codec::Decoder;

const DEFAULT_MAX_JOB_SIZE: u32 = bstk_proto::DEFAULT_MAX_JOB_SIZE;

fn decode_all(codec: &mut ServerCodec, buf: &mut BytesMut) -> Vec<Frame> {
    let mut out = Vec::new();
    while let Some(frame) = codec.decode(buf).expect("decode should not error") {
        out.push(frame);
    }
    out
}

#[test]
fn decodes_simple_command() {
    let mut codec = ServerCodec::new(DEFAULT_MAX_JOB_SIZE);
    let mut buf = BytesMut::from(&b"list-tubes\r\n"[..]);
    let frames = decode_all(&mut codec, &mut buf);
    assert_eq!(frames, vec![Frame::Command(Command::ListTubes)]);
}

#[test]
fn decodes_put_with_body() {
    let mut codec = ServerCodec::new(DEFAULT_MAX_JOB_SIZE);
    let mut buf = BytesMut::from(&b"put 0 0 100 5\r\nhello\r\n"[..]);
    let frames = decode_all(&mut codec, &mut buf);
    assert_eq!(
        frames,
        vec![Frame::Command(Command::Put {
            pri: 0,
            delay: 0,
            ttr: 100,
            body: bytes::Bytes::from_static(b"hello"),
        })]
    );
}

#[test]
fn pipelined_commands_all_decoded() {
    let mut codec = ServerCodec::new(DEFAULT_MAX_JOB_SIZE);
    let mut buf = BytesMut::from(&b"use a\r\nwatch b\r\nlist-tubes\r\nquit\r\n"[..]);
    let frames = decode_all(&mut codec, &mut buf);
    assert_eq!(frames.len(), 4);
    assert!(matches!(frames[0], Frame::Command(Command::Use(_))));
    assert!(matches!(frames[1], Frame::Command(Command::Watch(_))));
    assert!(matches!(frames[2], Frame::Command(Command::ListTubes)));
    assert!(matches!(frames[3], Frame::Command(Command::Quit)));
}

#[test]
fn pipelined_put_then_command() {
    let mut codec = ServerCodec::new(DEFAULT_MAX_JOB_SIZE);
    let mut buf = BytesMut::from(&b"put 0 0 100 5\r\nhello\r\nlist-tubes\r\n"[..]);
    let frames = decode_all(&mut codec, &mut buf);
    assert_eq!(frames.len(), 2);
    assert!(matches!(frames[0], Frame::Command(Command::Put { .. })));
    assert_eq!(frames[1], Frame::Command(Command::ListTubes));
}

#[test]
fn one_byte_at_a_time_fragmentation() {
    let mut codec = ServerCodec::new(DEFAULT_MAX_JOB_SIZE);
    let full = b"use foo\r\nput 0 0 100 5\r\nhello\r\nlist-tubes\r\n".to_vec();
    let mut buf = BytesMut::new();
    let mut frames = Vec::new();
    for &byte in &full {
        buf.extend_from_slice(&[byte]);
        while let Some(frame) = codec.decode(&mut buf).expect("decode should not error") {
            frames.push(frame);
        }
    }
    assert_eq!(frames.len(), 3);
    assert!(matches!(frames[0], Frame::Command(Command::Use(_))));
    assert!(matches!(frames[1], Frame::Command(Command::Put { .. })));
    assert_eq!(frames[2], Frame::Command(Command::ListTubes));
}

#[test]
fn random_splits_fragmentation() {
    // Deterministic pseudo-random split points (no external RNG dependency
    // needed here; the proptest-based roundtrip test covers randomized
    // command generation separately).
    let full = b"use foo\r\nreserve-with-timeout 5\r\nbury 1 2\r\nput 0 0 100 11\r\nhello world\r\nstats\r\n".to_vec();
    let split_patterns: &[&[usize]] = &[
        &[1, 3, 7, 2, 5, 100],
        &[10, 1, 1, 1, 50, 20],
        &[
            2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
            2, 2, 2, 2, 2, 2, 2, 2,
        ],
    ];
    for pattern in split_patterns {
        let mut codec = ServerCodec::new(DEFAULT_MAX_JOB_SIZE);
        let mut buf = BytesMut::new();
        let mut frames = Vec::new();
        let mut pos = 0;
        let mut i = 0;
        while pos < full.len() {
            let chunk = pattern[i % pattern.len()].max(1).min(full.len() - pos);
            buf.extend_from_slice(&full[pos..pos + chunk]);
            pos += chunk;
            i += 1;
            while let Some(frame) = codec.decode(&mut buf).expect("decode should not error") {
                frames.push(frame);
            }
        }
        assert_eq!(frames.len(), 5, "pattern {pattern:?} produced {frames:?}");
        assert!(matches!(frames[0], Frame::Command(Command::Use(_))));
        assert!(matches!(
            frames[1],
            Frame::Command(Command::ReserveWithTimeout(5))
        ));
        assert!(matches!(
            frames[2],
            Frame::Command(Command::Bury { id: 1, pri: 2 })
        ));
        assert!(matches!(frames[3], Frame::Command(Command::Put { .. })));
        assert_eq!(frames[4], Frame::Command(Command::Stats));
    }
}

#[test]
fn job_too_big_then_next_command_ok() {
    let mut codec = ServerCodec::new(4); // tiny limit
    let mut buf = BytesMut::from(&b"put 0 0 100 10\r\n0123456789\r\nlist-tubes\r\n"[..]);
    let frames = decode_all(&mut codec, &mut buf);
    assert_eq!(
        frames,
        vec![
            Frame::Error(Response::JobTooBig),
            Frame::Command(Command::ListTubes),
        ]
    );
}

#[test]
fn job_too_big_discard_is_incremental_not_double_buffered() {
    // Feed the too-big body in small pieces; codec should not emit
    // anything until the full (body+2) bytes have arrived.
    let mut codec = ServerCodec::new(4);
    let mut buf = BytesMut::from(&b"put 0 0 100 10\r\n"[..]);
    assert_eq!(codec.decode(&mut buf).unwrap(), None);
    buf.extend_from_slice(b"012345");
    assert_eq!(codec.decode(&mut buf).unwrap(), None);
    buf.extend_from_slice(b"6789\r\n");
    assert_eq!(
        codec.decode(&mut buf).unwrap(),
        Some(Frame::Error(Response::JobTooBig))
    );
    // Next command still works normally.
    buf.extend_from_slice(b"list-tubes\r\n");
    assert_eq!(
        codec.decode(&mut buf).unwrap(),
        Some(Frame::Command(Command::ListTubes))
    );
}

#[test]
fn put_trailing_garbage_after_size_bad_format() {
    // Unlike the JOB_TOO_BIG case, when body_size is within limits the
    // trailing garbage on the `put` line is checked and rejected, and no
    // body bytes are consumed (they get reparsed as the next command).
    let mut codec = ServerCodec::new(DEFAULT_MAX_JOB_SIZE);
    let mut buf = BytesMut::from(&b"put 0 0 0 5 extra\r\nhello\r\n"[..]);
    let frames = decode_all(&mut codec, &mut buf);
    assert_eq!(
        frames,
        vec![
            Frame::Error(Response::BadFormat),
            Frame::Error(Response::UnknownCommand),
        ]
    );
}

#[test]
fn job_too_big_ignores_trailing_garbage_on_put_line() {
    let mut codec = ServerCodec::new(4);
    let mut buf = BytesMut::from(&b"put 0 0 100 10 extra garbage\r\n0123456789\r\nstats\r\n"[..]);
    let frames = decode_all(&mut codec, &mut buf);
    assert_eq!(
        frames,
        vec![
            Frame::Error(Response::JobTooBig),
            Frame::Command(Command::Stats)
        ]
    );
}

#[test]
fn expected_crlf_then_next_command_ok() {
    let mut codec = ServerCodec::new(DEFAULT_MAX_JOB_SIZE);
    // body_size=5, but body+trailer bytes are "helloXX" (wrong trailer).
    let mut buf = BytesMut::from(&b"put 0 0 100 5\r\nhelloXXlist-tubes\r\n"[..]);
    let frames = decode_all(&mut codec, &mut buf);
    assert_eq!(
        frames,
        vec![
            Frame::Error(Response::ExpectedCrlf),
            Frame::Command(Command::ListTubes),
        ]
    );
}

#[test]
fn overlong_line_then_next_command_ok() {
    let mut codec = ServerCodec::new(DEFAULT_MAX_JOB_SIZE);
    let long_line = vec![b'a'; 300];
    let mut buf = BytesMut::new();
    buf.extend_from_slice(&long_line);
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(b"list-tubes\r\n");
    let frames = decode_all(&mut codec, &mut buf);
    assert_eq!(
        frames,
        vec![
            Frame::Error(Response::BadFormat),
            Frame::Command(Command::ListTubes),
        ]
    );
}

#[test]
fn overlong_line_exact_boundary() {
    // "use " (4) + 220 'a's + "\r\n" (2) = 226 total, which does not fit
    // in the 224-byte window before the terminator would appear.
    let mut codec = ServerCodec::new(DEFAULT_MAX_JOB_SIZE);
    let mut buf = BytesMut::new();
    buf.extend_from_slice(b"use ");
    buf.extend_from_slice(&vec![b'a'; 220]);
    buf.extend_from_slice(b"\r\n");
    let frames = decode_all(&mut codec, &mut buf);
    assert_eq!(frames, vec![Frame::Error(Response::BadFormat)]);
}

#[test]
fn max_valid_line_length_222_chars_ok() {
    // content length 222 + "\r\n" = 224 total: exactly LINE_BUF_SIZE, still
    // valid. Use `kick` with zero-padded digits (no tube-name-length
    // limit to worry about) to isolate the line-length boundary.
    let mut codec = ServerCodec::new(DEFAULT_MAX_JOB_SIZE);
    let digits_len = 222 - "kick ".len();
    let mut digits = "0".repeat(digits_len - 1);
    digits.push('5');
    let mut buf = BytesMut::new();
    buf.extend_from_slice(b"kick ");
    buf.extend_from_slice(digits.as_bytes());
    buf.extend_from_slice(b"\r\n");
    assert_eq!(buf.len(), 224);
    let frames = decode_all(&mut codec, &mut buf);
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0], Frame::Command(Command::Kick(5)));
}

#[test]
fn bare_lf_does_not_terminate_line() {
    let mut codec = ServerCodec::new(DEFAULT_MAX_JOB_SIZE);
    let mut buf = BytesMut::from(&b"list-tubes\n"[..]);
    assert_eq!(codec.decode(&mut buf).unwrap(), None);
}

#[test]
fn bare_cr_not_followed_by_lf_blocks_later_valid_crlf() {
    // Quirk: scan_line_end only ever looks at the *first* '\r' byte; if
    // it's not immediately followed by '\n', the line is "not found yet"
    // even though a well-formed "\r\n" exists further along the buffer.
    let mut codec = ServerCodec::new(DEFAULT_MAX_JOB_SIZE);
    let mut buf = BytesMut::from(&b"list-tubes\rXlist-tubes\r\n"[..]);
    assert_eq!(codec.decode(&mut buf).unwrap(), None);
}

#[test]
fn discard_from_overflow_window_immediately_continues_scanning() {
    // If, after discarding a full LINE_BUF_SIZE window, the remaining
    // buffered bytes already contain a valid line terminator, it should
    // resolve in the same decode() call sequence without needing another
    // read: here the bad (overlong) line is terminated right after the
    // 224-byte window, and a separate valid command follows it.
    let mut codec = ServerCodec::new(DEFAULT_MAX_JOB_SIZE);
    let mut buf = BytesMut::new();
    buf.extend_from_slice(&vec![b'a'; 224]); // exactly one overflow window, no CRLF
    buf.extend_from_slice(b"\r\n"); // terminates the bad line
    buf.extend_from_slice(b"list-tubes\r\n");
    let frames = decode_all(&mut codec, &mut buf);
    assert_eq!(
        frames,
        vec![
            Frame::Error(Response::BadFormat),
            Frame::Command(Command::ListTubes),
        ]
    );
}

#[test]
fn discard_from_overflow_window_consumes_terminator_from_continuation() {
    // If no extra terminator separates the discarded window from the
    // next bytes, the first "\r\n" found in the continuation ends the bad
    // line (matching prot.c: there is no way to "resync" mid-line other
    // than finding a CRLF).
    let mut codec = ServerCodec::new(DEFAULT_MAX_JOB_SIZE);
    let mut buf = BytesMut::new();
    buf.extend_from_slice(&vec![b'a'; 224]);
    buf.extend_from_slice(b"list-tubes\r\n");
    let frames = decode_all(&mut codec, &mut buf);
    assert_eq!(frames, vec![Frame::Error(Response::BadFormat)]);
}

#[test]
fn empty_buffer_buf_advance_noop_sanity() {
    // Smoke test that decode() on an empty buffer just waits.
    let mut codec = ServerCodec::new(DEFAULT_MAX_JOB_SIZE);
    let mut buf = BytesMut::new();
    assert_eq!(codec.decode(&mut buf).unwrap(), None);
    buf.advance(0);
}
