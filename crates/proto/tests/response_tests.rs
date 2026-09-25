//! Byte-exact `Response::encode` tests, one per variant.

use bstk_proto::{Response, TubeName};
use bytes::{Bytes, BytesMut};

fn encode(resp: &Response) -> Vec<u8> {
    let mut buf = BytesMut::new();
    resp.encode(&mut buf);
    buf.to_vec()
}

fn tube(s: &str) -> TubeName {
    TubeName::new(s).expect("valid tube name in test fixture")
}

#[test]
fn inserted() {
    assert_eq!(encode(&Response::Inserted(42)), b"INSERTED 42\r\n");
}

#[test]
fn buried_id() {
    assert_eq!(encode(&Response::BuriedId(7)), b"BURIED 7\r\n");
}

#[test]
fn buried() {
    assert_eq!(encode(&Response::Buried), b"BURIED\r\n");
}

#[test]
fn using() {
    assert_eq!(
        encode(&Response::Using(tube("default"))),
        b"USING default\r\n"
    );
}

#[test]
fn reserved() {
    let resp = Response::Reserved {
        id: 5,
        body: Bytes::from_static(b"hello"),
    };
    assert_eq!(encode(&resp), b"RESERVED 5 5\r\nhello\r\n");
}

#[test]
fn reserved_empty_body() {
    let resp = Response::Reserved {
        id: 5,
        body: Bytes::new(),
    };
    assert_eq!(encode(&resp), b"RESERVED 5 0\r\n\r\n");
}

#[test]
fn found() {
    let resp = Response::Found {
        id: 9,
        body: Bytes::from_static(b"data"),
    };
    assert_eq!(encode(&resp), b"FOUND 9 4\r\ndata\r\n");
}

#[test]
fn watching() {
    assert_eq!(encode(&Response::Watching(3)), b"WATCHING 3\r\n");
}

#[test]
fn kicked_count() {
    assert_eq!(encode(&Response::Kicked(2)), b"KICKED 2\r\n");
}

#[test]
fn kicked_job() {
    assert_eq!(encode(&Response::KickedJob), b"KICKED\r\n");
}

#[test]
fn ok_payload() {
    let resp = Response::Ok(Bytes::from_static(b"---\nfoo: 1\n"));
    assert_eq!(encode(&resp), b"OK 11\r\n---\nfoo: 1\n\r\n");
}

#[test]
fn deadline_soon() {
    assert_eq!(encode(&Response::DeadlineSoon), b"DEADLINE_SOON\r\n");
}

#[test]
fn timed_out() {
    assert_eq!(encode(&Response::TimedOut), b"TIMED_OUT\r\n");
}

#[test]
fn deleted() {
    assert_eq!(encode(&Response::Deleted), b"DELETED\r\n");
}

#[test]
fn released() {
    assert_eq!(encode(&Response::Released), b"RELEASED\r\n");
}

#[test]
fn touched() {
    assert_eq!(encode(&Response::Touched), b"TOUCHED\r\n");
}

#[test]
fn not_found() {
    assert_eq!(encode(&Response::NotFound), b"NOT_FOUND\r\n");
}

#[test]
fn not_ignored() {
    assert_eq!(encode(&Response::NotIgnored), b"NOT_IGNORED\r\n");
}

#[test]
fn paused() {
    assert_eq!(encode(&Response::Paused), b"PAUSED\r\n");
}

#[test]
fn expected_crlf() {
    assert_eq!(encode(&Response::ExpectedCrlf), b"EXPECTED_CRLF\r\n");
}

#[test]
fn job_too_big() {
    assert_eq!(encode(&Response::JobTooBig), b"JOB_TOO_BIG\r\n");
}

#[test]
fn draining() {
    assert_eq!(encode(&Response::Draining), b"DRAINING\r\n");
}

#[test]
fn out_of_memory() {
    assert_eq!(encode(&Response::OutOfMemory), b"OUT_OF_MEMORY\r\n");
}

#[test]
fn internal_error() {
    assert_eq!(encode(&Response::InternalError), b"INTERNAL_ERROR\r\n");
}

#[test]
fn bad_format() {
    assert_eq!(encode(&Response::BadFormat), b"BAD_FORMAT\r\n");
}

#[test]
fn unknown_command() {
    assert_eq!(encode(&Response::UnknownCommand), b"UNKNOWN_COMMAND\r\n");
}
