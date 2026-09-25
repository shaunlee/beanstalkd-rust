//! Byte-exact response encoding (`MSG_*` / `reply_line` formats in prot.c).

use bytes::BytesMut;

use crate::Response;

fn push(dst: &mut BytesMut, s: &str) {
    dst.extend_from_slice(s.as_bytes());
}

/// Appends `<header>\r\n<body>\r\n` where `<header>` already includes the
/// announced byte count. Used by `RESERVED`, `FOUND` and `OK`.
fn push_with_body(dst: &mut BytesMut, header: &str, body: &[u8]) {
    push(dst, header);
    dst.extend_from_slice(body);
    dst.extend_from_slice(b"\r\n");
}

impl Response {
    /// Append the wire form (including trailing `\r\n`, and body + `\r\n`
    /// where applicable) to `dst`.
    pub fn encode(&self, dst: &mut BytesMut) {
        match self {
            Response::Inserted(id) => push(dst, &format!("INSERTED {id}\r\n")),
            Response::BuriedId(id) => push(dst, &format!("BURIED {id}\r\n")),
            Response::Buried => push(dst, "BURIED\r\n"),
            Response::Using(tube) => push(dst, &format!("USING {tube}\r\n")),
            Response::Reserved { id, body } => {
                push_with_body(dst, &format!("RESERVED {id} {}\r\n", body.len()), body);
            }
            Response::Found { id, body } => {
                push_with_body(dst, &format!("FOUND {id} {}\r\n", body.len()), body);
            }
            Response::Watching(n) => push(dst, &format!("WATCHING {n}\r\n")),
            Response::Kicked(n) => push(dst, &format!("KICKED {n}\r\n")),
            Response::KickedJob => push(dst, "KICKED\r\n"),
            Response::Ok(body) => {
                push_with_body(dst, &format!("OK {}\r\n", body.len()), body);
            }
            Response::DeadlineSoon => push(dst, "DEADLINE_SOON\r\n"),
            Response::TimedOut => push(dst, "TIMED_OUT\r\n"),
            Response::Deleted => push(dst, "DELETED\r\n"),
            Response::Released => push(dst, "RELEASED\r\n"),
            Response::Touched => push(dst, "TOUCHED\r\n"),
            Response::NotFound => push(dst, "NOT_FOUND\r\n"),
            Response::NotIgnored => push(dst, "NOT_IGNORED\r\n"),
            Response::Paused => push(dst, "PAUSED\r\n"),
            Response::ExpectedCrlf => push(dst, "EXPECTED_CRLF\r\n"),
            Response::JobTooBig => push(dst, "JOB_TOO_BIG\r\n"),
            Response::Draining => push(dst, "DRAINING\r\n"),
            Response::OutOfMemory => push(dst, "OUT_OF_MEMORY\r\n"),
            Response::InternalError => push(dst, "INTERNAL_ERROR\r\n"),
            Response::BadFormat => push(dst, "BAD_FORMAT\r\n"),
            Response::UnknownCommand => push(dst, "UNKNOWN_COMMAND\r\n"),
            Response::Authenticated => push(dst, "AUTHENTICATED\r\n"),
            Response::Unauthorized => push(dst, "UNAUTHORIZED\r\n"),
        }
    }
}
