//! Framing: command lines, put bodies, oversize lines and bodies.
//!
//! Mirrors `conn_process_io`'s `STATE_WANT_COMMAND` / `STATE_WANT_ENDLINE` /
//! `STATE_WANT_DATA` / `STATE_BITBUCKET` handling in prot.c, plus
//! `scan_line_end` and `_skip`. See docs/COMPAT.md for the subtler quirks
//! (e.g. the single-`\r` `scan_line_end` semantics).

use bytes::{Buf, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::parse::{ParsedLine, parse_line_raw};
use crate::{Command, Frame, LINE_BUF_SIZE, PutRejection, Response};

/// Internal decoder state, mirroring the relevant `Conn` states in prot.c.
#[derive(Debug)]
enum State {
    /// `STATE_WANT_COMMAND` / `STATE_WANT_ENDLINE`. `overflowed` tracks
    /// whether the line currently being scanned has already exceeded
    /// `LINE_BUF_SIZE` at least once (in which case the next line found
    /// produces `BAD_FORMAT` instead of being dispatched).
    Line { overflowed: bool },
    /// `STATE_WANT_DATA`: reading a `put` body of exactly
    /// `body_size + 2` bytes (the `+2` is the client's trailing `\r\n`).
    PutBody {
        pri: u32,
        delay: u32,
        ttr: u32,
        body_size: u32,
    },
    /// `STATE_BITBUCKET`: discarding `remaining` bytes of an oversized
    /// `put` body before emitting `PutRejected(JobTooBig)`.
    Discard { remaining: u64 },
}

/// Server-side codec. Decodes client bytes into `Frame`s and encodes
/// `Response`s.
#[derive(Debug)]
pub struct ServerCodec {
    max_job_size: u32,
    state: State,
    /// Whether to emit `Frame::PutStarted` when a put header is accepted.
    emit_put_started: bool,
    /// Whether `auth <token>` lines decode to `Frame::Auth`.
    recognize_auth: bool,
}

impl ServerCodec {
    pub fn new(max_job_size: u32) -> Self {
        ServerCodec {
            max_job_size,
            state: State::Line { overflowed: false },
            emit_put_started: false,
            recognize_auth: false,
        }
    }

    /// Decode `auth <token>` lines as `Frame::Auth` (token authentication,
    /// a beanstalkd-rs extension). Off by default, so `auth` is an unknown
    /// command exactly as in the reference.
    pub fn recognize_auth(mut self) -> Self {
        self.recognize_auth = true;
        self
    }

    /// Also emit `Frame::PutStarted` as soon as a put command line is
    /// accepted, before its body is read, so the caller can apply the
    /// reference's header-time side effects (see `Frame::PutStarted`).
    pub fn emit_put_started(mut self) -> Self {
        self.emit_put_started = true;
        self
    }
}

/// Result of scanning a buffer for a `\r\n`-terminated line, following
/// `scan_line_end`'s exact (and slightly surprising) semantics: it looks
/// only for the *first* `\r` byte; if that byte is not immediately
/// followed by `\n`, the line is considered not-yet-found, even if a
/// well-formed `\r\n` exists later in the buffer.
enum LineScan {
    /// Found a full line; `usize` is the content length (excluding `\r\n`).
    Found(usize),
    /// No `\r\n` found and fewer than `LINE_BUF_SIZE` bytes buffered yet:
    /// need more data.
    Incomplete,
    /// No `\r\n` found within the first `LINE_BUF_SIZE` bytes: the line is
    /// too long (`STATE_WANT_ENDLINE` territory).
    Overflow,
}

fn scan_line_end(buf: &[u8]) -> LineScan {
    let window_len = buf.len().min(LINE_BUF_SIZE);
    let window = &buf[..window_len];
    if window_len >= 2 {
        let search_region = &window[..window_len - 1];
        if let Some(pos) = search_region.iter().position(|&b| b == b'\r')
            && window[pos + 1] == b'\n'
        {
            return LineScan::Found(pos);
        }
    }
    if window_len == LINE_BUF_SIZE {
        LineScan::Overflow
    } else {
        LineScan::Incomplete
    }
}

impl Decoder for ServerCodec {
    type Item = Frame;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Frame>, Self::Error> {
        loop {
            match &mut self.state {
                State::Line { overflowed } => match scan_line_end(src) {
                    LineScan::Incomplete => return Ok(None),
                    LineScan::Overflow => {
                        // Discard exactly this LINE_BUF_SIZE window and
                        // keep scanning immediately: prot.c always issues
                        // a read() capped at the remaining buffer space,
                        // so the discard boundary is deterministic and
                        // independent of how the bytes actually arrived.
                        src.advance(LINE_BUF_SIZE);
                        *overflowed = true;
                    }
                    LineScan::Found(content_len) => {
                        let was_overflowed = *overflowed;
                        let line = src.split_to(content_len + 2);
                        let line = &line[..content_len];

                        if was_overflowed {
                            self.state = State::Line { overflowed: false };
                            return Ok(Some(Frame::Error(Response::BadFormat)));
                        }

                        if self.recognize_auth
                            && let Some(token) = line.strip_prefix(b"auth ")
                        {
                            self.state = State::Line { overflowed: false };
                            return Ok(Some(Frame::Auth(Bytes::copy_from_slice(token))));
                        }

                        match parse_line_raw(line) {
                            Err(resp) => {
                                self.state = State::Line { overflowed: false };
                                return Ok(Some(Frame::Error(resp)));
                            }
                            Ok(ParsedLine::Command(cmd)) => {
                                self.state = State::Line { overflowed: false };
                                return Ok(Some(Frame::Command(cmd)));
                            }
                            Ok(ParsedLine::Put(header)) => {
                                if header.body_size > self.max_job_size {
                                    // JOB_TOO_BIG: the trailing-garbage
                                    // check on the `put` line is skipped
                                    // entirely in this branch, matching
                                    // prot.c's check ordering.
                                    self.state = State::Discard {
                                        remaining: u64::from(header.body_size) + 2,
                                    };
                                    if self.emit_put_started {
                                        return Ok(Some(Frame::PutStarted { too_big: true }));
                                    }
                                } else if header.trailing_garbage {
                                    self.state = State::Line { overflowed: false };
                                    return Ok(Some(Frame::PutRejected(
                                        PutRejection::TrailingGarbage,
                                    )));
                                } else {
                                    self.state = State::PutBody {
                                        pri: header.pri,
                                        delay: header.delay,
                                        ttr: header.ttr,
                                        body_size: header.body_size,
                                    };
                                    if self.emit_put_started {
                                        return Ok(Some(Frame::PutStarted { too_big: false }));
                                    }
                                }
                            }
                        }
                    }
                },
                State::PutBody {
                    pri,
                    delay,
                    ttr,
                    body_size,
                } => {
                    let need = *body_size as usize + 2;
                    if src.len() < need {
                        // Avoid growing `src` far beyond what has actually
                        // arrived; a modest reservation still helps avoid
                        // repeated tiny reallocations for large bodies.
                        let extra = (need - src.len()).min(64 * 1024);
                        src.reserve(extra);
                        return Ok(None);
                    }
                    let pri = *pri;
                    let delay = *delay;
                    let ttr = *ttr;
                    let body_with_crlf = src.split_to(need).freeze();
                    self.state = State::Line { overflowed: false };
                    let crlf_ok =
                        body_with_crlf[need - 2] == b'\r' && body_with_crlf[need - 1] == b'\n';
                    if !crlf_ok {
                        return Ok(Some(Frame::PutRejected(PutRejection::ExpectedCrlf)));
                    }
                    let body: Bytes = body_with_crlf.slice(0..need - 2);
                    return Ok(Some(Frame::Command(Command::Put {
                        pri,
                        delay,
                        ttr,
                        body,
                    })));
                }
                State::Discard { remaining } => {
                    if src.is_empty() {
                        return Ok(None);
                    }
                    let take = (*remaining).min(src.len() as u64) as usize;
                    src.advance(take);
                    *remaining -= take as u64;
                    if *remaining > 0 {
                        return Ok(None);
                    }
                    self.state = State::Line { overflowed: false };
                    return Ok(Some(Frame::PutRejected(PutRejection::JobTooBig)));
                }
            }
        }
    }
}

impl Encoder<Response> for ServerCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: Response, dst: &mut BytesMut) -> Result<(), Self::Error> {
        item.encode(dst);
        Ok(())
    }
}
