//! Command-line parsing (`dispatch_cmd` / `which_cmd` / `read_u32` & co. in prot.c).
//!
//! This module intentionally mirrors prot.c very literally (including its
//! quirks) rather than writing a "clean" parser, so that behavior stays
//! byte-exact. See docs/COMPAT.md for a summary of the more surprising
//! quirks mirrored here.

use bytes::Bytes;

use crate::{Command, JobId, Response, TubeName};

/// `NAME_CHARS` in prot.c.
fn is_name_char(b: u8) -> bool {
    b.is_ascii_alphanumeric()
        || matches!(
            b,
            b'-' | b'+' | b'/' | b';' | b'.' | b'$' | b'_' | b'(' | b')'
        )
}

/// `is_valid_tube(name, MAX_TUBE_NAME_LEN - 1)`.
pub(crate) fn validate_tube_name(name: &str) -> Option<()> {
    let bytes = name.as_bytes();
    if bytes.is_empty() || bytes.len() > crate::MAX_TUBE_NAME_LEN {
        return None;
    }
    if bytes[0] == b'-' {
        return None;
    }
    if !bytes.iter().all(|&b| is_name_char(b)) {
        return None;
    }
    Some(())
}

/// Skip leading `' '` bytes only (not other whitespace), like the
/// `while (buf[0] == ' ') buf++;` loop in read_u32/read_u64/read_tube_name.
fn skip_leading_spaces(buf: &[u8]) -> &[u8] {
    let mut i = 0;
    while i < buf.len() && buf[i] == b' ' {
        i += 1;
    }
    &buf[i..]
}

/// Parses a run of leading ASCII digits into a `u128` (wide enough to
/// unambiguously detect "does not fit in u64" without itself overflowing
/// for any realistic input length). Returns `None` if there is no leading
/// digit at all (mirrors `strtoumax` returning `tend == buf`).
fn parse_digits(buf: &[u8]) -> Option<(u128, &[u8])> {
    if buf.first().is_none_or(|b| !b.is_ascii_digit()) {
        return None;
    }
    let mut i = 0;
    while i < buf.len() && buf[i].is_ascii_digit() {
        i += 1;
    }
    let mut val: u128 = 0;
    for &d in &buf[..i] {
        val = val.saturating_mul(10).saturating_add(u128::from(d - b'0'));
    }
    Some((val, &buf[i..]))
}

/// `read_u64`: skip leading spaces, reject non-digit, reject overflow of
/// u64 (mirrors `strtoumax` setting `errno = ERANGE`). Returns the parsed
/// value and the remainder of the buffer (not required to be fully
/// consumed; caller decides).
fn read_u64_partial(buf: &[u8]) -> Option<(u64, &[u8])> {
    let buf = skip_leading_spaces(buf);
    let (val, rest) = parse_digits(buf)?;
    if val > u128::from(u64::MAX) {
        return None;
    }
    Some((val as u64, rest))
}

/// `read_u32`: like `read_u64_partial` but additionally rejects values
/// that don't fit in u32.
fn read_u32_partial(buf: &[u8]) -> Option<(u32, &[u8])> {
    let (val, rest) = read_u64_partial(buf)?;
    if val > u64::from(u32::MAX) {
        return None;
    }
    Some((val as u32, rest))
}

/// `read_u64` with `end == NULL`: the whole remainder must be consumed.
fn read_u64_full(buf: &[u8]) -> Option<u64> {
    let (val, rest) = read_u64_partial(buf)?;
    rest.is_empty().then_some(val)
}

/// `read_u32` with `end == NULL` (also used for `read_duration` with
/// `end == NULL`, since we keep durations in whole seconds here).
fn read_u32_full(buf: &[u8]) -> Option<u32> {
    let (val, rest) = read_u32_partial(buf)?;
    rest.is_empty().then_some(val)
}

/// `read_tube_name`: skip leading spaces, then take the maximal run of
/// `NAME_CHARS` bytes (note: digits are `NAME_CHARS` too, so a tube name
/// immediately followed by a duration with no separating space greedily
/// swallows the duration's digits, per prot.c).
fn read_tube_name_span(buf: &[u8]) -> Option<(&[u8], &[u8])> {
    let buf = skip_leading_spaces(buf);
    let len = buf.iter().take_while(|&&b| is_name_char(b)).count();
    if len == 0 {
        return None;
    }
    Some((&buf[..len], &buf[len..]))
}

/// A tube name is valid ASCII (subset of `NAME_CHARS`) by construction, so
/// this conversion cannot fail.
fn tube_name_from_bytes(bytes: &[u8]) -> Option<TubeName> {
    let s = std::str::from_utf8(bytes).ok()?;
    TubeName::new(s)
}

/// A `use`/`watch`/`ignore`/`stats-tube` tube name must occupy the *entire*
/// remainder of the line: no leading-space skip, no trailing garbage
/// tolerated (unlike `read_tube_name_span`, which is only used by
/// `pause-tube`).
fn tube_name_from_full_rest(rest: &[u8]) -> Result<TubeName, Response> {
    let s = std::str::from_utf8(rest).map_err(|_| Response::BadFormat)?;
    TubeName::new(s).ok_or(Response::BadFormat)
}

/// Header fields parsed from a `put` command line, before the body has
/// been read by the codec.
pub(crate) struct PutHeader {
    pub pri: u32,
    pub delay: u32,
    pub ttr: u32,
    pub body_size: u32,
    /// Whether there was trailing garbage after `body_size` on the command
    /// line. Checked by the codec only when `body_size` is within limits
    /// (prot.c checks the size-limit *before* the trailing-garbage check,
    /// so oversized jobs with trailing garbage on the `put` line still get
    /// `JOB_TOO_BIG`, not `BAD_FORMAT`).
    pub trailing_garbage: bool,
}

/// Result of parsing one command line, before the codec has decided what
/// to do about a `put` body (which requires knowing `max_job_size`, a
/// runtime-configured value `parse_line` itself has no access to).
pub(crate) enum ParsedLine {
    Command(Command),
    Put(PutHeader),
}

const CMD_TESTS: &[(&[u8], Op)] = &[
    (b"put ", Op::Put),
    (b"peek ", Op::PeekJob),
    (b"peek-ready", Op::PeekReady),
    (b"peek-delayed", Op::PeekDelayed),
    (b"peek-buried", Op::PeekBuried),
    (b"reserve-with-timeout ", Op::ReserveTimeout),
    (b"reserve-job ", Op::ReserveJob),
    (b"reserve", Op::Reserve),
    (b"delete ", Op::Delete),
    (b"release ", Op::Release),
    (b"bury ", Op::Bury),
    (b"kick ", Op::Kick),
    (b"kick-job ", Op::KickJob),
    (b"touch ", Op::Touch),
    (b"stats-job ", Op::StatsJob),
    (b"stats-tube ", Op::StatsTube),
    (b"stats", Op::Stats),
    (b"use ", Op::Use),
    (b"watch ", Op::Watch),
    (b"ignore ", Op::Ignore),
    (b"list-tubes-watched", Op::ListTubesWatched),
    (b"list-tube-used", Op::ListTubeUsed),
    (b"list-tubes", Op::ListTubes),
    (b"quit", Op::Quit),
    (b"pause-tube", Op::PauseTube),
];

#[derive(Clone, Copy)]
enum Op {
    Put,
    PeekJob,
    PeekReady,
    PeekDelayed,
    PeekBuried,
    ReserveTimeout,
    ReserveJob,
    Reserve,
    Delete,
    Release,
    Bury,
    Kick,
    KickJob,
    Touch,
    StatsJob,
    StatsTube,
    Stats,
    Use,
    Watch,
    Ignore,
    ListTubesWatched,
    ListTubeUsed,
    ListTubes,
    Quit,
    PauseTube,
}

/// `which_cmd`: tries prefixes in exactly the order prot.c does (some are
/// prefixes of others, so order matters).
fn which_cmd(line: &[u8]) -> Option<(Op, usize)> {
    for &(prefix, op) in CMD_TESTS {
        if line.len() >= prefix.len() && &line[..prefix.len()] == prefix {
            return Some((op, prefix.len()));
        }
    }
    None
}

/// `strtoul(buf, &end, 10)` truncated into a `uint` (u32), as used verbatim
/// by `OP_KICK` (not `read_u32`!). Quirks mirrored here (verified against
/// the reference binary):
/// - skips any ASCII whitespace (not just `' '`), matching `isspace`.
/// - accepts an optional leading `+` or `-` sign; a negative value wraps
///   around within 64 bits (as `unsigned long` on a 64-bit host) and is
///   then truncated to 32 bits, all without error.
/// - no error for trailing garbage after the digits (unlike `read_u32`).
/// - only errors (`BAD_FORMAT`) when there are no digits at all, or the
///   64-bit magnitude itself overflows (`errno == ERANGE`).
fn parse_kick_count(buf: &[u8]) -> Option<u32> {
    let mut i = 0;
    while i < buf.len() && buf[i].is_ascii_whitespace() {
        i += 1;
    }
    let negative = match buf.get(i) {
        Some(b'-') => {
            i += 1;
            true
        }
        Some(b'+') => {
            i += 1;
            false
        }
        _ => false,
    };
    let digits_start = i;
    while i < buf.len() && buf[i].is_ascii_digit() {
        i += 1;
    }
    if i == digits_start {
        return None;
    }
    let mut val: u128 = 0;
    for &d in &buf[digits_start..i] {
        val = val.saturating_mul(10).saturating_add(u128::from(d - b'0'));
    }
    if val > u128::from(u64::MAX) {
        return None;
    }
    let val = val as u64;
    let val = if negative { val.wrapping_neg() } else { val };
    Some(val as u32)
}

/// Parse one command line (without the trailing `\r\n`) into a `Command`.
///
/// For `put`, the returned `Command::Put` carries an empty body; the codec
/// fills in the body after reading it. Errors are returned as the exact
/// `Response` the server must send (e.g. `BadFormat`, `UnknownCommand`).
pub fn parse_line(line: &[u8]) -> Result<Command, Response> {
    match parse_line_raw(line)? {
        ParsedLine::Command(cmd) => Ok(cmd),
        ParsedLine::Put(h) => Ok(Command::Put {
            pri: h.pri,
            delay: h.delay,
            ttr: h.ttr,
            body: Bytes::new(),
        }),
    }
}

/// Like `parse_line`, but for `put` also exposes the parsed `body_size`
/// (needed by the codec to know how many body bytes to read, and to
/// decide `JOB_TOO_BIG` vs `BAD_FORMAT` ordering). Not part of the public
/// API: only the codec needs this.
pub(crate) fn parse_line_raw(line: &[u8]) -> Result<ParsedLine, Response> {
    // "check for possible maliciousness": an embedded NUL byte in the
    // command line is rejected unconditionally, before any command-specific
    // parsing (mirrors the `strlen(c->cmd) != c->cmd_len - 2` check in
    // dispatch_cmd, which runs before `which_cmd`).
    if line.contains(&0u8) {
        return Err(Response::BadFormat);
    }

    let Some((op, prefix_len)) = which_cmd(line) else {
        return Err(Response::UnknownCommand);
    };
    let rest = &line[prefix_len..];

    match op {
        Op::Put => {
            let (pri, rest) = read_u32_partial(rest).ok_or(Response::BadFormat)?;
            let (delay, rest) = read_u32_partial(rest).ok_or(Response::BadFormat)?;
            let (ttr, rest) = read_u32_partial(rest).ok_or(Response::BadFormat)?;
            let (body_size, rest) = read_u32_partial(rest).ok_or(Response::BadFormat)?;
            Ok(ParsedLine::Put(PutHeader {
                pri,
                delay,
                ttr,
                body_size,
                trailing_garbage: !rest.is_empty(),
            }))
        }
        Op::PeekJob => {
            let id: JobId = read_u64_full(rest).ok_or(Response::BadFormat)?;
            Ok(ParsedLine::Command(Command::Peek(id)))
        }
        Op::PeekReady => {
            if !rest.is_empty() {
                return Err(Response::BadFormat);
            }
            Ok(ParsedLine::Command(Command::PeekReady))
        }
        Op::PeekDelayed => {
            if !rest.is_empty() {
                return Err(Response::BadFormat);
            }
            Ok(ParsedLine::Command(Command::PeekDelayed))
        }
        Op::PeekBuried => {
            if !rest.is_empty() {
                return Err(Response::BadFormat);
            }
            Ok(ParsedLine::Command(Command::PeekBuried))
        }
        Op::ReserveTimeout => {
            // Trailing garbage after the timeout value is NOT checked in
            // prot.c (the `end_buf` from `read_u32` is captured but never
            // examined for `OP_RESERVE_TIMEOUT`), so we deliberately
            // discard `_rest` here.
            let (timeout, _rest) = read_u32_partial(rest).ok_or(Response::BadFormat)?;
            if timeout > i32::MAX as u32 {
                return Err(Response::BadFormat);
            }
            Ok(ParsedLine::Command(Command::ReserveWithTimeout(timeout)))
        }
        Op::ReserveJob => {
            let id: JobId = read_u64_full(rest).ok_or(Response::BadFormat)?;
            Ok(ParsedLine::Command(Command::ReserveJob(id)))
        }
        Op::Reserve => {
            if !rest.is_empty() {
                return Err(Response::BadFormat);
            }
            Ok(ParsedLine::Command(Command::Reserve))
        }
        Op::Delete => {
            let id: JobId = read_u64_full(rest).ok_or(Response::BadFormat)?;
            Ok(ParsedLine::Command(Command::Delete(id)))
        }
        Op::Release => {
            let (id, rest) = read_u64_partial(rest).ok_or(Response::BadFormat)?;
            let (pri, rest) = read_u32_partial(rest).ok_or(Response::BadFormat)?;
            let delay = read_u32_full(rest).ok_or(Response::BadFormat)?;
            Ok(ParsedLine::Command(Command::Release { id, pri, delay }))
        }
        Op::Bury => {
            let (id, rest) = read_u64_partial(rest).ok_or(Response::BadFormat)?;
            let pri = read_u32_full(rest).ok_or(Response::BadFormat)?;
            Ok(ParsedLine::Command(Command::Bury { id, pri }))
        }
        Op::Kick => {
            let count = parse_kick_count(rest).ok_or(Response::BadFormat)?;
            Ok(ParsedLine::Command(Command::Kick(count)))
        }
        Op::KickJob => {
            let id: JobId = read_u64_full(rest).ok_or(Response::BadFormat)?;
            Ok(ParsedLine::Command(Command::KickJob(id)))
        }
        Op::Touch => {
            let id: JobId = read_u64_full(rest).ok_or(Response::BadFormat)?;
            Ok(ParsedLine::Command(Command::Touch(id)))
        }
        Op::StatsJob => {
            let id: JobId = read_u64_full(rest).ok_or(Response::BadFormat)?;
            Ok(ParsedLine::Command(Command::StatsJob(id)))
        }
        Op::StatsTube => {
            let tube = tube_name_from_full_rest(rest)?;
            Ok(ParsedLine::Command(Command::StatsTube(tube)))
        }
        Op::Stats => {
            if !rest.is_empty() {
                return Err(Response::BadFormat);
            }
            Ok(ParsedLine::Command(Command::Stats))
        }
        Op::Use => {
            let tube = tube_name_from_full_rest(rest)?;
            Ok(ParsedLine::Command(Command::Use(tube)))
        }
        Op::Watch => {
            let tube = tube_name_from_full_rest(rest)?;
            Ok(ParsedLine::Command(Command::Watch(tube)))
        }
        Op::Ignore => {
            let tube = tube_name_from_full_rest(rest)?;
            Ok(ParsedLine::Command(Command::Ignore(tube)))
        }
        Op::ListTubesWatched => {
            if !rest.is_empty() {
                return Err(Response::BadFormat);
            }
            Ok(ParsedLine::Command(Command::ListTubesWatched))
        }
        Op::ListTubeUsed => {
            if !rest.is_empty() {
                return Err(Response::BadFormat);
            }
            Ok(ParsedLine::Command(Command::ListTubeUsed))
        }
        Op::ListTubes => {
            if !rest.is_empty() {
                return Err(Response::BadFormat);
            }
            Ok(ParsedLine::Command(Command::ListTubes))
        }
        Op::Quit => {
            // prot.c does not check trailing garbage for `quit` at all: it
            // closes the connection unconditionally once the prefix
            // matches (the malicious-NUL check above still applies).
            Ok(ParsedLine::Command(Command::Quit))
        }
        Op::PauseTube => {
            let (name, rest) = read_tube_name_span(rest).ok_or(Response::BadFormat)?;
            let delay = read_u32_full(rest).ok_or(Response::BadFormat)?;
            // prot.c increments op_ct[OP_PAUSE_TUBE] *before* running
            // is_valid_tube on the name span, so an invalid name is still a
            // counted command (the engine replies BAD_FORMAT).
            match tube_name_from_bytes(name) {
                Some(tube) => Ok(ParsedLine::Command(Command::PauseTube { tube, delay })),
                None => Ok(ParsedLine::Command(Command::PauseTubeBadName)),
            }
        }
    }
}
