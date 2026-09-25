//! Parser for the differential-test case DSL (`tests/compat/cases/*.bt`).
//!
//! Grammar, one directive per line:
//!
//! ```text
//! # a comment (also allowed as a trailing "  # comment" after a directive)
//! !args -z 10                     # optional, extra CLI args for both servers
//! @c1 send "put 0 0 5\r\nhello\r\n"   # \r \n \\ \" \xNN escapes are supported
//! @c1 recv                        # read one complete response (line [+ body])
//! @c1 recv_none 200ms             # assert (record) that nothing arrives for 200ms
//! @c1 recv_closed 200ms           # record whether the peer closed the connection
//! @c1 shutdown_write              # half-close (TCP FIN on the write side)
//! @c1 close                       # close and forget the connection
//! sleep 1100ms                    # sleep the whole harness for a duration
//! ```
//!
//! Connections are named `@c1`, `@c2`, ... and are opened lazily the first
//! time they are referenced.

use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// A single parsed step together with its 1-based source line number.
#[derive(Debug, Clone)]
pub struct Step {
    pub line: u32,
    pub kind: StepKind,
}

#[derive(Debug, Clone)]
pub enum StepKind {
    Send { conn: String, data: Vec<u8> },
    Recv { conn: String },
    RecvNone { conn: String, dur: Duration },
    RecvClosed { conn: String, dur: Duration },
    Sleep(Duration),
    ShutdownWrite { conn: String },
    Close { conn: String },
}

impl StepKind {
    /// The connection name this step acts on, if any (`Sleep` has none).
    pub fn conn(&self) -> Option<&str> {
        match self {
            StepKind::Send { conn, .. }
            | StepKind::Recv { conn }
            | StepKind::RecvNone { conn, .. }
            | StepKind::RecvClosed { conn, .. }
            | StepKind::ShutdownWrite { conn }
            | StepKind::Close { conn } => Some(conn.as_str()),
            StepKind::Sleep(_) => None,
        }
    }

    /// A short human-readable description of this step, for diagnostics.
    pub fn describe(&self) -> String {
        match self {
            StepKind::Send { conn, data } => {
                format!("@{conn} send \"{}\"", crate::escape::escape_bytes(data))
            }
            StepKind::Recv { conn } => format!("@{conn} recv"),
            StepKind::RecvNone { conn, dur } => format!("@{conn} recv_none {dur:?}"),
            StepKind::RecvClosed { conn, dur } => format!("@{conn} recv_closed {dur:?}"),
            StepKind::Sleep(dur) => format!("sleep {dur:?}"),
            StepKind::ShutdownWrite { conn } => format!("@{conn} shutdown_write"),
            StepKind::Close { conn } => format!("@{conn} close"),
        }
    }
}

/// A parsed `.bt` case file.
#[derive(Debug, Clone)]
pub struct CaseFile {
    pub path: PathBuf,
    /// Extra CLI arguments to pass to both server binaries (from `!args`).
    pub extra_args: Vec<String>,
    pub steps: Vec<Step>,
}

impl CaseFile {
    pub fn name(&self) -> String {
        self.path
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| self.path.to_string_lossy().into_owned())
    }
}

#[derive(Debug, Clone)]
pub struct ParseError {
    pub path: PathBuf,
    pub line: u32,
    pub message: String,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}: {}", self.path.display(), self.line, self.message)
    }
}

impl std::error::Error for ParseError {}

/// Parse a case file from disk.
pub fn parse_case_file(path: &Path) -> Result<CaseFile, ParseError> {
    let text = fs::read_to_string(path).map_err(|e| ParseError {
        path: path.to_path_buf(),
        line: 0,
        message: format!("cannot read case file: {e}"),
    })?;
    parse_case_str(path, &text)
}

/// Parse case source text already loaded into memory (used by unit tests).
pub fn parse_case_str(path: &Path, text: &str) -> Result<CaseFile, ParseError> {
    let mut extra_args = Vec::new();
    let mut steps = Vec::new();

    for (idx, raw_line) in text.lines().enumerate() {
        let line_no = (idx + 1) as u32;
        let err = |message: String| ParseError {
            path: path.to_path_buf(),
            line: line_no,
            message,
        };

        let stripped = strip_comment(raw_line);
        let line = stripped.trim();
        if line.is_empty() {
            continue;
        }

        if let Some(rest) = line.strip_prefix("!args") {
            let rest = rest.trim();
            if rest.is_empty() {
                return Err(err("!args requires at least one argument".to_string()));
            }
            extra_args.extend(rest.split_whitespace().map(str::to_string));
            continue;
        }

        if let Some(rest) = line.strip_prefix("sleep") {
            if !rest.starts_with(char::is_whitespace) {
                return Err(err(format!("unknown directive: {line}")));
            }
            let dur = parse_duration(rest.trim()).map_err(&err)?;
            steps.push(Step {
                line: line_no,
                kind: StepKind::Sleep(dur),
            });
            continue;
        }

        if let Some(rest) = line.strip_prefix('@') {
            let (conn_tok, rest) = split_first_word(rest);
            if conn_tok.is_empty() {
                return Err(err(
                    "expected a connection name after '@' (e.g. @c1)".to_string()
                ));
            }
            if !conn_tok
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_')
            {
                return Err(err(format!(
                    "invalid connection name '@{conn_tok}': only alphanumerics and '_' are allowed"
                )));
            }
            let conn = conn_tok.to_string();
            let (action, rest) = split_first_word(rest.trim_start());
            let kind = match action {
                "send" => {
                    let data = parse_quoted(rest.trim(), &err)?;
                    StepKind::Send { conn, data }
                }
                "recv" => {
                    if !rest.trim().is_empty() {
                        return Err(err("recv takes no arguments".to_string()));
                    }
                    StepKind::Recv { conn }
                }
                "recv_none" => {
                    let dur = parse_duration(rest.trim()).map_err(&err)?;
                    StepKind::RecvNone { conn, dur }
                }
                "recv_closed" => {
                    let dur = parse_duration(rest.trim()).map_err(&err)?;
                    StepKind::RecvClosed { conn, dur }
                }
                "shutdown_write" => {
                    if !rest.trim().is_empty() {
                        return Err(err("shutdown_write takes no arguments".to_string()));
                    }
                    StepKind::ShutdownWrite { conn }
                }
                "close" => {
                    if !rest.trim().is_empty() {
                        return Err(err("close takes no arguments".to_string()));
                    }
                    StepKind::Close { conn }
                }
                "" => return Err(err("expected an action after connection name".to_string())),
                other => return Err(err(format!("unknown connection action: {other}"))),
            };
            steps.push(Step {
                line: line_no,
                kind,
            });
            continue;
        }

        return Err(err(format!("unrecognized directive: {line}")));
    }

    Ok(CaseFile {
        path: path.to_path_buf(),
        extra_args,
        steps,
    })
}

/// Strip a trailing `# ...` comment, ignoring `#` characters that appear
/// inside a double-quoted string.
fn strip_comment(line: &str) -> &str {
    let mut in_quotes = false;
    let mut escaped = false;
    for (i, c) in line.char_indices() {
        if in_quotes {
            if escaped {
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else if c == '"' {
                in_quotes = false;
            }
        } else if c == '"' {
            in_quotes = true;
        } else if c == '#' {
            return &line[..i];
        }
    }
    line
}

/// Split off the first whitespace-delimited word, returning `(word, rest)`.
fn split_first_word(s: &str) -> (&str, &str) {
    match s.find(char::is_whitespace) {
        Some(i) => (&s[..i], &s[i..]),
        None => (s, ""),
    }
}

fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("expected a duration (e.g. 200ms, 1s)".to_string());
    }
    if let Some(num) = s.strip_suffix("ms") {
        let ms: u64 = num
            .trim()
            .parse()
            .map_err(|_| format!("invalid millisecond duration: {s}"))?;
        return Ok(Duration::from_millis(ms));
    }
    if let Some(num) = s.strip_suffix('s') {
        let secs: f64 = num
            .trim()
            .parse()
            .map_err(|_| format!("invalid second duration: {s}"))?;
        if secs < 0.0 || !secs.is_finite() {
            return Err(format!("invalid second duration: {s}"));
        }
        return Ok(Duration::from_secs_f64(secs));
    }
    Err(format!("duration must end with 'ms' or 's': {s}"))
}

/// Parse a double-quoted string literal with `\r \n \\ \" \xNN` escapes.
fn parse_quoted(s: &str, err: &dyn Fn(String) -> ParseError) -> Result<Vec<u8>, ParseError> {
    let mut chars = s.char_indices().peekable();
    match chars.next() {
        Some((_, '"')) => {}
        _ => {
            return Err(err(
                "send requires a double-quoted string argument".to_string()
            ));
        }
    }

    let bytes = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 1usize; // byte index, right after the opening quote
    loop {
        if i >= bytes.len() {
            return Err(err("unterminated string literal".to_string()));
        }
        let b = bytes[i];
        if b == b'"' {
            // The rest of the line (already comment-stripped) must be blank.
            if s[i + 1..].trim().is_empty() {
                return Ok(out);
            }
            return Err(err(
                "unexpected trailing content after closing quote".to_string()
            ));
        }
        if b == b'\\' {
            i += 1;
            if i >= bytes.len() {
                return Err(err("unterminated escape sequence".to_string()));
            }
            match bytes[i] {
                b'r' => {
                    out.push(b'\r');
                    i += 1;
                }
                b'n' => {
                    out.push(b'\n');
                    i += 1;
                }
                b'\\' => {
                    out.push(b'\\');
                    i += 1;
                }
                b'"' => {
                    out.push(b'"');
                    i += 1;
                }
                b't' => {
                    out.push(b'\t');
                    i += 1;
                }
                b'x' => {
                    if i + 2 >= bytes.len() {
                        return Err(err("incomplete \\xNN escape".to_string()));
                    }
                    let hex = std::str::from_utf8(&bytes[i + 1..i + 3])
                        .map_err(|_| err(format!("invalid \\x escape near byte {i}")))?;
                    let val = u8::from_str_radix(hex, 16)
                        .map_err(|_| err(format!("invalid hex digits in \\x{hex}")))?;
                    out.push(val);
                    i += 3;
                }
                other => {
                    return Err(err(format!(
                        "unsupported escape '\\{}': only \\r \\n \\t \\\\ \\\" \\xNN are allowed",
                        other as char
                    )));
                }
            }
            continue;
        }
        out.push(b);
        i += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path() -> PathBuf {
        PathBuf::from("test.bt")
    }

    #[test]
    fn parses_send_with_escapes() {
        let src = r#"@c1 send "put 0 0 5\r\nhello\r\n""#;
        let case = parse_case_str(&path(), src).expect("should parse");
        assert_eq!(case.steps.len(), 1);
        match &case.steps[0].kind {
            StepKind::Send { conn, data } => {
                assert_eq!(conn, "c1");
                assert_eq!(data, b"put 0 0 5\r\nhello\r\n");
            }
            other => panic!("unexpected step: {other:?}"),
        }
    }

    #[test]
    fn parses_hex_escape() {
        let src = r#"@c1 send "\x00\x1b\xFF""#;
        let case = parse_case_str(&path(), src).expect("should parse");
        match &case.steps[0].kind {
            StepKind::Send { data, .. } => assert_eq!(data, &[0x00, 0x1b, 0xff]),
            other => panic!("unexpected step: {other:?}"),
        }
    }

    #[test]
    fn parses_comment_lines_and_trailing_comments() {
        let src = "# a full-line comment\n@c1 recv   # trailing comment\n";
        let case = parse_case_str(&path(), src).expect("should parse");
        assert_eq!(case.steps.len(), 1);
        assert!(matches!(&case.steps[0].kind, StepKind::Recv { conn } if conn == "c1"));
    }

    #[test]
    fn hash_inside_quotes_is_not_a_comment() {
        let src = r#"@c1 send "a#b""#;
        let case = parse_case_str(&path(), src).expect("should parse");
        match &case.steps[0].kind {
            StepKind::Send { data, .. } => assert_eq!(data, b"a#b"),
            other => panic!("unexpected step: {other:?}"),
        }
    }

    #[test]
    fn parses_sleep_ms_and_s() {
        let src = "sleep 1100ms\nsleep 2s\n";
        let case = parse_case_str(&path(), src).expect("should parse");
        assert!(
            matches!(case.steps[0].kind, StepKind::Sleep(d) if d == Duration::from_millis(1100))
        );
        assert!(matches!(case.steps[1].kind, StepKind::Sleep(d) if d == Duration::from_secs(2)));
    }

    #[test]
    fn parses_recv_none_recv_closed_shutdown_close() {
        let src = "@c1 recv_none 200ms\n@c1 recv_closed 50ms\n@c1 shutdown_write\n@c1 close\n";
        let case = parse_case_str(&path(), src).expect("should parse");
        assert_eq!(case.steps.len(), 4);
        assert!(matches!(
            case.steps[0].kind,
            StepKind::RecvNone { dur, .. } if dur == Duration::from_millis(200)
        ));
        assert!(matches!(
            case.steps[1].kind,
            StepKind::RecvClosed { dur, .. } if dur == Duration::from_millis(50)
        ));
        assert!(matches!(case.steps[2].kind, StepKind::ShutdownWrite { .. }));
        assert!(matches!(case.steps[3].kind, StepKind::Close { .. }));
    }

    #[test]
    fn parses_args_header() {
        let src = "!args -z 10\n@c1 send \"x\"\n";
        let case = parse_case_str(&path(), src).expect("should parse");
        assert_eq!(case.extra_args, vec!["-z".to_string(), "10".to_string()]);
    }

    #[test]
    fn error_on_unterminated_string() {
        let src = r#"@c1 send "abc"#;
        let err = parse_case_str(&path(), src).expect_err("should fail");
        assert_eq!(err.line, 1);
        assert!(err.message.contains("unterminated"));
    }

    #[test]
    fn error_on_bad_escape() {
        let src = r#"@c1 send "\q""#;
        let err = parse_case_str(&path(), src).expect_err("should fail");
        assert!(err.message.contains("unsupported escape"));
    }

    #[test]
    fn error_on_unknown_directive() {
        let src = "frobnicate\n";
        let err = parse_case_str(&path(), src).expect_err("should fail");
        assert_eq!(err.line, 1);
        assert!(err.message.contains("unrecognized directive"));
    }

    #[test]
    fn error_on_unknown_action() {
        let src = "@c1 bogus\n";
        let err = parse_case_str(&path(), src).expect_err("should fail");
        assert!(err.message.contains("unknown connection action"));
    }

    #[test]
    fn error_message_includes_file_and_line() {
        let src = "@c1 recv\nfrobnicate\n";
        let err = parse_case_str(Path::new("cases/foo.bt"), src).expect_err("should fail");
        let text = err.to_string();
        assert!(text.starts_with("cases/foo.bt:2:"));
    }

    #[test]
    fn error_on_bad_duration_suffix() {
        let src = "sleep 200\n";
        let err = parse_case_str(&path(), src).expect_err("should fail");
        assert!(err.message.contains("duration must end with"));
    }
}
