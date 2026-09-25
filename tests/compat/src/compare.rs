//! Diffing two transcripts (one per server) produced from the same script.

use std::collections::HashMap;

use crate::conn::{Outcome, outcomes_equal};
use crate::dsl::Step;
use crate::escape::escape_bytes;
use crate::mask::mask_response;

/// A single point of disagreement between the two servers' transcripts.
#[derive(Debug, Clone)]
pub struct Mismatch {
    pub line: u32,
    pub step_desc: String,
    /// The most recent `send` on the same connection before this step, if any.
    pub last_send: Option<(u32, String)>,
    pub expected: String,
    pub actual: String,
}

/// Compare two transcripts (produced by [`crate::conn::execute`] from the
/// same step list) and report every step where they disagree.
pub fn compare(steps: &[Step], a: &[Outcome], b: &[Outcome]) -> Vec<Mismatch> {
    assert_eq!(
        steps.len(),
        a.len(),
        "internal error: transcript A length does not match step count"
    );
    assert_eq!(
        steps.len(),
        b.len(),
        "internal error: transcript B length does not match step count"
    );

    let mut mismatches = Vec::new();
    let mut last_send: HashMap<&str, (u32, String)> = HashMap::new();

    for (i, step) in steps.iter().enumerate() {
        if let crate::dsl::StepKind::Send { conn, data } = &step.kind {
            last_send.insert(conn.as_str(), (step.line, escape_bytes(data)));
        }

        if !outcomes_equal(&a[i], &b[i]) {
            let conn = step.kind.conn();
            let last_send = conn.and_then(|c| last_send.get(c).cloned());
            mismatches.push(Mismatch {
                line: step.line,
                step_desc: step.kind.describe(),
                last_send,
                expected: format_outcome(&a[i]),
                actual: format_outcome(&b[i]),
            });
        }
    }
    mismatches
}

fn format_outcome(o: &Outcome) -> String {
    match o {
        Outcome::Sent(n) => format!("sent({n} bytes)"),
        Outcome::Received(bytes) => format!("\"{}\"", escape_bytes(&mask_response(bytes))),
        Outcome::Timeout => "<timeout>".to_string(),
        Outcome::NoBytes => "<no bytes arrived>".to_string(),
        Outcome::SomeBytes(bytes) => format!("<bytes arrived: \"{}\">", escape_bytes(bytes)),
        Outcome::Closed => "<connection closed>".to_string(),
        Outcome::StillOpen => "<connection still open>".to_string(),
        Outcome::Slept => "<slept>".to_string(),
        Outcome::ShutdownDone => "<shutdown_write done>".to_string(),
        Outcome::ClosedDone => "<close done>".to_string(),
        Outcome::IoError(msg) => format!("<io error: {msg}>"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::StepKind;
    use std::path::PathBuf;

    fn step(line: u32, kind: StepKind) -> Step {
        Step { line, kind }
    }

    #[test]
    fn no_mismatches_for_identical_transcripts() {
        let steps = vec![step(
            1,
            StepKind::Send {
                conn: "c1".to_string(),
                data: b"stats\r\n".to_vec(),
            },
        )];
        let a = vec![Outcome::Sent(7)];
        let b = vec![Outcome::Sent(7)];
        assert!(compare(&steps, &a, &b).is_empty());
    }

    #[test]
    fn masking_suppresses_stats_mismatches() {
        let steps = vec![step(
            2,
            StepKind::Recv {
                conn: "c1".to_string(),
            },
        )];
        let a = vec![Outcome::Received(b"OK 9\r\npid: 111\n\r\n".to_vec())];
        let b = vec![Outcome::Received(b"OK 9\r\npid: 222\n\r\n".to_vec())];
        assert!(compare(&steps, &a, &b).is_empty());
    }

    #[test]
    fn reports_mismatch_with_last_send_and_line() {
        let steps = vec![
            step(
                1,
                StepKind::Send {
                    conn: "c1".to_string(),
                    data: b"reserve\r\n".to_vec(),
                },
            ),
            step(
                2,
                StepKind::Recv {
                    conn: "c1".to_string(),
                },
            ),
        ];
        let a = vec![
            Outcome::Sent(9),
            Outcome::Received(b"TIMED_OUT\r\n".to_vec()),
        ];
        let b = vec![Outcome::Sent(9), Outcome::Timeout];
        let mismatches = compare(&steps, &a, &b);
        assert_eq!(mismatches.len(), 1);
        let m = &mismatches[0];
        assert_eq!(m.line, 2);
        assert_eq!(m.last_send.as_ref().expect("send recorded").0, 1);
        assert!(
            m.last_send
                .as_ref()
                .expect("send recorded")
                .1
                .contains("reserve")
        );
        assert!(m.expected.contains("TIMED_OUT"));
        assert!(m.actual.contains("timeout"));
    }

    #[test]
    #[should_panic(expected = "does not match step count")]
    fn panics_on_length_mismatch() {
        let steps = vec![step(
            1,
            StepKind::Close {
                conn: "c1".to_string(),
            },
        )];
        let a = vec![Outcome::ClosedDone];
        let b: Vec<Outcome> = vec![];
        let _ = compare(&steps, &a, &b);
        let _unused = PathBuf::new();
    }
}
