//! Masking of volatile fields in `OK <n>\r\n<yaml>\r\n` responses (stats,
//! stats-job, stats-tube) so that differential comparisons ignore fields
//! that legitimately vary between two separate server processes/runs.
//!
//! See docs/DESIGN.md section 8.

/// YAML keys whose values are masked in any `OK` response body. None of
/// these names appears in more than one of the stats formats.
const MASKED_KEYS: &[&str] = &[
    // stats (server-wide)
    "pid",
    "version",
    "rusage-utime",
    "rusage-stime",
    "uptime",
    "hostname",
    "os",
    "platform",
    // stats-job
    "age",
    "time-left",
    // stats-tube
    "pause-time-left",
];

/// Keys masked only in server-wide `stats`. `id` is the random server id
/// there, but the (deterministic) job id in `stats-job`, which must be
/// compared.
const SERVER_STATS_ONLY_KEYS: &[&str] = &["id"];

/// A key that only appears in server-wide `stats` output.
const SERVER_STATS_MARKER: &[u8] = b"\ntotal-connections: ";

/// Keys masked only when the case runs with a binlog directory (see
/// docs/PLAN.md section 4.2, decision 7): file numbering and compaction
/// moves are layout details that may legitimately differ. `file` appears
/// only in `stats-job`; the `binlog-*` keys appear only in `stats`.
/// `binlog-records-written` and `binlog-max-size` stay compared.
const BINLOG_MASKED_KEYS: &[&str] = &[
    // stats-job
    "file",
    // stats
    "binlog-oldest-index",
    "binlog-current-index",
    "binlog-records-migrated",
];

const MASKED_VALUE: &str = "<masked>";

/// Which optional masks apply to a case.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MaskMode {
    /// The servers run with a binlog directory: also mask
    /// [`BINLOG_MASKED_KEYS`].
    pub binlog: bool,
}

/// Mask volatile fields in a full response (as captured by the harness: the
/// status line plus any trailing body). Only `OK <n>\r\n...` responses are
/// touched; anything else (RESERVED/FOUND job bodies, plain word replies,
/// list-tubes YAML lists, ...) is returned unchanged.
pub fn mask_response(resp: &[u8]) -> Vec<u8> {
    mask_response_with(resp, MaskMode::default())
}

/// Like [`mask_response`], additionally applying the masks enabled by `mode`.
pub fn mask_response_with(resp: &[u8], mode: MaskMode) -> Vec<u8> {
    let Some(line_end) = find(resp, b"\r\n") else {
        return resp.to_vec();
    };
    let head = &resp[..line_end];
    let Some(n_str) = head.strip_prefix(b"OK ") else {
        return resp.to_vec();
    };
    let Ok(n) = std::str::from_utf8(n_str).unwrap_or("").parse::<usize>() else {
        return resp.to_vec();
    };
    let body_start = line_end + 2;
    let body_end = body_start + n;
    // trailer is the "\r\n" that follows the YAML body; if the captured
    // response is shorter than expected (shouldn't happen for well-formed
    // captures), fall back to returning it unchanged.
    if resp.len() < body_end + 2 {
        return resp.to_vec();
    }
    let body = &resp[body_start..body_end];
    let trailer = &resp[body_end..body_end + 2];

    let masked_body = mask_yaml_body(body, mode);

    let mut out = Vec::with_capacity(masked_body.len() + trailer.len() + 16);
    out.extend_from_slice(b"OK ");
    out.extend_from_slice(masked_body.len().to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(&masked_body);
    out.extend_from_slice(trailer);
    out
}

fn mask_yaml_body(body: &[u8], mode: MaskMode) -> Vec<u8> {
    let server_stats = find(body, SERVER_STATS_MARKER).is_some();
    let mut out = Vec::with_capacity(body.len());
    let mut rest = body;
    while let Some(nl) = rest.iter().position(|&b| b == b'\n') {
        out.extend_from_slice(&mask_line(&rest[..nl], server_stats, mode));
        out.push(b'\n');
        rest = &rest[nl + 1..];
    }
    if !rest.is_empty() {
        out.extend_from_slice(&mask_line(rest, server_stats, mode));
    }
    out
}

fn mask_line(line: &[u8], server_stats: bool, mode: MaskMode) -> Vec<u8> {
    if let Some(colon) = line.iter().position(|&b| b == b':') {
        let key = &line[..colon];
        if let Ok(key_str) = std::str::from_utf8(key)
            && (MASKED_KEYS.contains(&key_str)
                || (server_stats && SERVER_STATS_ONLY_KEYS.contains(&key_str))
                || (mode.binlog && BINLOG_MASKED_KEYS.contains(&key_str)))
        {
            let mut out = Vec::with_capacity(key.len() + 2 + MASKED_VALUE.len());
            out.extend_from_slice(key);
            out.extend_from_slice(b": ");
            out.extend_from_slice(MASKED_VALUE.as_bytes());
            return out;
        }
    }
    line.to_vec()
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok_response(body: &str) -> Vec<u8> {
        let mut resp = format!("OK {}\r\n", body.len()).into_bytes();
        resp.extend_from_slice(body.as_bytes());
        resp.extend_from_slice(b"\r\n");
        resp
    }

    #[test]
    fn non_ok_responses_pass_through_unchanged() {
        let resp = b"DELETED\r\n".to_vec();
        assert_eq!(mask_response(&resp), resp);
        let resp = b"RESERVED 1 5\r\nhello\r\n".to_vec();
        assert_eq!(mask_response(&resp), resp);
    }

    #[test]
    fn masks_stats_fields_and_recomputes_length() {
        let body = "---\ncurrent-jobs-ready: 0\npid: 12345\nversion: \"1.13\"\nrusage-utime: 0.000000\nrusage-stime: 0.000000\nuptime: 42\ntotal-connections: 1\nid: abcdef0123456789\nhostname: \"myhost\"\nos: \"Darwin\"\nplatform: \"arm64\"\n";
        let resp = ok_response(body);
        let masked = mask_response(&resp);
        let masked_str = String::from_utf8(masked.clone()).expect("utf8");
        assert!(masked_str.contains("pid: <masked>"));
        assert!(masked_str.contains("version: <masked>"));
        assert!(masked_str.contains("rusage-utime: <masked>"));
        assert!(masked_str.contains("rusage-stime: <masked>"));
        assert!(masked_str.contains("uptime: <masked>"));
        assert!(masked_str.contains("id: <masked>"));
        assert!(masked_str.contains("hostname: <masked>"));
        assert!(masked_str.contains("os: <masked>"));
        assert!(masked_str.contains("platform: <masked>"));
        assert!(masked_str.contains("current-jobs-ready: 0"));

        // The declared length in the OK line must match the masked body length.
        let line_end = masked.iter().position(|&b| b == b'\r').expect("crlf");
        let declared: usize = std::str::from_utf8(&masked[3..line_end])
            .expect("utf8")
            .parse()
            .expect("number");
        let body_start = line_end + 2;
        let actual_body_len = masked.len() - body_start - 2; // minus trailing \r\n
        assert_eq!(declared, actual_body_len);
    }

    #[test]
    fn masks_stats_job_time_fields() {
        let body = "---\nid: 1\ntube: \"default\"\nstate: reserved\npri: 0\nage: 7\ndelay: 0\nttr: 60\ntime-left: 55\nfile: 0\nreserves: 1\ntimeouts: 0\nreleases: 0\nburies: 0\nkicks: 0\n";
        let resp = ok_response(body);
        let masked = String::from_utf8(mask_response(&resp)).expect("utf8");
        assert!(masked.contains("age: <masked>"));
        assert!(masked.contains("time-left: <masked>"));
        assert!(masked.contains("state: reserved"));
        // The job id is deterministic and must still be compared.
        assert!(masked.contains("---\nid: 1\n"));
    }

    #[test]
    fn masks_stats_tube_pause_time_left() {
        let body = "---\nname: \"default\"\npause: 5\npause-time-left: 3\n";
        let resp = ok_response(body);
        let masked = String::from_utf8(mask_response(&resp)).expect("utf8");
        assert!(masked.contains("pause-time-left: <masked>"));
        assert!(masked.contains("pause: 5"));
    }

    #[test]
    fn masking_makes_differing_volatile_values_compare_equal() {
        let body_a = "---\npid: 111\nuptime: 1\n";
        let body_b = "---\npid: 222\nuptime: 99\n";
        assert_eq!(
            mask_response(&ok_response(body_a)),
            mask_response(&ok_response(body_b))
        );
    }

    const BINLOG: MaskMode = MaskMode { binlog: true };

    #[test]
    fn binlog_fields_are_compared_without_binlog_mode() {
        let body = "---\nbinlog-oldest-index: 1\nbinlog-current-index: 2\nbinlog-records-migrated: 3\nbinlog-records-written: 4\nbinlog-max-size: 10485760\ntotal-connections: 1\n";
        let resp = ok_response(body);
        assert_eq!(mask_response(&resp), resp);
        let job = ok_response("---\nid: 1\nfile: 3\nreserves: 0\n");
        assert_eq!(mask_response(&job), job);
    }

    #[test]
    fn binlog_mode_masks_layout_fields_in_stats() {
        let body = "---\ncurrent-jobs-ready: 1\nbinlog-oldest-index: 1\nbinlog-current-index: 2\nbinlog-records-migrated: 3\nbinlog-records-written: 4\nbinlog-max-size: 10485760\ntotal-connections: 1\nid: abc\n";
        let masked =
            String::from_utf8(mask_response_with(&ok_response(body), BINLOG)).expect("utf8");
        assert!(masked.contains("binlog-oldest-index: <masked>\n"));
        assert!(masked.contains("binlog-current-index: <masked>\n"));
        assert!(masked.contains("binlog-records-migrated: <masked>\n"));
        assert!(masked.contains("binlog-records-written: 4\n"));
        assert!(masked.contains("binlog-max-size: 10485760\n"));
        assert!(masked.contains("current-jobs-ready: 1\n"));
        assert!(masked.contains("id: <masked>\n"));
    }

    #[test]
    fn binlog_mode_masks_file_in_stats_job_only() {
        let body = "---\nid: 7\ntube: \"default\"\nstate: ready\nfile: 3\nreserves: 2\n";
        let masked =
            String::from_utf8(mask_response_with(&ok_response(body), BINLOG)).expect("utf8");
        assert!(masked.contains("file: <masked>\n"));
        assert!(masked.contains("---\nid: 7\n"));
        assert!(masked.contains("reserves: 2\n"));
        // A differing file number compares equal only in binlog mode.
        let other = ok_response(&body.replace("file: 3", "file: 9"));
        assert_eq!(
            mask_response_with(&ok_response(body), BINLOG),
            mask_response_with(&other, BINLOG)
        );
        assert_ne!(mask_response(&ok_response(body)), mask_response(&other));
    }

    #[test]
    fn binlog_mode_keeps_records_written_compared() {
        let a = ok_response("---\nbinlog-records-written: 4\ntotal-connections: 1\n");
        let b = ok_response("---\nbinlog-records-written: 5\ntotal-connections: 1\n");
        assert_ne!(
            mask_response_with(&a, BINLOG),
            mask_response_with(&b, BINLOG)
        );
    }

    #[test]
    fn list_tubes_yaml_list_is_untouched() {
        let body = "---\n- default\n- foo\n";
        let resp = ok_response(body);
        assert_eq!(mask_response(&resp), resp);
    }
}
