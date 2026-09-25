//! Masking of volatile fields in `OK <n>\r\n<yaml>\r\n` responses (stats,
//! stats-job, stats-tube) so that differential comparisons ignore fields
//! that legitimately vary between two separate server processes/runs.
//!
//! See docs/DESIGN.md section 8.

/// YAML keys whose values are masked before comparison. The three formats
/// (stats, stats-job, stats-tube) never share a key name, so a single flat
/// list is safe to apply to any `OK` response body.
const MASKED_KEYS: &[&str] = &[
    // stats (server-wide)
    "pid",
    "version",
    "rusage-utime",
    "rusage-stime",
    "uptime",
    "id",
    "hostname",
    "os",
    "platform",
    // stats-job
    "age",
    "time-left",
    // stats-tube
    "pause-time-left",
];

const MASKED_VALUE: &str = "<masked>";

/// Mask volatile fields in a full response (as captured by the harness: the
/// status line plus any trailing body). Only `OK <n>\r\n...` responses are
/// touched; anything else (RESERVED/FOUND job bodies, plain word replies,
/// list-tubes YAML lists, ...) is returned unchanged.
pub fn mask_response(resp: &[u8]) -> Vec<u8> {
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

    let masked_body = mask_yaml_body(body);

    let mut out = Vec::with_capacity(masked_body.len() + trailer.len() + 16);
    out.extend_from_slice(b"OK ");
    out.extend_from_slice(masked_body.len().to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(&masked_body);
    out.extend_from_slice(trailer);
    out
}

fn mask_yaml_body(body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(body.len());
    let mut rest = body;
    while let Some(nl) = rest.iter().position(|&b| b == b'\n') {
        out.extend_from_slice(&mask_line(&rest[..nl]));
        out.push(b'\n');
        rest = &rest[nl + 1..];
    }
    if !rest.is_empty() {
        out.extend_from_slice(&mask_line(rest));
    }
    out
}

fn mask_line(line: &[u8]) -> Vec<u8> {
    if let Some(colon) = line.iter().position(|&b| b == b':') {
        let key = &line[..colon];
        if let Ok(key_str) = std::str::from_utf8(key)
            && MASKED_KEYS.contains(&key_str)
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
        let body = "---\ncurrent-jobs-ready: 0\npid: 12345\nversion: \"1.13\"\nrusage-utime: 0.000000\nrusage-stime: 0.000000\nuptime: 42\nid: abcdef0123456789\nhostname: \"myhost\"\nos: \"Darwin\"\nplatform: \"arm64\"\n";
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

    #[test]
    fn list_tubes_yaml_list_is_untouched() {
        let body = "---\n- default\n- foo\n";
        let resp = ok_response(body);
        assert_eq!(mask_response(&resp), resp);
    }
}
