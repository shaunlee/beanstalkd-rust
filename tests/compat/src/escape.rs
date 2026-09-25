//! Human-readable escaping of raw protocol bytes, used for diagnostics.

/// Render bytes the way they'd be written as a `.bt` string literal:
/// `\r`, `\n`, `\\`, `\"` are escaped, other non-printable bytes become
/// `\xNN`, everything else is passed through as-is.
pub fn escape_bytes(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len());
    for &b in data {
        match b {
            b'\r' => out.push_str("\\r"),
            b'\n' => out.push_str("\\n"),
            b'\\' => out.push_str("\\\\"),
            b'"' => out.push_str("\\\""),
            0x20..=0x7e => out.push(b as char),
            _ => out.push_str(&format!("\\x{b:02x}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escapes_control_and_special_bytes() {
        assert_eq!(escape_bytes(b"a\r\nb\"c\\d"), "a\\r\\nb\\\"c\\\\d");
        assert_eq!(escape_bytes(&[0x00, 0x1b, 0xff]), "\\x00\\x1b\\xff");
        assert_eq!(escape_bytes(b"hello"), "hello");
    }
}
