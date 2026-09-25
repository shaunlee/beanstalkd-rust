//! Command-line interface, mirroring the reference's `-l`, `-p`, `-z`, `-V`,
//! `-v`, `-h` flags (see `util.c: optparse` / `usage`).

use std::net::IpAddr;

use clap::{ArgAction, Parser};

use bstk_proto::{DEFAULT_MAX_JOB_SIZE, MAX_JOB_SIZE_LIMIT};

/// beanstalkd-rs: a byte-for-byte compatible reimplementation of beanstalkd.
#[derive(Parser, Debug)]
#[command(name = "beanstalkd-rs", disable_version_flag = true)]
pub struct Cli {
    /// Listen on address
    #[arg(short = 'l', value_name = "ADDR", default_value = "0.0.0.0")]
    pub listen_addr: IpAddr,

    /// Listen on port
    #[arg(short = 'p', value_name = "PORT", default_value_t = 11300)]
    pub port: u16,

    /// Set the maximum job size in bytes (clamped to the reference's max)
    #[arg(
        short = 'z',
        value_name = "BYTES",
        default_value_t = DEFAULT_MAX_JOB_SIZE,
        value_parser = parse_max_job_size,
        allow_hyphen_values = true,
    )]
    pub max_job_size: u32,

    /// Increase verbosity (repeatable)
    #[arg(short = 'V', action = ArgAction::Count)]
    pub verbose: u8,

    /// Show version information and exit
    #[arg(short = 'v', action = ArgAction::SetTrue)]
    pub version: bool,
}

/// Parses `-z`'s argument the way the reference's `parse_size_t` does, then
/// clamps to `JOB_DATA_SIZE_LIMIT_MAX` with a warning, matching `optparse`'s
/// `-z` case.
fn parse_max_job_size(s: &str) -> Result<u32, String> {
    let value = scan_size_t(s).ok_or_else(|| format!("invalid size: {s}"))?;
    if value > u64::from(MAX_JOB_SIZE_LIMIT) {
        eprintln!("beanstalkd-rs: maximum job size was set to {MAX_JOB_SIZE_LIMIT}");
        Ok(MAX_JOB_SIZE_LIMIT)
    } else {
        // `value <= MAX_JOB_SIZE_LIMIT` (a u32), so this cast never truncates.
        Ok(value as u32)
    }
}

/// `sscanf(str, "%zu%c", ...) == 1` on a 64-bit host: `%zu` is `strtoull`
/// base 10 (leading whitespace skipped, optional sign, a negative value
/// wraps, overflow saturates to `u64::MAX`), and any character after the
/// digits matches `%c`, which the reference rejects.
fn scan_size_t(s: &str) -> Option<u64> {
    let rest = s.trim_start_matches([' ', '\t', '\n', '\x0b', '\x0c', '\r']);
    let (negative, digits) = match rest.as_bytes().first() {
        Some(b'-') => (true, &rest[1..]),
        Some(b'+') => (false, &rest[1..]),
        _ => (false, rest),
    };
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let magnitude = digits.bytes().try_fold(0u64, |acc, b| {
        acc.checked_mul(10)?.checked_add(u64::from(b - b'0'))
    });
    Some(match magnitude {
        // strtoull: out of range gives ULLONG_MAX, sign notwithstanding.
        None => u64::MAX,
        Some(v) if negative => v.wrapping_neg(),
        Some(v) => v,
    })
}

/// Maps `-V` repeat count to a tracing level, matching the reference's
/// `verbose++` behavior (more `-V` = more output).
pub fn tracing_level(verbose: u8) -> tracing::Level {
    match verbose {
        0 => tracing::Level::WARN,
        1 => tracing::Level::INFO,
        2 => tracing::Level::DEBUG,
        _ => tracing::Level::TRACE,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_job_size_follows_sscanf_zu() {
        assert_eq!(parse_max_job_size("10"), Ok(10));
        assert_eq!(parse_max_job_size("+5"), Ok(5));
        assert_eq!(parse_max_job_size(" \t7"), Ok(7));
        assert_eq!(parse_max_job_size("0"), Ok(0));
        // -1 wraps to SIZE_MAX, overflow saturates; both clamp to 1 GiB.
        assert_eq!(parse_max_job_size("-1"), Ok(MAX_JOB_SIZE_LIMIT));
        assert_eq!(
            parse_max_job_size("18446744073709551616"),
            Ok(MAX_JOB_SIZE_LIMIT)
        );
        assert_eq!(parse_max_job_size("-0"), Ok(0));
        for bad in ["", " ", "5x", "5 ", "0x10", "-", "+-1", "abc"] {
            assert!(
                parse_max_job_size(bad).is_err(),
                "{bad:?} should be rejected"
            );
        }
    }

    #[test]
    fn max_job_size_accepts_a_negative_value_argument() {
        let cli = Cli::try_parse_from(["beanstalkd-rs", "-z", "-1"]).expect("-z -1 parses");
        assert_eq!(cli.max_job_size, MAX_JOB_SIZE_LIMIT);
    }
}
