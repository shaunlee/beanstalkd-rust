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
    )]
    pub max_job_size: u32,

    /// Increase verbosity (repeatable)
    #[arg(short = 'V', action = ArgAction::Count)]
    pub verbose: u8,

    /// Show version information and exit
    #[arg(short = 'v', action = ArgAction::SetTrue)]
    pub version: bool,
}

/// Parses `-z`'s argument the way the reference's `parse_size_t` does (an
/// unsigned decimal integer, no trailing garbage), then clamps to
/// `JOB_DATA_SIZE_LIMIT_MAX` with a warning, matching `optparse`'s `-z` case.
fn parse_max_job_size(s: &str) -> Result<u32, String> {
    let value: u64 = s.parse().map_err(|_| format!("invalid size: {s}"))?;
    if value > u64::from(MAX_JOB_SIZE_LIMIT) {
        eprintln!("beanstalkd-rs: maximum job size was set to {MAX_JOB_SIZE_LIMIT}");
        Ok(MAX_JOB_SIZE_LIMIT)
    } else {
        // `value <= MAX_JOB_SIZE_LIMIT` (a u32), so this cast never truncates.
        Ok(value as u32)
    }
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
