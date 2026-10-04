//! Command-line interface, mirroring the reference's `-b`, `-f`, `-F`,
//! `-l`, `-p`, `-s`, `-u`, `-z`, `-V`, `-v`, `-h` flags (see
//! `util.c: optparse` / `usage` and the defaults in `serv.c`).
//!
//! Like `optparse`, a repeated flag overrides the earlier occurrence, and
//! `-f` / `-F` interact in argument order: `-f MS` sets the fsync rate and
//! turns fsync on, `-F` turns it off (keeping the rate), so the last of the
//! two wins (`-f 10 -F` never fsyncs, `-F -f 10` fsyncs every 10 ms).
//!
//! Options that the reference does not have are long-only (`--config`,
//! `--check-config`, `--cluster-init`, `--threads`, and `--version` as an
//! alias of `-v`) so they never shadow a reference short flag (including the
//! removed `-c` and `-n`).
//!
//! `beanstalkd-rs cluster <command>` is the one subcommand (the operator's
//! membership tool, `cluster_cli`). It is a real clap subcommand, so the flat
//! flags stay exactly as above: the parent has no positional arguments, and
//! `cluster` can only appear where a positional would be an error anyway.
//! Flat flags given before it would be silently ignored, so they are refused.

use std::net::IpAddr;
use std::path::PathBuf;
use std::time::Duration;

use clap::error::ErrorKind;
use clap::parser::ValueSource;
use clap::{ArgAction, ArgMatches, CommandFactory, FromArgMatches, Parser, Subcommand};

use bstk_engine::DEFAULT_BINLOG_MAX_SIZE;
use bstk_proto::{DEFAULT_MAX_JOB_SIZE, MAX_JOB_SIZE_LIMIT};
use bstk_store::SyncPolicy;

use crate::cluster_cli::ClusterArgs;

/// `DEFAULT_FSYNC_MS` in dat.h: the fsync rate when `-b` is given without
/// `-f` or `-F` (`serv.c` starts with `wantsync = 1`).
pub const DEFAULT_FSYNC_MS: u64 = 50;

/// beanstalkd-rs: a byte-for-byte compatible reimplementation of beanstalkd.
#[derive(Parser, Debug)]
#[command(
    name = "beanstalkd-rs",
    disable_version_flag = true,
    disable_help_subcommand = true,
    args_override_self = true
)]
pub struct Cli {
    /// Write-ahead log directory
    #[arg(short = 'b', value_name = "DIR")]
    pub binlog_dir: Option<PathBuf>,

    /// fsync at most once every MS milliseconds (default is 50ms); use -f0
    /// for "always fsync"
    #[arg(
        short = 'f',
        value_name = "MS",
        value_parser = parse_size_arg,
        allow_hyphen_values = true,
    )]
    pub fsync_ms: Option<u64>,

    /// Never fsync
    #[arg(short = 'F', action = ArgAction::SetTrue)]
    pub never_fsync: bool,

    /// Set the size of each write-ahead log file (rounded up to a multiple
    /// of 4096 bytes)
    #[arg(
        short = 's',
        value_name = "BYTES",
        default_value_t = DEFAULT_BINLOG_MAX_SIZE,
        value_parser = parse_size_arg,
        allow_hyphen_values = true,
    )]
    pub binlog_file_size: u64,

    /// Become user and group (not supported by beanstalkd-rs)
    #[arg(short = 'u', value_name = "USER")]
    pub user: Option<String>,

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
    #[arg(short = 'v', long = "version", action = ArgAction::SetTrue)]
    pub version: bool,

    /// Read settings from a TOML configuration file (command-line flags
    /// override its values)
    #[arg(long = "config", value_name = "PATH")]
    pub config: Option<PathBuf>,

    /// Validate the configuration, print a summary and exit
    #[arg(long = "check-config", action = ArgAction::SetTrue)]
    pub check_config: bool,

    /// Cluster mode: bootstrap a new cluster from [[cluster.peer]] (first
    /// start only, on every initial node, with an empty cluster.data_dir)
    #[arg(long = "cluster-init", action = ArgAction::SetTrue)]
    pub cluster_init: bool,

    /// Tokio worker-thread count (default: 1 standalone, 2 with a TLS
    /// listener, -b or `[cluster]`; see docs/DESIGN.md §3)
    #[arg(
        long = "threads",
        value_name = "N",
        value_parser = clap::value_parser!(u16).range(1..=256),
    )]
    pub threads: Option<u16>,

    /// An operator command instead of a server (see `beanstalkd-rs cluster
    /// --help`)
    #[command(subcommand)]
    pub sub: Option<Sub>,

    /// Resolved fsync policy (from `-f` / `-F` in argument order); only
    /// meaningful with `-b`.
    #[arg(skip = SyncPolicy::Interval(Duration::from_millis(DEFAULT_FSYNC_MS)))]
    pub sync: SyncPolicy,

    /// Which defaulted options were given on the command line (so that a
    /// configuration file value applies only when they were not).
    #[arg(skip)]
    pub given: Given,
}

#[derive(Subcommand, Debug)]
pub enum Sub {
    /// Inspect and change the membership of a running cluster
    Cluster(ClusterArgs),
}

/// Whether an option with a default value (or a derived value, like
/// `sync`) was given on the command line. Filled in by `from_matches`;
/// all `false` for a `Cli` built any other way.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Given {
    /// `-l`
    pub listen_addr: bool,
    /// `-p`
    pub port: bool,
    /// `-z`
    pub max_job_size: bool,
    /// `-s`
    pub binlog_file_size: bool,
    /// `-f` or `-F`
    pub sync: bool,
}

impl Cli {
    /// Parses the process arguments (exiting on error, like
    /// `Parser::parse`) and resolves the fsync policy.
    pub fn from_env() -> Cli {
        let matches = Cli::command().get_matches();
        Cli::from_matches(&matches).unwrap_or_else(|e| e.exit())
    }

    pub(crate) fn from_matches(matches: &ArgMatches) -> Result<Cli, clap::Error> {
        let mut cli = Cli::from_arg_matches(matches)?;
        // Only the server's own flags: the command's global options are
        // propagated into these matches as well.
        if matches.subcommand().is_some()
            && Cli::command().get_arguments().any(|a| {
                matches.value_source(a.get_id().as_str()) == Some(ValueSource::CommandLine)
            })
        {
            return Err(Cli::command().error(
                ErrorKind::ArgumentConflict,
                "server flags cannot be combined with the `cluster` command; give the command's \
                 own options after it (see `beanstalkd-rs cluster --help`)",
            ));
        }
        // Defaults (e.g. `-F`'s implicit `false`) have indices too; only
        // occurrences on the command line count.
        let last = |id: &str| {
            (matches.value_source(id) == Some(ValueSource::CommandLine))
                .then(|| matches.indices_of(id).and_then(|mut i| i.next_back()))
                .flatten()
        };
        let never = match (last("never_fsync"), last("fsync_ms")) {
            (Some(big_f), Some(f)) => big_f > f,
            (Some(_), None) => true,
            (None, _) => false,
        };
        cli.sync = sync_policy(cli.fsync_ms.unwrap_or(DEFAULT_FSYNC_MS), never);
        let given = |id: &str| matches.value_source(id) == Some(ValueSource::CommandLine);
        cli.given = Given {
            listen_addr: given("listen_addr"),
            port: given("port"),
            max_job_size: given("max_job_size"),
            binlog_file_size: given("binlog_file_size"),
            sync: given("never_fsync") || given("fsync_ms"),
        };
        Ok(cli)
    }

    #[cfg(test)]
    pub(crate) fn try_parse_args(args: &[&str]) -> Result<Cli, clap::Error> {
        let matches = Cli::command().try_get_matches_from(args)?;
        Cli::from_matches(&matches)
    }
}

/// `wal.syncrate = (int64) ms * 1000000` with `wantsync = !never`.
/// `walsync` fsyncs when `now >= lastsync + syncrate`, so a rate of 0 (or
/// one that wrapped negative) means fsync on every write.
fn sync_policy(ms: u64, never: bool) -> SyncPolicy {
    if never {
        return SyncPolicy::Never;
    }
    // Two's-complement reinterpretation, as the C cast does.
    let rate = i64::from_ne_bytes(ms.to_ne_bytes()).wrapping_mul(1_000_000);
    match u64::try_from(rate) {
        Ok(nanos) if nanos > 0 => SyncPolicy::Interval(Duration::from_nanos(nanos)),
        _ => SyncPolicy::Always,
    }
}

/// `-s` and `-f` arguments: the reference's `parse_size_t`.
fn parse_size_arg(s: &str) -> Result<u64, String> {
    scan_size_t(s).ok_or_else(|| format!("invalid size: {s}"))
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

/// Maps the `-V` count to a tracing level like the reference's `verbose++`.
/// The server uses `config::LogLevel::from_verbosity`; this stays as the
/// reference the config tests check it against.
#[cfg(test)]
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
    fn binlog_defaults_mirror_serv_c() {
        let cli = parse(&[]).expect("no args");
        assert_eq!(cli.binlog_dir, None);
        assert_eq!(cli.binlog_file_size, 10 << 20);
        assert_eq!(cli.sync, SyncPolicy::Interval(Duration::from_millis(50)));
        let cli = parse(&["-b", "/x", "-s", "1000"]).expect("-b -s");
        assert_eq!(cli.binlog_dir, Some(PathBuf::from("/x")));
        assert_eq!(cli.binlog_file_size, 1000);
    }

    #[test]
    fn fsync_flags_follow_argument_order() {
        let sync = |args: &[&str]| parse(args).expect("parses").sync;
        let ms = |n| SyncPolicy::Interval(Duration::from_millis(n));
        assert_eq!(sync(&["-f0"]), SyncPolicy::Always);
        assert_eq!(sync(&["-f", "0"]), SyncPolicy::Always);
        assert_eq!(sync(&["-f", "10"]), ms(10));
        assert_eq!(sync(&["-F"]), SyncPolicy::Never);
        assert_eq!(sync(&["-f", "10", "-F"]), SyncPolicy::Never);
        assert_eq!(sync(&["-F", "-f", "10"]), ms(10));
        assert_eq!(sync(&["-F", "-f", "10", "-F"]), SyncPolicy::Never);
        assert_eq!(sync(&["-f", "10", "-F", "-f", "20"]), ms(20));
        assert_eq!(sync(&["-f", "10", "-f", "20"]), ms(20));
        // -F keeps the rate: fsync is off, whatever -f said before.
        assert_eq!(sync(&["-f0", "-F"]), SyncPolicy::Never);
        // (int64) SIZE_MAX * 1000000 wraps negative: always fsync.
        assert_eq!(sync(&["-f", "-1"]), SyncPolicy::Always);
        assert!(parse(&["-f", "x"]).is_err());
        assert!(parse(&["-s", "10k"]).is_err());
    }

    #[test]
    fn size_flag_is_parse_size_t() {
        assert_eq!(
            parse(&["-s", "-1"]).expect("-s -1").binlog_file_size,
            u64::MAX
        );
        assert_eq!(parse(&["-s", " 4097"]).expect("-s").binlog_file_size, 4097);
    }

    fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
        let mut argv = vec!["beanstalkd-rs"];
        argv.extend_from_slice(args);
        Cli::try_parse_args(&argv)
    }

    #[test]
    fn given_tracks_command_line_occurrences() {
        let cli = parse(&[]).expect("no args");
        assert_eq!(cli.given, Given::default());
        assert_eq!(cli.config, None);
        assert!(!cli.check_config);
        let cli = parse(&["-l", "127.0.0.1", "-p", "1", "-z", "9", "-s", "5", "-F"]).expect("all");
        assert_eq!(
            cli.given,
            Given {
                listen_addr: true,
                port: true,
                max_job_size: true,
                binlog_file_size: true,
                sync: true,
            }
        );
        assert!(parse(&["-f", "3"]).expect("-f").given.sync);
        // Giving the default value explicitly still counts as given.
        assert!(parse(&["-p", "11300"]).expect("-p").given.port);
    }

    #[test]
    fn new_options_are_long_only() {
        let cli = parse(&["--config", "/etc/b.toml", "--check-config"]).expect("long flags");
        assert_eq!(cli.config, Some(PathBuf::from("/etc/b.toml")));
        assert!(cli.check_config);
        let cmd = Cli::command();
        for id in ["config", "check_config", "cluster_init", "threads"] {
            let arg = cmd
                .get_arguments()
                .find(|a| a.get_id() == id)
                .expect("argument exists");
            assert_eq!(arg.get_short(), None, "{id} must have no short flag");
        }
        let mut shorts: Vec<char> = cmd.get_arguments().filter_map(|a| a.get_short()).collect();
        shorts.sort_unstable();
        assert_eq!(shorts, ['F', 'V', 'b', 'f', 'l', 'p', 's', 'u', 'v', 'z']);
    }

    #[test]
    fn version_has_reference_short_flag_and_long_alias() {
        assert!(parse(&["-v"]).expect("-v").version);
        assert!(parse(&["--version"]).expect("--version").version);
        assert!(!parse(&[]).expect("no args").version);
    }

    #[test]
    fn threads_flag_is_long_only_and_range_checked() {
        assert_eq!(parse(&[]).expect("no args").threads, None);
        assert_eq!(
            parse(&["--threads", "4"]).expect("--threads 4").threads,
            Some(4)
        );
        assert!(parse(&["--threads", "0"]).is_err());
        assert!(parse(&["--threads", "257"]).is_err());
        assert!(parse(&["--threads", "256"]).is_ok());
        assert!(parse(&["--threads", "1"]).is_ok());
    }

    #[test]
    fn max_job_size_accepts_a_negative_value_argument() {
        let cli = Cli::try_parse_from(["beanstalkd-rs", "-z", "-1"]).expect("-z -1 parses");
        assert_eq!(cli.max_job_size, MAX_JOB_SIZE_LIMIT);
    }

    fn cluster(args: &[&str]) -> Result<ClusterArgs, clap::Error> {
        let mut argv = vec!["cluster"];
        argv.extend_from_slice(args);
        match parse(&argv)?.sub {
            Some(Sub::Cluster(a)) => Ok(a),
            None => panic!("no subcommand"),
        }
    }

    #[test]
    fn cluster_subcommand_leaves_the_flat_flags_alone() {
        let cli = parse(&["-l", "127.0.0.1", "-p", "1", "-F", "-f", "10", "-V"]).expect("flat");
        assert!(cli.sub.is_none());
        assert_eq!(cli.port, 1);
        assert_eq!(cli.sync, SyncPolicy::Interval(Duration::from_millis(10)));
        // The parent has no positional arguments and no `help` subcommand:
        // a stray word is still a usage error.
        assert!(parse(&["status"]).is_err());
        assert!(parse(&["help"]).is_err());
        assert!(parse(&["clusters"]).is_err());
        // Flags that are not the command's are refused after it...
        assert!(cluster(&["status", "-p", "1"]).is_err());
        assert!(cluster(&["status", "-F"]).is_err());
        // ...and before it, where they would be silently dropped.
        for flat in [
            &["-p", "1"][..],
            &["-V"],
            &["--config", "x"],
            &["--check-config"],
            &["-f", "0"],
        ] {
            let mut argv = flat.to_vec();
            argv.extend(["cluster", "status", "--node", "h:1"]);
            let e = parse(&argv).expect_err("flat flags before the command");
            assert_eq!(
                e.kind(),
                clap::error::ErrorKind::ArgumentConflict,
                "{flat:?}"
            );
        }
        // The command's own --config is not the server's.
        let a = cluster(&["status", "--config", "n.toml"]).expect("config");
        assert_eq!(a.common.config, Some(PathBuf::from("n.toml")));
    }

    #[test]
    fn cluster_commands_parse() {
        use crate::cluster_cli::ClusterCmd;
        let a = cluster(&[
            "status",
            "--node",
            "h:1",
            "--node",
            "[::1]:2",
            "--insecure-plaintext",
        ])
        .expect("status");
        assert_eq!(a.command, ClusterCmd::Status);
        assert_eq!(a.common.node, ["h:1", "[::1]:2"]);
        assert!(a.common.insecure_plaintext && !a.common.json);
        assert_eq!(a.common.timeout, Duration::from_secs(30));

        // Options may precede the command as well as follow it.
        let a = cluster(&[
            "--node",
            "h:1",
            "--json",
            "--timeout",
            "5s",
            "add",
            "4",
            "h:4",
        ])
        .expect("add");
        assert_eq!(
            a.command,
            ClusterCmd::Add {
                id: 4,
                addr: "h:4".into()
            }
        );
        assert!(a.common.json);
        assert_eq!(a.common.node, ["h:1"]);
        assert_eq!(a.common.timeout, Duration::from_secs(5));

        let a = cluster(&[
            "promote", "4", "--force", "--cert", "c", "--key", "k", "--ca", "a",
        ])
        .expect("promote");
        assert_eq!(a.command, ClusterCmd::Promote { id: 4, force: true });
        assert_eq!(a.common.cert, Some(PathBuf::from("c")));
        let a = cluster(&["remove", "2"]).expect("remove");
        assert_eq!(
            a.command,
            ClusterCmd::Remove {
                id: 2,
                force: false
            }
        );
        let a = cluster(&["set-addr", "2", "h:9", "--force"]).expect("set-addr");
        assert_eq!(
            a.command,
            ClusterCmd::SetAddr {
                id: 2,
                addr: "h:9".into(),
                force: true
            }
        );
    }

    #[test]
    fn cluster_usage_errors() {
        // No command: usage text and clap's usage status (2).
        let e = parse(&["cluster"]).expect_err("no command");
        assert_eq!(e.exit_code(), 2);
        for bad in [
            &["frobnicate"][..],
            &["add", "4"],
            &["add", "0", "h:4"],
            &["add", "65536", "h:4"],
            &["add", "x", "h:4"],
            &["add", "4", "nohost"],
            &["add", "4", "h:0"],
            &["promote"],
            &["remove", "1", "2"],
            &["status", "--node", "nohost"],
            &["status", "--timeout", "0"],
            &["status", "--timeout", "soon"],
            &["status", "--force"],
            &["add", "4", "h:4", "--nope"],
        ] {
            let e = cluster(bad).expect_err("usage error");
            assert_eq!(e.exit_code(), 2, "{bad:?}");
        }
    }
}
