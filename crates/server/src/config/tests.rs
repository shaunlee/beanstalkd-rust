use std::net::{IpAddr, Ipv4Addr};

use bstk_engine::DEFAULT_BINLOG_MAX_SIZE;
use bstk_proto::DEFAULT_MAX_JOB_SIZE;

use super::*;
use crate::cli;

const TOKEN_A: &str = "s3cret-token-AAAA";
const TOKEN_B: &str = "an0ther-token-BBBB";

fn cli(args: &[&str]) -> Cli {
    let mut argv = vec!["beanstalkd-rs"];
    argv.extend_from_slice(args);
    Cli::try_parse_args(&argv).expect("valid command line")
}

fn parse(text: &str) -> Result<FileConfig, ConfigError> {
    FileConfig::parse(text, Path::new("/etc/beanstalkd/b.toml"))
}

fn resolve_str(args: &[&str], text: &str) -> Result<ResolvedConfig, ConfigError> {
    resolve(&cli(args), Some(parse(text)?))
}

fn resolved(args: &[&str], text: &str) -> ResolvedConfig {
    resolve_str(args, text).expect("valid configuration")
}

/// The error message for an invalid configuration.
fn error(args: &[&str], text: &str) -> String {
    resolve_str(args, text)
        .expect_err("invalid configuration")
        .to_string()
}

fn addr(s: &str) -> SocketAddr {
    s.parse().expect("socket address")
}

const TLS: &str = "[tls]\ncert = \"server.pem\"\nkey = \"server.key\"\n";

const FULL: &str = r#"
[server]
max_job_size = 1000
max_pending_connections = 64

[[listener]]
addr = "0.0.0.0:11300"

[[listener]]
addr = "0.0.0.0:11301"
tls = true
auth = "token"

[[listener]]
addr = "[::1]:11302"
tls = true
auth = "mtls"

[tls]
cert = "server.pem"
key = "/abs/server.key"
client_ca = "ca/ca.pem"

[auth]
tokens = ["s3cret-token-AAAA", "an0ther-token-BBBB", "s3cret-token-AAAA"]
timeout = "2500ms"

[binlog]
dir = "wal"
fsync = "1s"
file_size = 4096

[http]
addr = "127.0.0.1:9180"
max_tube_series = 50
snapshot_min_interval = "3s"

[log]
level = "debug"
format = "json"
"#;

#[test]
fn full_example_parses() {
    let c = resolved(&[], FULL);
    assert_eq!(c.source, Some(PathBuf::from("/etc/beanstalkd/b.toml")));
    assert_eq!(
        c.listeners,
        vec![
            Listener {
                addr: addr("0.0.0.0:11300"),
                tls: false,
                auth: AuthMode::None
            },
            Listener {
                addr: addr("0.0.0.0:11301"),
                tls: true,
                auth: AuthMode::Token
            },
            Listener {
                addr: addr("[::1]:11302"),
                tls: true,
                auth: AuthMode::Mtls
            },
        ]
    );
    assert_eq!(
        c.tls,
        Some(TlsFiles {
            cert: PathBuf::from("/etc/beanstalkd/server.pem"),
            key: PathBuf::from("/abs/server.key"),
            client_ca: Some(PathBuf::from("/etc/beanstalkd/ca/ca.pem")),
        })
    );
    // Deduplicated, in order.
    let tokens: Vec<&[u8]> = c.tokens.iter().collect();
    assert_eq!(tokens, vec![TOKEN_A.as_bytes(), TOKEN_B.as_bytes()]);
    assert_eq!(c.max_job_size, 1000);
    assert_eq!(c.max_pending_connections, 64);
    assert_eq!(c.auth_timeout, Duration::from_millis(2500));
    assert!(c.warnings.is_empty());
    assert_eq!(
        c.binlog,
        BinlogSettings {
            dir: Some(PathBuf::from("/etc/beanstalkd/wal")),
            file_size: 4096,
            sync: SyncPolicy::Interval(Duration::from_secs(1)),
        }
    );
    assert_eq!(
        c.binlog.wal_options(),
        Some(WalOptions {
            dir: PathBuf::from("/etc/beanstalkd/wal"),
            file_size: 4096,
            sync: SyncPolicy::Interval(Duration::from_secs(1)),
        })
    );
    assert_eq!(
        c.http,
        Some(HttpSettings {
            addr: addr("127.0.0.1:9180"),
            max_tube_series: 50,
            snapshot_min_interval: Duration::from_secs(3),
        })
    );
    assert_eq!(
        c.log,
        LogSettings {
            level: LogLevel::Debug,
            format: LogFormat::Json
        }
    );
}

#[test]
fn example_file_in_docs_is_valid() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/beanstalkd-rs.example.toml");
    let text = std::fs::read_to_string(&path).expect("example file exists");
    let file = FileConfig::parse(&text, &path).expect("example parses");
    // The example names a tokens_file, which is not shipped; everything
    // else must validate once the token source is filled in.
    let mut file = file;
    file.tokens_file = Some(TokensFile {
        path: PathBuf::from("tokens.txt"),
        contents: format!("{TOKEN_B}\n"),
        mode_warning: None,
    });
    let c = resolve(&cli(&[]), Some(file)).expect("example validates");
    assert!(!c.listeners.is_empty());
}

#[test]
fn no_file_is_todays_command_line() {
    let c = resolve(&cli(&[]), None).expect("defaults");
    assert_eq!(
        c,
        ResolvedConfig {
            source: None,
            listeners: vec![Listener {
                addr: addr("0.0.0.0:11300"),
                tls: false,
                auth: AuthMode::None
            }],
            tls: None,
            tokens: Tokens::default(),
            auth_timeout: DEFAULT_AUTH_TIMEOUT,
            max_job_size: DEFAULT_MAX_JOB_SIZE,
            max_pending_connections: DEFAULT_MAX_PENDING_CONNECTIONS,
            binlog: BinlogSettings {
                dir: None,
                file_size: DEFAULT_BINLOG_MAX_SIZE,
                sync: SyncPolicy::Interval(Duration::from_millis(cli::DEFAULT_FSYNC_MS)),
            },
            http: None,
            log: LogSettings {
                level: LogLevel::Warn,
                format: LogFormat::Text
            },
            warnings: Vec::new(),
        }
    );
    assert_eq!(DEFAULT_AUTH_TIMEOUT, Duration::from_secs(10));
    assert_eq!(DEFAULT_MAX_PENDING_CONNECTIONS, 1024);
    assert_eq!(c.binlog.wal_options(), None);

    // Every flag passes through unchanged, quirks included.
    let args = [
        "-l",
        "127.0.0.1",
        "-p",
        "1234",
        "-z",
        "-1",
        "-b",
        "rel/wal",
        "-s",
        "-1",
        "-f",
        "10",
        "-F",
        "-VV",
    ];
    let cl = cli(&args);
    let c = resolve(&cl, None).expect("flags");
    assert_eq!(
        c.listeners,
        vec![Listener {
            addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 1234),
            tls: false,
            auth: AuthMode::None
        }]
    );
    assert_eq!(c.max_job_size, MAX_JOB_SIZE_LIMIT);
    assert_eq!(c.binlog.dir, Some(PathBuf::from("rel/wal")));
    assert_eq!(c.binlog.file_size, u64::MAX);
    assert_eq!(c.binlog.sync, SyncPolicy::Never);
    assert_eq!(c.log.level, LogLevel::Debug);
    assert_eq!(c.max_job_size, cl.max_job_size);
    assert_eq!(c.binlog.sync, cl.sync);

    // An empty file changes nothing either (except naming the source).
    let mut c = resolved(&args, "");
    c.source = None;
    assert_eq!(c, resolve(&cl, None).expect("flags"));
}

#[test]
fn verbosity_matches_cli_tracing_level() {
    for v in 0..6 {
        assert_eq!(
            LogLevel::from_verbosity(v).to_tracing(),
            cli::tracing_level(v)
        );
    }
    assert!(LogLevel::Error < LogLevel::Warn);
    assert!(LogLevel::Warn < LogLevel::Info);
    assert!(LogLevel::Info < LogLevel::Debug);
    assert!(LogLevel::Debug < LogLevel::Trace);
}

#[test]
fn unknown_keys_are_rejected_everywhere() {
    let cases = [
        ("bogus = 1\n", 1),
        ("[bogus]\n", 1),
        ("[server]\nmax_job_size = 1\nbogus = 1\n", 3),
        ("[server]\nmax_pending_connection = 1\n", 2),
        ("[auth]\ntimeouts = \"1s\"\n", 2),
        ("[http]\nsnapshot_interval = \"1s\"\n", 2),
        ("[[listener]]\naddr = \"127.0.0.1:1\"\nbogus = 1\n", 3),
        ("[tls]\nbogus = \"x\"\n", 2),
        ("[auth]\nbogus = 1\n", 2),
        ("[binlog]\nbogus = 1\n", 2),
        ("[http]\nbogus = 1\n", 2),
        ("[log]\nbogus = 1\n", 2),
        // A singular [listener] table instead of [[listener]].
        ("[listener]\naddr = \"127.0.0.1:1\"\n", 1),
    ];
    for (text, line) in cases {
        let e = parse(text).expect_err(text);
        let ConfigError::Parse {
            line: got,
            ref message,
            ref path,
            ..
        } = e
        else {
            panic!("{text:?}: expected a parse error, got {e:?}");
        };
        assert_eq!(got, line, "{text:?}: {e}");
        assert_eq!(path, Path::new("/etc/beanstalkd/b.toml"));
        if text.contains("bogus") {
            assert!(message.contains("bogus"), "{text:?}: {message}");
        }
    }
}

#[test]
fn bad_enum_values_and_types_are_rejected() {
    for text in [
        "[[listener]]\naddr = \"127.0.0.1:1\"\nauth = \"password\"\n",
        "[[listener]]\naddr = \"127.0.0.1:1\"\ntls = \"yes\"\n",
        "[[listener]]\ntls = false\n",
        "[log]\nlevel = \"verbose\"\n",
        "[log]\nformat = \"xml\"\n",
        "[server]\nmax_job_size = \"10\"\n",
        "[binlog]\nfsync = 50\n",
        "[auth]\ntimeout = 10\n",
        "[server]\nmax_pending_connections = \"10\"\n",
        "[http]\naddr = \"127.0.0.1:1\"\nsnapshot_min_interval = 1\n",
        "this is not toml",
    ] {
        assert!(
            matches!(parse(text), Err(ConfigError::Parse { .. })),
            "{text:?}"
        );
    }
}

#[test]
fn parse_errors_do_not_echo_tokens() {
    for text in [
        format!("[auth]\ntokens = \"{TOKEN_A}\"\n"),
        format!("[auth]\ntokens = [\"{TOKEN_A}\" \"x\"]\n"),
        format!("[auth]\ntokens = [\"{TOKEN_A}\"]\nbogus = 1\n"),
    ] {
        let e = parse(&text).expect_err(&text);
        let shown = format!("{e} {e:?}");
        assert!(!shown.contains(TOKEN_A), "{shown}");
    }
}

#[test]
fn tls_listener_needs_cert_and_key() {
    let l = "[[listener]]\naddr = \"127.0.0.1:1\"\ntls = true\n";
    let e = error(&[], l);
    assert!(e.contains("listener[0]") && e.contains("tls.cert"), "{e}");
    let e = error(&[], &format!("{l}[tls]\ncert = \"c.pem\"\n"));
    assert!(e.contains("tls.key"), "{e}");
    let e = error(&[], &format!("{l}[tls]\nkey = \"k.pem\"\n"));
    assert!(e.contains("tls.cert"), "{e}");
    let e = error(&[], "[tls]\nclient_ca = \"ca.pem\"\n");
    assert!(e.contains("tls.client_ca"), "{e}");
    assert!(resolved(&[], &format!("{l}{TLS}")).listeners[0].tls);
    // [tls] without any TLS listener is accepted (and unused).
    assert!(resolved(&[], TLS).tls.is_some());
}

#[test]
fn token_auth_requires_tls() {
    let text = format!(
        "[[listener]]\naddr = \"127.0.0.1:1\"\nauth = \"token\"\n{TLS}[auth]\ntokens = [\"{TOKEN_A}\"]\n"
    );
    let e = error(&[], &text);
    assert!(
        e.contains("listener[0]") && e.contains("auth = \"token\"") && e.contains("tls = true"),
        "{e}"
    );
    assert!(e.contains("clear"), "{e}");
}

#[test]
fn mtls_requires_tls_and_client_ca() {
    let mtls = "[[listener]]\naddr = \"127.0.0.1:1\"\nauth = \"mtls\"\n";
    let with_ca = format!("{TLS}client_ca = \"ca.pem\"\n");
    let e = error(&[], &format!("{mtls}{with_ca}"));
    assert!(e.contains("auth = \"mtls\" requires tls = true"), "{e}");
    let mtls_tls = format!("{mtls}tls = true\n");
    let e = error(&[], &format!("{mtls_tls}{TLS}"));
    assert!(e.contains("tls.client_ca"), "{e}");
    let c = resolved(&[], &format!("{mtls_tls}{with_ca}"));
    assert_eq!(c.listeners[0].auth, AuthMode::Mtls);
    assert_eq!(
        c.tls.and_then(|t| t.client_ca),
        Some(PathBuf::from("/etc/beanstalkd/ca.pem"))
    );
}

#[test]
fn token_auth_requires_tokens() {
    let base = format!("[[listener]]\naddr = \"127.0.0.1:1\"\ntls = true\nauth = \"token\"\n{TLS}");
    for extra in ["", "[auth]\n", "[auth]\ntokens = []\n"] {
        let e = error(&[], &format!("{base}{extra}"));
        assert!(
            e.contains("at least one token") && e.contains("auth.tokens"),
            "{extra:?}: {e}"
        );
    }
    let c = resolved(&[], &format!("{base}[auth]\ntokens = [\"{TOKEN_A}\"]\n"));
    assert_eq!(c.tokens.len(), 1);
}

#[test]
fn invalid_tokens_are_rejected_without_echoing_them() {
    let long = "x".repeat(MAX_TOKEN_LEN + 1);
    let bad = [
        ("", "empty"),
        ("has space", "whitespace"),
        ("tab\there", "whitespace"),
        ("cr\rlf", "whitespace"),
        ("nul\0x", "control"),
        ("del\x7fx", "control"),
        ("nbsp\u{a0}x", "whitespace"),
        (long.as_str(), "218 bytes"),
    ];
    for (i, (token, why)) in bad.iter().enumerate() {
        let text = format!("[auth]\ntokens = [\"{TOKEN_A}\", {}]\n", toml_string(token));
        let e = error(&[], &text);
        assert!(e.contains("auth.tokens[1]") && e.contains(why), "{i}: {e}");
        assert!(!e.contains(TOKEN_A), "{e}");
        if token.len() > 3 {
            assert!(!e.contains(token), "{e}");
        }
    }
    // The longest token that fits a protocol line is accepted.
    let max = "y".repeat(MAX_TOKEN_LEN);
    assert_eq!(MAX_TOKEN_LEN, 217);
    assert_eq!(("auth ".len() + max.len() + 2), bstk_proto::LINE_BUF_SIZE);
    let c = resolved(&[], &format!("[auth]\ntokens = [\"{max}\"]\n"));
    assert_eq!(c.tokens.iter().next(), Some(max.as_bytes()));
    // Non-ASCII tokens without whitespace are fine.
    resolved(&[], "[auth]\ntokens = [\"jeton-\u{e9}t\u{e9}\"]\n");
}

/// A TOML basic string for `s` (escaping what TOML requires).
fn toml_string(s: &str) -> String {
    let mut out = String::from("\"");
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c if c.is_control() => out.push_str(&format!("\\u{:04X}", u32::from(c))),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

#[test]
fn duplicate_and_overlapping_listeners_are_rejected() {
    let two =
        |a: &str, b: &str| format!("[[listener]]\naddr = \"{a}\"\n[[listener]]\naddr = \"{b}\"\n");
    for (a, b) in [
        ("127.0.0.1:11300", "127.0.0.1:11300"),
        ("0.0.0.0:11300", "127.0.0.1:11300"),
        ("[::1]:11300", "[::]:11300"),
        ("[::]:11300", "0.0.0.0:11300"),
    ] {
        let e = error(&[], &two(a, b));
        assert!(
            e.contains("listener[1].addr") && e.contains("listener[0].addr"),
            "{a} {b}: {e}"
        );
    }
    for (a, b) in [
        ("127.0.0.1:11300", "127.0.0.1:11301"),
        ("127.0.0.1:11300", "127.0.0.2:11300"),
        // Ephemeral ports never collide.
        ("127.0.0.1:0", "127.0.0.1:0"),
    ] {
        assert_eq!(resolved(&[], &two(a, b)).listeners.len(), 2, "{a} {b}");
    }
}

#[test]
fn addresses_must_be_ip_port() {
    for bad in [
        "localhost:11300",
        "127.0.0.1",
        "11300",
        "::1:11300",
        "127.0.0.1:99999",
        "",
    ] {
        let e = error(&[], &format!("[[listener]]\naddr = \"{bad}\"\n"));
        assert!(
            e.contains("listener[0].addr") && e.contains("IP:port"),
            "{bad}: {e}"
        );
        let e = error(&[], &format!("[http]\naddr = \"{bad}\"\n"));
        assert!(
            e.contains("http.addr") && e.contains("IP:port"),
            "{bad}: {e}"
        );
    }
}

#[test]
fn http_settings() {
    let c = resolved(&[], "[http]\naddr = \"127.0.0.1:9180\"\n");
    assert_eq!(
        c.http,
        Some(HttpSettings {
            addr: addr("127.0.0.1:9180"),
            max_tube_series: DEFAULT_MAX_TUBE_SERIES,
            snapshot_min_interval: DEFAULT_SNAPSHOT_MIN_INTERVAL,
        })
    );
    let interval = |v: &str| {
        resolved(
            &[],
            &format!("[http]\naddr = \"127.0.0.1:9180\"\nsnapshot_min_interval = \"{v}\"\n"),
        )
        .http
        .map(|h| h.snapshot_min_interval)
    };
    assert_eq!(interval("250ms"), Some(Duration::from_millis(250)));
    assert_eq!(interval("2s"), Some(Duration::from_secs(2)));
    // Zero disables the snapshot cache.
    assert_eq!(interval("0s"), Some(Duration::ZERO));
    for bad in ["", "1", "1m", "-1s", "1.5s", "never"] {
        let e = error(
            &[],
            &format!("[http]\naddr = \"127.0.0.1:9180\"\nsnapshot_min_interval = \"{bad}\"\n"),
        );
        assert!(e.contains("http.snapshot_min_interval"), "{bad:?}: {e}");
    }
    let e = error(&[], "[http]\nmax_tube_series = 5\n");
    assert!(e.contains("http.addr is required"), "{e}");
    let e = error(
        &[],
        "[http]\naddr = \"127.0.0.1:9180\"\nmax_tube_series = -1\n",
    );
    assert!(e.contains("http.max_tube_series"), "{e}");
    assert_eq!(
        resolved(
            &[],
            "[http]\naddr = \"127.0.0.1:9180\"\nmax_tube_series = 0\n"
        )
        .http
        .map(|h| h.max_tube_series),
        Some(0)
    );
}

#[test]
fn http_addr_must_differ_from_listeners() {
    // Against the default -l / -p listener.
    let e = error(&[], "[http]\naddr = \"0.0.0.0:11300\"\n");
    assert!(e.contains("http.addr") && e.contains("listener[0]"), "{e}");
    let e = error(&["-p", "9180"], "[http]\naddr = \"127.0.0.1:9180\"\n");
    assert!(e.contains("http.addr"), "{e}");
    // Against file listeners.
    let e = error(
        &[],
        "[[listener]]\naddr = \"127.0.0.1:1\"\n[[listener]]\naddr = \"127.0.0.1:2\"\n\
         [http]\naddr = \"127.0.0.1:2\"\n",
    );
    assert!(e.contains("listener[1]"), "{e}");
    resolved(&[], "[http]\naddr = \"127.0.0.1:11301\"\n");
}

#[test]
fn fsync_values() {
    let sync = |args: &[&str], v: &str| {
        resolved(args, &format!("[binlog]\nfsync = \"{v}\"\n"))
            .binlog
            .sync
    };
    let ms = |n| SyncPolicy::Interval(Duration::from_millis(n));
    assert_eq!(sync(&[], "always"), SyncPolicy::Always);
    assert_eq!(sync(&[], "never"), SyncPolicy::Never);
    assert_eq!(sync(&[], "50ms"), ms(50));
    assert_eq!(sync(&[], "1ms"), ms(1));
    assert_eq!(sync(&[], "2s"), ms(2000));
    assert_eq!(sync(&[], "0ms"), SyncPolicy::Always);
    assert_eq!(sync(&[], "0s"), SyncPolicy::Always);
    assert_eq!(sync(&[], "9223372036854ms"), ms(9_223_372_036_854));
    for bad in [
        "",
        "50",
        "ms",
        "s",
        "-5ms",
        "+5ms",
        "5 ms",
        " 5ms",
        "5ms ",
        "5m",
        "5h",
        "1.5s",
        "5MS",
        "Always",
        "9223372036855ms",
        "18446744073709551616s",
    ] {
        let e = error(&[], &format!("[binlog]\nfsync = \"{bad}\"\n"));
        assert!(e.contains("binlog.fsync"), "{bad:?}: {e}");
    }
}

#[test]
fn auth_timeout_values() {
    let timeout = |v: &str| resolved(&[], &format!("[auth]\ntimeout = \"{v}\"\n")).auth_timeout;
    assert_eq!(timeout("1ms"), Duration::from_millis(1));
    assert_eq!(timeout("500ms"), Duration::from_millis(500));
    assert_eq!(timeout("30s"), Duration::from_secs(30));
    assert_eq!(resolved(&[], "").auth_timeout, DEFAULT_AUTH_TIMEOUT);
    for bad in [
        "0s",
        "0ms",
        "",
        "10",
        "always",
        "never",
        "-1s",
        "1.5s",
        "5m",
        "9223372036855ms",
    ] {
        let e = error(&[], &format!("[auth]\ntimeout = \"{bad}\"\n"));
        assert!(e.contains("auth.timeout"), "{bad:?}: {e}");
    }
}

#[test]
fn max_pending_connections_values() {
    let max = |v: i64| {
        resolved(&[], &format!("[server]\nmax_pending_connections = {v}\n")).max_pending_connections
    };
    assert_eq!(max(1), 1);
    assert_eq!(max(100_000), 100_000);
    assert_eq!(
        resolved(&[], "").max_pending_connections,
        DEFAULT_MAX_PENDING_CONNECTIONS
    );
    for bad in [0, -1, i64::MIN] {
        let e = error(&[], &format!("[server]\nmax_pending_connections = {bad}\n"));
        assert!(e.contains("server.max_pending_connections"), "{bad}: {e}");
    }
}

#[test]
fn tokens_file_mode_warnings() {
    let p = Path::new("/etc/beanstalkd/tokens.txt");
    for ok in [0o600, 0o400, 0o700, 0o100_600, 0o4600] {
        assert_eq!(tokens_file_mode_warning(p, ok), None, "{ok:o}");
    }
    for loose in [0o644, 0o640, 0o604, 0o660, 0o666, 0o610, 0o601, 0o100_644] {
        let w = tokens_file_mode_warning(p, loose).expect("warning");
        assert!(
            w.contains("auth.tokens_file") && w.contains("tokens.txt") && w.contains("chmod 600"),
            "{w}"
        );
        assert!(w.contains(&format!("{:04o}", loose & 0o7777)), "{w}");
    }

    // Through `load`: a group/world-readable file warns but still works.
    let dir = tempfile::tempdir().expect("tempdir");
    let tokens = dir.path().join("tokens.txt");
    std::fs::write(&tokens, format!("{TOKEN_A}\n")).expect("write");
    let conf = dir.path().join("b.toml");
    std::fs::write(&conf, "[auth]\ntokens_file = \"tokens.txt\"\n").expect("write");
    let conf_arg = conf.to_str().expect("utf-8");
    let set_mode = |mode| {
        std::fs::set_permissions(&tokens, std::fs::Permissions::from_mode(mode)).expect("chmod")
    };
    set_mode(0o644);
    let c = load(&cli(&["--config", conf_arg])).expect("still valid");
    assert_eq!(c.tokens.len(), 1);
    assert_eq!(c.warnings.len(), 1, "{:?}", c.warnings);
    assert!(c.warnings[0].contains("0644"), "{:?}", c.warnings);
    assert!(!c.warnings[0].contains(TOKEN_A));

    let run = |args: &[&str]| {
        let (mut out, mut err) = (Vec::new(), Vec::new());
        let status = run_check(&cli(args), &mut out, &mut err);
        (
            status,
            String::from_utf8(out).expect("utf-8"),
            String::from_utf8(err).expect("utf-8"),
        )
    };
    let (status, out, err) = run(&["--check-config", "--config", conf_arg]);
    assert_eq!(status, 0);
    assert!(out.starts_with("configuration OK"), "{out}");
    assert!(
        err.starts_with("beanstalkd-rs: warning: auth.tokens_file") && err.contains("0644"),
        "{err}"
    );

    set_mode(0o600);
    let c = load(&cli(&["--config", conf_arg])).expect("valid");
    assert!(c.warnings.is_empty(), "{:?}", c.warnings);
    let (status, _, err) = run(&["--check-config", "--config", conf_arg]);
    assert_eq!(status, 0);
    assert_eq!(err, "");
}

#[test]
fn size_limits() {
    let e = error(
        &[],
        &format!(
            "[server]\nmax_job_size = {}\n",
            u64::from(MAX_JOB_SIZE_LIMIT) + 1
        ),
    );
    assert!(e.contains("server.max_job_size"), "{e}");
    let e = error(&[], "[server]\nmax_job_size = -1\n");
    assert!(e.contains("server.max_job_size"), "{e}");
    let c = resolved(
        &[],
        &format!("[server]\nmax_job_size = {MAX_JOB_SIZE_LIMIT}\n"),
    );
    assert_eq!(c.max_job_size, MAX_JOB_SIZE_LIMIT);
    assert_eq!(
        resolved(&[], "[server]\nmax_job_size = 0\n").max_job_size,
        0
    );

    let e = error(
        &[],
        &format!("[binlog]\nfile_size = {}\n", MAX_BINLOG_FILE_SIZE + 1),
    );
    assert!(e.contains("binlog.file_size"), "{e}");
    let e = error(&[], "[binlog]\nfile_size = -1\n");
    assert!(e.contains("binlog.file_size"), "{e}");
    let c = resolved(
        &[],
        &format!("[binlog]\nfile_size = {MAX_BINLOG_FILE_SIZE}\n"),
    );
    assert_eq!(c.binlog.file_size, MAX_BINLOG_FILE_SIZE);
}

#[test]
fn file_values_are_validated_even_when_overridden() {
    let e = error(&["-z", "5"], "[server]\nmax_job_size = -1\n");
    assert!(e.contains("server.max_job_size"), "{e}");
    let e = error(&["-F"], "[binlog]\nfsync = \"soon\"\n");
    assert!(e.contains("binlog.fsync"), "{e}");
}

#[test]
fn command_line_overrides_the_file() {
    let file = "[server]\nmax_job_size = 1000\n[binlog]\ndir = \"wal\"\nfsync = \"always\"\n\
                file_size = 8192\n[log]\nlevel = \"info\"\n";

    // File values apply when the flags are absent.
    let c = resolved(&[], file);
    assert_eq!(c.max_job_size, 1000);
    assert_eq!(c.binlog.dir, Some(PathBuf::from("/etc/beanstalkd/wal")));
    assert_eq!(c.binlog.sync, SyncPolicy::Always);
    assert_eq!(c.binlog.file_size, 8192);
    assert_eq!(c.log.level, LogLevel::Info);

    // -z (even when equal to the default, and with -z's clamping).
    assert_eq!(resolved(&["-z", "7"], file).max_job_size, 7);
    assert_eq!(resolved(&["-z", "65535"], file).max_job_size, 65535);
    assert_eq!(
        resolved(&["-z", "-1"], file).max_job_size,
        MAX_JOB_SIZE_LIMIT
    );
    // -b (relative to the working directory, not the file).
    assert_eq!(
        resolved(&["-b", "other"], file).binlog.dir,
        Some(PathBuf::from("other"))
    );
    // -s
    assert_eq!(resolved(&["-s", "4097"], file).binlog.file_size, 4097);
    assert_eq!(
        resolved(&["-s", "10485760"], file).binlog.file_size,
        10_485_760
    );
    // -f / -F, with their argument-order interplay.
    let sync = |args: &[&str]| resolved(args, file).binlog.sync;
    assert_eq!(
        sync(&["-f", "10"]),
        SyncPolicy::Interval(Duration::from_millis(10))
    );
    assert_eq!(sync(&["-F"]), SyncPolicy::Never);
    assert_eq!(
        sync(&["-F", "-f", "20"]),
        SyncPolicy::Interval(Duration::from_millis(20))
    );
    assert_eq!(
        sync(&["-f", "50"]),
        SyncPolicy::Interval(Duration::from_millis(50))
    );
    let never = "[binlog]\nfsync = \"never\"\n";
    assert_eq!(resolved(&["-f0"], never).binlog.sync, SyncPolicy::Always);
    // -V only raises the level.
    let level = |args: &[&str], text: &str| resolved(args, text).log.level;
    assert_eq!(level(&["-V"], file), LogLevel::Info);
    assert_eq!(level(&["-VV"], file), LogLevel::Debug);
    assert_eq!(level(&["-VVV"], file), LogLevel::Trace);
    let trace = "[log]\nlevel = \"trace\"\n";
    assert_eq!(level(&["-V"], trace), LogLevel::Trace);
    // Without -V the file may lower the level below today's default.
    let quiet = "[log]\nlevel = \"error\"\n";
    assert_eq!(level(&[], quiet), LogLevel::Error);
    assert_eq!(level(&["-V"], quiet), LogLevel::Info);
    assert_eq!(level(&[], "[log]\nformat = \"json\"\n"), LogLevel::Warn);
}

#[test]
fn listen_flags_conflict_with_listener_entries() {
    let file = "[[listener]]\naddr = \"127.0.0.1:11300\"\n";
    for args in [
        &["-l", "127.0.0.1"][..],
        &["-p", "11301"],
        &["-l", "0.0.0.0", "-p", "11300"],
    ] {
        let e = error(args, file);
        assert!(
            e.contains("-l / -p") && e.contains("[[listener]]") && e.contains("b.toml"),
            "{args:?}: {e}"
        );
    }
    // Without [[listener]], -l / -p still define the listener.
    let c = resolved(
        &["-l", "127.0.0.1", "-p", "7"],
        "[server]\nmax_job_size = 1\n",
    );
    assert_eq!(
        c.listeners,
        vec![Listener {
            addr: addr("127.0.0.1:7"),
            tls: false,
            auth: AuthMode::None
        }]
    );
    // An explicitly empty listener array counts as none.
    assert_eq!(
        resolved(&["-p", "7"], "listener = []\n").listeners[0]
            .addr
            .port(),
        7
    );
}

#[test]
fn load_resolves_paths_and_reads_tokens_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let conf_dir = dir.path().join("conf");
    std::fs::create_dir_all(conf_dir.join("secrets")).expect("mkdir");
    std::fs::write(
        conf_dir.join("secrets/tokens.txt"),
        format!(
            "# comment line\n\n{TOKEN_A}\r\n   \n  {TOKEN_B}  \n\t# indented comment\n{TOKEN_A}\n"
        ),
    )
    .expect("write tokens");
    let conf = conf_dir.join("b.toml");
    std::fs::write(
        &conf,
        "[[listener]]\naddr = \"127.0.0.1:0\"\ntls = true\nauth = \"token\"\n\
         [tls]\ncert = \"server.pem\"\nkey = \"keys/server.key\"\n\
         [auth]\ntokens = [\"inline-token\"]\ntokens_file = \"secrets/tokens.txt\"\n",
    )
    .expect("write config");

    let conf_arg = conf.to_str().expect("utf-8 path");
    let c = load(&cli(&["--config", conf_arg])).expect("loads");
    assert_eq!(c.source, Some(conf.clone()));
    let tokens: Vec<&[u8]> = c.tokens.iter().collect();
    assert_eq!(
        tokens,
        vec![
            b"inline-token".as_slice(),
            TOKEN_A.as_bytes(),
            TOKEN_B.as_bytes()
        ]
    );
    assert_eq!(
        c.tls,
        Some(TlsFiles {
            cert: conf_dir.join("server.pem"),
            key: conf_dir.join("keys/server.key"),
            client_ca: None,
        })
    );

    // A bad line is reported by file and line number, without its value.
    std::fs::write(
        conf_dir.join("secrets/tokens.txt"),
        format!("{TOKEN_A}\n\nbad token-value\n"),
    )
    .expect("rewrite tokens");
    let e = load(&cli(&["--config", conf_arg]))
        .expect_err("bad token")
        .to_string();
    assert!(
        e.contains("auth.tokens_file") && e.contains("tokens.txt") && e.contains("line 3"),
        "{e}"
    );
    assert!(!e.contains("token-value") && !e.contains(TOKEN_A), "{e}");

    // A missing tokens file (or config file) is a read error naming it.
    std::fs::remove_file(conf_dir.join("secrets/tokens.txt")).expect("rm");
    let e = load(&cli(&["--config", conf_arg])).expect_err("missing tokens file");
    assert!(
        matches!(e, ConfigError::Read { ref path, .. } if path.ends_with("secrets/tokens.txt"))
    );
    let missing = dir.path().join("nope.toml");
    let e = load(&cli(&["--config", missing.to_str().expect("utf-8")])).expect_err("missing");
    assert!(e.to_string().contains("nope.toml"), "{e}");
}

#[test]
fn relative_config_path_keeps_paths_relative() {
    let file = FileConfig::parse(TLS, Path::new("b.toml")).expect("parses");
    let c = resolve(&cli(&[]), Some(file)).expect("valid");
    assert_eq!(c.tls.map(|t| t.cert), Some(PathBuf::from("server.pem")));
    let file = FileConfig::parse(TLS, Path::new("conf/b.toml")).expect("parses");
    let c = resolve(&cli(&[]), Some(file)).expect("valid");
    assert_eq!(
        c.tls.map(|t| t.cert),
        Some(PathBuf::from("conf/server.pem"))
    );
}

#[test]
fn tokens_file_must_be_loaded() {
    // `parse` alone does not read tokens_file; resolving must not silently
    // drop it.
    let e = error(&[], "[auth]\ntokens_file = \"t.txt\"\n");
    assert!(e.contains("auth.tokens_file"), "{e}");
}

#[test]
fn debug_output_redacts_tokens() {
    let c = resolved(&[], FULL);
    let shown = format!("{c:?} {c:#?}");
    assert!(
        !shown.contains(TOKEN_A) && !shown.contains(TOKEN_B),
        "{shown}"
    );
    assert!(shown.contains("<2 redacted>"), "{shown}");

    let file = parse(FULL).expect("parses");
    let shown = format!("{file:?}");
    assert!(
        !shown.contains(TOKEN_A) && !shown.contains(TOKEN_B),
        "{shown}"
    );

    let tf = TokensFile {
        path: PathBuf::from("t"),
        contents: TOKEN_A.to_owned(),
        mode_warning: None,
    };
    assert!(!format!("{tf:?}").contains(TOKEN_A));
}

#[test]
fn check_config_summary() {
    let c = resolved(&[], FULL);
    let s = summary(&c);
    assert!(!s.contains(TOKEN_A) && !s.contains(TOKEN_B), "{s}");
    for expected in [
        "configuration OK: /etc/beanstalkd/b.toml",
        "listener 0.0.0.0:11300 (plaintext, auth none)",
        "listener 0.0.0.0:11301 (tls, auth token)",
        "listener [::1]:11302 (tls, auth mtls)",
        "client_ca /etc/beanstalkd/ca/ca.pem",
        "auth: 2 token(s), timeout 2.5s",
        "max pending connections: 64",
        "max job size: 1000",
        "binlog: /etc/beanstalkd/wal (file size 4096, fsync every 1s)",
        "http: 127.0.0.1:9180 (max tube series 50, snapshot min interval 3s)",
        "log: level debug, format json",
    ] {
        assert!(s.contains(expected), "missing {expected:?} in\n{s}");
    }

    let (s, warnings) = check(&cli(&[])).expect("defaults are valid");
    assert!(warnings.is_empty());
    assert_eq!(
        s,
        "configuration OK: command line only\n\
         listener 0.0.0.0:11300 (plaintext, auth none)\n\
         max job size: 65535\n\
         binlog: disabled\n\
         http: disabled\n\
         log: level warn, format text\n"
    );
}

#[test]
fn run_check_exit_status_and_output() {
    let dir = tempfile::tempdir().expect("tempdir");
    let good = dir.path().join("good.toml");
    std::fs::write(&good, format!("[auth]\ntokens = [\"{TOKEN_A}\"]\n")).expect("write");
    let bad = dir.path().join("bad.toml");
    std::fs::write(
        &bad,
        "[[listener]]\naddr = \"127.0.0.1:1\"\nauth = \"token\"\n",
    )
    .expect("write");

    let run = |args: &[&str]| {
        let (mut out, mut err) = (Vec::new(), Vec::new());
        let status = run_check(&cli(args), &mut out, &mut err);
        (
            status,
            String::from_utf8(out).expect("utf-8"),
            String::from_utf8(err).expect("utf-8"),
        )
    };
    let good = good.to_str().expect("utf-8");
    let bad = bad.to_str().expect("utf-8");

    let (status, out, err) = run(&["--check-config", "--config", good]);
    assert_eq!(status, 0);
    assert!(
        out.starts_with("configuration OK: ") && out.contains("auth: 1 token(s), timeout 10s"),
        "{out}"
    );
    assert!(!out.contains(TOKEN_A), "{out}");
    assert_eq!(err, "");

    let (status, out, err) = run(&["--check-config", "--config", bad]);
    assert_eq!(status, EXIT_CONFIG);
    assert_ne!(status, 0);
    assert_eq!(out, "");
    assert!(
        err.contains("invalid configuration") && err.contains("listener[0]"),
        "{err}"
    );

    // -l / -p against [[listener]] is caught too.
    let (status, _, err) = run(&["--check-config", "--config", bad, "-p", "1"]);
    assert_eq!(status, EXIT_CONFIG);
    assert!(err.contains("-l / -p"), "{err}");

    // Without --config, the command line alone is checked.
    let (status, out, _) = run(&["--check-config", "-p", "1234"]);
    assert_eq!(status, 0);
    assert!(out.contains("listener 0.0.0.0:1234"), "{out}");
}
