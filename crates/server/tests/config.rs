//! The configuration file as the server binary uses it (P2-T4):
//! `--check-config`, invalid combinations, precedence, logging format.

#![allow(clippy::unwrap_used)]

mod common;

use std::path::Path;

use common::p2::{Certs, ConfigServer, run};

const TOKEN: &str = "check-config-secret-token";

fn write(dir: &Path, text: &str) -> String {
    let path = dir.join("config.toml");
    std::fs::write(&path, text).unwrap();
    path.display().to_string()
}

fn good_config() -> String {
    format!(
        r#"
[[listener]]
addr = "127.0.0.1:0"

[[listener]]
addr = "127.0.0.1:0"
tls = true
auth = "token"

[[listener]]
addr = "127.0.0.1:0"
tls = true
auth = "mtls"

[tls]
cert = "server.pem"
key = "server.key"
client_ca = "ca.pem"

[auth]
tokens = ["{TOKEN}"]

[http]
addr = "127.0.0.1:0"
"#
    )
}

#[test]
fn check_config_accepts_a_valid_file() {
    let dir = tempfile::tempdir().unwrap();
    Certs::generate(dir.path());
    let path = write(dir.path(), &good_config());
    let (status, out, err) = run(&["--config", &path, "--check-config"]);
    assert_eq!(status.code(), Some(0), "{err}");
    assert!(out.starts_with("configuration OK"), "{out}");
    assert!(out.contains("(tls, auth token)"), "{out}");
    assert!(out.contains("auth: 1 token(s)"), "{out}");
    assert!(!out.contains(TOKEN) && !err.contains(TOKEN));
}

#[test]
fn check_config_without_a_file_checks_the_flags() {
    let (status, out, _) = run(&["--check-config", "-l", "127.0.0.1", "-p", "0"]);
    assert_eq!(status.code(), Some(0));
    assert!(out.contains("command line only"), "{out}");
}

#[test]
fn check_config_rejects_invalid_files() {
    let dir = tempfile::tempdir().unwrap();
    Certs::generate(dir.path());
    let cases: [(&str, &str); 5] = [
        ("unknown key", "[server]\nmax_jobsize = 5\n"),
        (
            "token auth on plaintext",
            &format!(
                "[[listener]]\naddr = \"127.0.0.1:0\"\nauth = \"token\"\n[auth]\ntokens = [\"{TOKEN}\"]\n"
            ),
        ),
        (
            "missing certificate file",
            "[[listener]]\naddr = \"127.0.0.1:0\"\ntls = true\n[tls]\ncert = \"nope.pem\"\nkey = \"server.key\"\n",
        ),
        (
            "not a certificate",
            "[[listener]]\naddr = \"127.0.0.1:0\"\ntls = true\n[tls]\ncert = \"config.toml\"\nkey = \"server.key\"\n",
        ),
        (
            "key does not match",
            "[[listener]]\naddr = \"127.0.0.1:0\"\ntls = true\n[tls]\ncert = \"ca.pem\"\nkey = \"server.key\"\n",
        ),
    ];
    for (what, text) in cases {
        let path = write(dir.path(), text);
        let (status, out, err) = run(&["--config", &path, "--check-config"]);
        assert_eq!(status.code(), Some(1), "{what}: {out} {err}");
        assert!(err.contains("invalid configuration"), "{what}: {err}");
        assert!(!err.contains(TOKEN), "{what}: {err}");
        // Starting the server fails the same way.
        let (status, _, err) = run(&["--config", &path]);
        assert_eq!(status.code(), Some(1), "{what}: {err}");
        assert!(!err.contains(TOKEN), "{what}: {err}");
    }
}

#[test]
fn listen_flags_cannot_be_combined_with_listeners() {
    let dir = tempfile::tempdir().unwrap();
    let path = write(dir.path(), "[[listener]]\naddr = \"127.0.0.1:0\"\n");
    for flags in [&["-l", "127.0.0.1"][..], &["-p", "11300"]] {
        let mut args = vec!["--config", path.as_str()];
        args.extend_from_slice(flags);
        let (status, _, err) = run(&args);
        assert_eq!(status.code(), Some(1), "{flags:?}: {err}");
        args.push("--check-config");
        let (status, _, err) = run(&args);
        assert_eq!(status.code(), Some(1), "{flags:?}: {err}");
    }
}

#[test]
fn missing_config_file_is_a_startup_error() {
    let (status, _, err) = run(&["--config", "/nonexistent/beanstalkd-rs.toml"]);
    assert_eq!(status.code(), Some(1));
    assert!(err.contains("cannot read"), "{err}");
}

#[test]
fn command_line_overrides_the_file() {
    let (server, _) = ConfigServer::start(
        "[[listener]]\naddr = \"127.0.0.1:{port0}\"\n[server]\nmax_job_size = 10\n",
        1,
        &["-z", "20"],
    );
    let mut c = server.plain(0);
    assert_eq!(c.stat("stats", "max-job-size"), "20");
    assert_eq!(c.put(&[b'x'; 20]), "INSERTED 1");
    assert_eq!(c.put(&[b'x'; 21]), "JOB_TOO_BIG");
}

#[test]
fn json_log_format() {
    let (mut server, _) = ConfigServer::start(
        "[[listener]]\naddr = \"127.0.0.1:{port0}\"\n[log]\nlevel = \"info\"\nformat = \"json\"\n",
        1,
        &[],
    );
    let status = server.stop(nix::sys::signal::Signal::SIGTERM);
    assert!(status.success());
    let log = server.stderr();
    let lines: Vec<&str> = log.lines().collect();
    assert!(lines.len() >= 2, "{log}");
    for line in lines {
        let v: serde_json::Value =
            serde_json::from_str(line).unwrap_or_else(|e| panic!("not JSON ({e}): {line}"));
        assert!(v["level"].is_string() && v["fields"].is_object(), "{line}");
    }
    assert!(log.contains("beanstalkd-rs listening"), "{log}");
}
