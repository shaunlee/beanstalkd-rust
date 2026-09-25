//! Token authentication on TLS listeners (P2-T4): nothing an
//! unauthenticated client sends reaches the engine, a wrong token closes
//! the connection, and tokens never appear in the logs.

#![allow(clippy::unwrap_used)]

mod common;

use std::time::Duration;

use common::p2::{Certs, ClientCert, ConfigServer, End, Proto, TlsStream, stat_of};

const GOOD: &str = "s3cr3t-Token-4b1d";
const OTHER_GOOD: &str = "second-valid-token";
const WRONG: &str = "wr0ng-Token-77aa";

fn config(level: &str) -> String {
    format!(
        r#"
[[listener]]
addr = "127.0.0.1:{{port0}}"
tls = true
auth = "token"

[tls]
cert = "server.pem"
key = "server.key"

[auth]
tokens = ["{GOOD}", "{OTHER_GOOD}"]

[log]
level = "{level}"
"#
    )
}

fn start() -> (ConfigServer, Certs) {
    ConfigServer::start(&config("warn"), 1, &[])
}

fn connect(server: &ConfigServer, certs: &Certs) -> Proto<TlsStream> {
    server.tls(0, certs.client_config(ClientCert::None))
}

fn authed(server: &ConfigServer, certs: &Certs) -> Proto<TlsStream> {
    let mut c = connect(server, certs);
    assert_eq!(c.cmd(&format!("auth {GOOD}")), "AUTHENTICATED");
    c
}

/// Sends `data` and expects exactly `reply` and then the end of the
/// connection.
fn expect_reply_then_close(c: &mut Proto<TlsStream>, data: &[u8], reply: &str) {
    c.send(data);
    let (bytes, end) = c.read_to_end(Duration::from_secs(5));
    assert_eq!(String::from_utf8_lossy(&bytes), reply);
    assert_eq!(end, End::Closed);
}

#[test]
fn correct_token_authenticates_and_commands_work() {
    let (server, certs) = start();
    let mut c = authed(&server, &certs);
    assert_eq!(c.put(b"job"), "INSERTED 1");
    let (h, body) = c.body_reply("reserve");
    assert_eq!((h.as_str(), body.as_slice()), ("RESERVED 1 3", &b"job"[..]));
    assert_eq!(c.cmd("delete 1"), "DELETED");

    // Any configured token works.
    let mut other = connect(&server, &certs);
    assert_eq!(other.cmd(&format!("auth {OTHER_GOOD}")), "AUTHENTICATED");
    assert_eq!(other.cmd("use t"), "USING t");
}

#[test]
fn auth_and_a_pipelined_command_are_both_served() {
    let (server, certs) = start();
    let mut c = connect(&server, &certs);
    c.send(format!("auth {GOOD}\r\nput 0 0 60 1\r\nx\r\n").as_bytes());
    assert_eq!(c.read_line(), "AUTHENTICATED\r\n");
    assert_eq!(c.read_line(), "INSERTED 1\r\n");
}

#[test]
fn wrong_token_gets_unauthorized_and_close() {
    let (server, certs) = start();
    let mut c = connect(&server, &certs);
    expect_reply_then_close(
        &mut c,
        format!("auth {WRONG}\r\n").as_bytes(),
        "UNAUTHORIZED\r\n",
    );
    // Prefixes, extensions and an empty token are all wrong.
    for bad in [&GOOD[..GOOD.len() - 1], &format!("{GOOD}x"), "", " "] {
        let mut c = connect(&server, &certs);
        expect_reply_then_close(
            &mut c,
            format!("auth {bad}\r\n").as_bytes(),
            "UNAUTHORIZED\r\n",
        );
    }
}

#[test]
fn pipelined_wrong_auth_and_stats_reach_nothing() {
    let (server, certs) = start();
    let mut c = connect(&server, &certs);
    expect_reply_then_close(
        &mut c,
        format!("auth {WRONG}\r\nstats\r\n").as_bytes(),
        "UNAUTHORIZED\r\n",
    );
    let mut ok = authed(&server, &certs);
    let stats = ok.yaml("stats");
    // Only this connection and its own `stats`.
    assert_eq!(stat_of(&stats, "cmd-stats"), "1");
    assert_eq!(stat_of(&stats, "current-connections"), "1");
    assert_eq!(stat_of(&stats, "total-connections"), "1");
}

#[test]
fn put_before_auth_consumes_no_job_id() {
    let (server, certs) = start();
    let mut c = connect(&server, &certs);
    expect_reply_then_close(&mut c, b"put 0 0 60 5\r\nhello\r\n", "UNAUTHORIZED\r\n");
    // A put header alone is refused at once, before any body arrives.
    let mut c = connect(&server, &certs);
    expect_reply_then_close(&mut c, b"put 0 0 60 100000\r\n", "UNAUTHORIZED\r\n");

    let mut ok = authed(&server, &certs);
    assert_eq!(ok.put(b"first"), "INSERTED 1");
    let stats = ok.yaml("stats");
    assert_eq!(stat_of(&stats, "cmd-put"), "1");
    assert_eq!(stat_of(&stats, "total-jobs"), "1");
    assert_eq!(stat_of(&stats, "current-producers"), "1");
}

#[test]
fn any_other_input_before_auth_is_refused() {
    let (server, certs) = start();
    for input in [
        &b"stats\r\n"[..],
        b"list-tubes\r\n",
        b"use foo\r\n",
        b"reserve-with-timeout 0\r\n",
        b"no-such-command\r\n",
        b"\r\n",
        b"put 0 0 60 x\r\n",
    ] {
        let mut c = connect(&server, &certs);
        expect_reply_then_close(&mut c, input, "UNAUTHORIZED\r\n");
    }
    // An overlong line is refused once it ends.
    let mut c = connect(&server, &certs);
    let mut long = vec![b'a'; 1000];
    long.extend_from_slice(b"\r\n");
    expect_reply_then_close(&mut c, &long, "UNAUTHORIZED\r\n");

    let mut ok = authed(&server, &certs);
    let stats = ok.yaml("stats");
    assert_eq!(stat_of(&stats, "cmd-stats"), "1");
    assert_eq!(stat_of(&stats, "cmd-use"), "0");
    assert_eq!(stat_of(&stats, "cmd-list-tubes"), "0");
    assert_eq!(stat_of(&stats, "cmd-reserve-with-timeout"), "0");
    assert_eq!(stat_of(&stats, "total-connections"), "1");
    assert_eq!(stat_of(&stats, "current-tubes"), "1");
}

#[test]
fn quit_before_auth_closes_silently() {
    let (server, certs) = start();
    let mut c = connect(&server, &certs);
    expect_reply_then_close(&mut c, b"quit\r\n", "");
    let mut ok = authed(&server, &certs);
    assert_eq!(ok.stat("stats", "total-connections"), "1");
}

#[test]
fn unauthenticated_connections_are_not_counted() {
    let (server, certs) = start();
    // Handshake done, never authenticated.
    let idle: Vec<_> = (0..5).map(|_| connect(&server, &certs)).collect();
    let mut ok = authed(&server, &certs);
    let stats = ok.yaml("stats");
    assert_eq!(stat_of(&stats, "current-connections"), "1");
    assert_eq!(stat_of(&stats, "total-connections"), "1");
    drop(idle);
    // Connections that close before authenticating send no Disconnect
    // either: the counters stay consistent.
    std::thread::sleep(Duration::from_millis(200));
    let stats = ok.yaml("stats");
    assert_eq!(stat_of(&stats, "current-connections"), "1");
    assert_eq!(stat_of(&stats, "total-connections"), "1");
}

#[test]
fn re_auth_after_authentication() {
    let (server, certs) = start();
    let mut c = authed(&server, &certs);
    // A correct token again: AUTHENTICATED, no other effect.
    assert_eq!(c.cmd(&format!("auth {OTHER_GOOD}")), "AUTHENTICATED");
    assert_eq!(c.put(b"x"), "INSERTED 1");
    // A wrong one: UNAUTHORIZED and the connection is closed (and
    // disconnected from the engine, releasing what it held).
    assert_eq!(c.body_reply("reserve").0, "RESERVED 1 1");
    expect_reply_then_close(
        &mut c,
        format!("auth {WRONG}\r\n").as_bytes(),
        "UNAUTHORIZED\r\n",
    );
    std::thread::sleep(Duration::from_millis(200));
    let mut ok = authed(&server, &certs);
    let stats = ok.yaml("stats");
    assert_eq!(stat_of(&stats, "current-connections"), "1");
    assert_eq!(stat_of(&stats, "total-connections"), "2");
    assert_eq!(stat_of(&stats, "current-jobs-ready"), "1");
}

#[test]
fn tokens_never_appear_in_logs() {
    let (mut server, certs) = ConfigServer::start(&config("trace"), 1, &["-V", "-V", "-V"]);
    let mut c = authed(&server, &certs);
    assert_eq!(c.cmd(&format!("auth {GOOD}")), "AUTHENTICATED");
    assert_eq!(c.put(b"x"), "INSERTED 1");
    let mut bad = connect(&server, &certs);
    expect_reply_then_close(
        &mut bad,
        format!("auth {WRONG}\r\n").as_bytes(),
        "UNAUTHORIZED\r\n",
    );
    let mut bad = connect(&server, &certs);
    expect_reply_then_close(&mut bad, b"stats\r\n", "UNAUTHORIZED\r\n");
    expect_reply_then_close(
        &mut c,
        format!("auth {WRONG}x\r\n").as_bytes(),
        "UNAUTHORIZED\r\n",
    );
    let status = server.stop(nix::sys::signal::Signal::SIGTERM);
    assert!(status.success(), "{status}");

    let log = server.stderr();
    // Logging did happen, at info with the peer address.
    assert!(log.contains("authentication failed"), "{log}");
    assert!(log.contains("127.0.0.1:"), "{log}");
    // Debug and trace output is on too.
    assert!(log.contains("client authenticated"), "{log}");
    assert!(log.contains("TRACE"), "{log}");
    for secret in [GOOD, OTHER_GOOD, WRONG, "s3cr3t", "wr0ng"] {
        assert!(
            !log.contains(secret),
            "{secret} leaked into the log:\n{log}"
        );
    }
}
