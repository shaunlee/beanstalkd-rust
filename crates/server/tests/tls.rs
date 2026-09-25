//! TLS and mTLS listeners (P2-T4): the protocol over TLS, handshake
//! failures that never reach the engine, client-certificate checks, and
//! several listeners sharing one engine.

#![allow(clippy::unwrap_used)]

mod common;

use std::io::Write;
use std::net::Shutdown;
use std::time::Duration;

use common::p2::{ClientCert, ConfigServer, End, stat_of, tls_connect};

/// Port 0: TLS without client certificates; port 1: mTLS; port 2:
/// plaintext. (Startup probes every port with a bare TCP connection: that
/// counts in `total-connections` on the plaintext port only.)
const CONFIG: &str = r#"
[[listener]]
addr = "127.0.0.1:{port0}"
tls = true

[[listener]]
addr = "127.0.0.1:{port1}"
tls = true
auth = "mtls"

[[listener]]
addr = "127.0.0.1:{port2}"

[tls]
cert = "server.pem"
key = "server.key"
client_ca = "ca.pem"
"#;

/// The same TLS listeners without the plaintext one.
const TLS_ONLY: &str = r#"
[[listener]]
addr = "127.0.0.1:{port0}"
tls = true

[[listener]]
addr = "127.0.0.1:{port1}"
tls = true
auth = "mtls"

[tls]
cert = "server.pem"
key = "server.key"
client_ca = "ca.pem"
"#;

fn start() -> (ConfigServer, common::p2::Certs) {
    ConfigServer::start(TLS_ONLY, 2, &[])
}

#[test]
fn put_reserve_delete_over_tls() {
    let (server, certs) = start();
    let mut c = server.tls(0, certs.client_config(ClientCert::None));
    assert_eq!(c.put(b"hello"), "INSERTED 1");
    let (header, body) = c.body_reply("reserve");
    assert_eq!(header, "RESERVED 1 5");
    assert_eq!(body, b"hello");
    assert_eq!(c.cmd("delete 1"), "DELETED");
    // The listener does not know `auth`: it stays an unknown command.
    assert_eq!(c.cmd("auth something"), "UNKNOWN_COMMAND");
    let stats = c.yaml("stats");
    assert_eq!(stat_of(&stats, "current-connections"), "1");
    assert_eq!(stat_of(&stats, "total-connections"), "1");
}

#[test]
fn plaintext_client_on_a_tls_port_fails_cleanly() {
    let (server, certs) = start();
    for _ in 0..3 {
        let mut p = server.plain(0);
        p.send(b"stats\r\nput 0 0 1 1\r\nx\r\n");
        let (bytes, end) = p.read_to_end(Duration::from_secs(5));
        assert_ne!(end, End::Timeout, "the server must close the connection");
        let text = String::from_utf8_lossy(&bytes);
        assert!(
            !text.contains("OK ") && !text.contains("INSERTED"),
            "no protocol reply over a failed handshake: {text:?}"
        );
    }
    // No trace in the engine: no connection, no command, no job id.
    let mut c = server.tls(0, certs.client_config(ClientCert::None));
    let stats = c.yaml("stats");
    assert_eq!(stat_of(&stats, "total-connections"), "1");
    assert_eq!(stat_of(&stats, "current-connections"), "1");
    assert_eq!(stat_of(&stats, "cmd-put"), "0");
    assert_eq!(c.put(b"x"), "INSERTED 1");
}

/// With TLS 1.3 the client finishes its side of the handshake before the
/// server checks its certificate, so a rejection may only show when
/// reading. Either way no protocol reply may ever arrive.
fn assert_rejected(server: &ConfigServer, cert: ClientCert, certs: &common::p2::Certs) {
    let Ok(mut c) = tls_connect(server.addr(1), certs.client_config(cert)) else {
        return;
    };
    let _ = c.stream.write_all(b"stats\r\n");
    let _ = c.stream.flush();
    let (bytes, end) = c.read_to_end(Duration::from_secs(5));
    assert!(bytes.is_empty(), "{cert:?}: got {bytes:?}");
    assert_ne!(end, End::Timeout, "{cert:?}: connection must be closed");
    if let End::Error(e) = &end {
        assert!(
            e.contains("Certificate")
                || e.contains("certificate")
                || e.contains("reset")
                || e.contains("alert"),
            "{cert:?}: unexpected error {e}"
        );
    }
}

#[test]
fn mtls_requires_a_client_certificate_from_the_configured_ca() {
    let (server, certs) = start();
    assert_rejected(&server, ClientCert::None, &certs);
    assert_rejected(&server, ClientCert::WrongCa, &certs);

    let mut c = server.tls(1, certs.client_config(ClientCert::Valid));
    assert_eq!(c.put(b"mtls"), "INSERTED 1");
    // mTLS listeners do not recognize `auth` either.
    assert_eq!(c.cmd("auth token"), "UNKNOWN_COMMAND");
    let stats = c.yaml("stats");
    // The rejected handshakes never reached the engine.
    assert_eq!(stat_of(&stats, "total-connections"), "1");
    assert_eq!(stat_of(&stats, "current-connections"), "1");

    // A valid client certificate is not required (nor requested) on the
    // plain TLS listener, but is harmless there.
    let mut other = server.tls(0, certs.client_config(ClientCert::Valid));
    assert_eq!(other.cmd("use default"), "USING default");
}

#[test]
fn listeners_share_one_engine() {
    let (server, certs) = ConfigServer::start(CONFIG, 3, &[]);
    let mut plain = server.plain(2);
    let mut tls = server.tls(0, certs.client_config(ClientCert::None));
    let mut mtls = server.tls(1, certs.client_config(ClientCert::Valid));
    assert_eq!(plain.put(b"a"), "INSERTED 1");
    assert_eq!(tls.put(b"b"), "INSERTED 2");
    assert_eq!(mtls.put(b"c"), "INSERTED 3");
    let (h, body) = tls.body_reply("reserve");
    assert_eq!((h.as_str(), body.as_slice()), ("RESERVED 1 1", &b"a"[..]));
    let (h, _) = plain.body_reply("reserve");
    assert_eq!(h, "RESERVED 2 1");
    let stats = mtls.yaml("stats");
    assert_eq!(stat_of(&stats, "current-connections"), "3");
    // Plus the startup probe on the plaintext port.
    assert_eq!(stat_of(&stats, "total-connections"), "4");
    drop(plain);
    drop(tls);
    // The drop-guard Disconnect released the reserved jobs.
    std::thread::sleep(Duration::from_millis(200));
    let stats = mtls.yaml("stats");
    assert_eq!(stat_of(&stats, "current-connections"), "1");
    assert_eq!(stat_of(&stats, "current-jobs-ready"), "3");
}

#[test]
fn close_notify_half_close_times_out_a_waiting_reserve() {
    let (server, certs) = start();
    let mut c = server.tls(0, certs.client_config(ClientCert::None));
    c.send(b"reserve\r\n");
    // Half-close the TLS way: close_notify, then a TCP FIN.
    c.stream.conn.send_close_notify();
    while c.stream.conn.wants_write() {
        c.stream.conn.write_tls(&mut c.stream.sock).unwrap();
    }
    c.stream.sock.shutdown(Shutdown::Write).unwrap();
    let (bytes, end) = c.read_to_end(Duration::from_secs(5));
    assert_eq!(String::from_utf8_lossy(&bytes), "TIMED_OUT\r\n");
    assert_eq!(end, End::Closed);
}

#[test]
fn tcp_eof_without_close_notify_is_an_end_of_stream() {
    let (server, certs) = start();
    let mut c = server.tls(0, certs.client_config(ClientCert::None));
    // Pipelined put + reserve, then a bare TCP FIN (no close_notify): the
    // buffered commands are still served, as over plaintext.
    c.stream
        .write_all(b"put 0 0 60 2\r\nhi\r\nreserve\r\n")
        .unwrap();
    c.stream.flush().unwrap();
    c.stream.sock.shutdown(Shutdown::Write).unwrap();
    let (bytes, _end) = c.read_to_end(Duration::from_secs(5));
    assert_eq!(
        String::from_utf8_lossy(&bytes),
        "INSERTED 1\r\nRESERVED 1 2\r\nhi\r\n"
    );
}

#[test]
fn stalled_handshakes_do_not_block_other_clients() {
    let (server, certs) = start();
    // Connections that never start a handshake.
    let idle: Vec<_> = (0..20).map(|_| server.plain(0)).collect();
    let mut c = server.tls(0, certs.client_config(ClientCert::None));
    assert_eq!(c.put(b"x"), "INSERTED 1");
    assert_eq!(c.stat("stats", "current-connections"), "1");
    drop(idle);
}

#[test]
fn a_stalled_handshake_is_closed_after_the_timeout() {
    let (server, _certs) = start();
    let mut idle = server.plain(0);
    let started = std::time::Instant::now();
    let (bytes, end) = idle.read_to_end(Duration::from_secs(20));
    let waited = started.elapsed();
    assert!(bytes.is_empty(), "{bytes:?}");
    assert_ne!(end, End::Timeout);
    assert!(
        waited >= Duration::from_secs(9) && waited < Duration::from_secs(15),
        "closed after {waited:?}"
    );
}
