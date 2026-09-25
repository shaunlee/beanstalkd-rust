//! Black-box tests of `-b` (binlog) support: startup, restart after
//! SIGTERM and SIGKILL, lock contention, fsync modes, OUT_OF_MEMORY on
//! reservation failure, graceful shutdown and write-before-reply.

#![allow(clippy::unwrap_used)]

mod common;

use std::path::Path;

use nix::sys::signal::Signal;

use common::{Client, Server, run_to_exit};

fn start(dir: &Path, extra: &[&str]) -> Server {
    let mut args = vec!["-b", dir.to_str().unwrap()];
    args.extend_from_slice(extra);
    Server::start(&args)
}

fn state(c: &mut Client, id: u64) -> String {
    c.stat(&format!("stats-job {id}"), "state")
}

#[test]
fn starts_on_an_empty_directory() {
    let dir = tempfile::tempdir().unwrap();
    let bin = dir.path().join("wal");
    let server = start(&bin, &[]);
    let mut c = server.connect();
    assert_eq!(c.stat("stats", "binlog-max-size"), "10485760");
    assert_eq!(c.stat("stats", "binlog-records-written"), "0");
    assert_eq!(c.put(0, 0, 60, b"x"), "INSERTED 1");
    assert_eq!(c.stat("stats", "binlog-records-written"), "1");
    let names: Vec<String> = std::fs::read_dir(&bin)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert!(names.iter().any(|n| n == "lock"), "{names:?}");
    assert!(names.iter().any(|n| n.starts_with("binlog.")), "{names:?}");
}

#[test]
fn binlog_max_size_reports_s_unrounded() {
    let dir = tempfile::tempdir().unwrap();
    let server = start(dir.path(), &["-s", "5000"]);
    let mut c = server.connect();
    assert_eq!(c.stat("stats", "binlog-max-size"), "5000");
    // Without -b too, as the reference.
    let server = Server::start(&["-s", "5000"]);
    let mut c = server.connect();
    assert_eq!(c.stat("stats", "binlog-max-size"), "5000");
}

/// Puts five jobs and moves them through every journaled transition,
/// leaving job 1 reserved (not journaled) when the server is stopped.
fn build_state(c: &mut Client) {
    for n in 1..=5 {
        assert_eq!(
            c.put(100, 0, 60, format!("job{n}").as_bytes()),
            format!("INSERTED {n}")
        );
    }
    // 2: buried with pri 5.
    assert_eq!(c.cmd("reserve-job 2"), "RESERVED 2 4");
    c.read_line();
    assert_eq!(c.cmd("bury 2 5"), "BURIED");
    // 3: released with a delay.
    assert_eq!(c.cmd("reserve-job 3"), "RESERVED 3 4");
    c.read_line();
    assert_eq!(c.cmd("release 3 7 1000"), "RELEASED");
    // 4: buried, then kicked.
    assert_eq!(c.cmd("reserve-job 4"), "RESERVED 4 4");
    c.read_line();
    assert_eq!(c.cmd("bury 4 100"), "BURIED");
    assert_eq!(c.cmd("kick-job 4"), "KICKED");
    // 5: deleted.
    assert_eq!(c.cmd("delete 5"), "DELETED");
    // 1: reserved at shutdown.
    assert_eq!(c.cmd("reserve-job 1"), "RESERVED 1 4");
    c.read_line();
}

fn check_state(c: &mut Client) {
    assert_eq!(state(c, 1), "ready");
    assert_eq!(c.stat("stats-job 1", "reserves"), "0");
    assert_eq!(state(c, 2), "buried");
    assert_eq!(c.stat("stats-job 2", "pri"), "5");
    assert_eq!(state(c, 3), "delayed");
    assert_eq!(c.stat("stats-job 3", "pri"), "7");
    assert_eq!(c.stat("stats-job 3", "delay"), "1000");
    assert_eq!(c.stat("stats-job 3", "releases"), "1");
    assert_eq!(state(c, 4), "ready");
    assert_eq!(c.stat("stats-job 4", "kicks"), "1");
    assert_eq!(c.cmd("stats-job 5"), "NOT_FOUND");
    c.send(b"peek 4\r\n");
    let (header, body) = c.read_body_reply();
    assert_eq!(header, "FOUND 4 4\r\n");
    assert_eq!(body, b"job4");
    // Ids continue after the highest one in the binlog.
    assert_eq!(c.put(0, 0, 60, b"new"), "INSERTED 6");
}

fn restart_keeps_state(sig: Signal, extra: &[&str]) {
    let dir = tempfile::tempdir().unwrap();
    let mut server = start(dir.path(), extra);
    build_state(&mut server.connect());
    let status = server.stop(sig);
    if sig != Signal::SIGKILL {
        assert_eq!(status.code(), Some(0), "stderr: {}", server.stderr());
    }
    drop(server);
    let server = start(dir.path(), extra);
    check_state(&mut server.connect());
}

#[test]
fn state_survives_sigterm_restart() {
    restart_keeps_state(Signal::SIGTERM, &[]);
}

#[test]
fn state_survives_sigint_restart() {
    restart_keeps_state(Signal::SIGINT, &[]);
}

#[test]
fn state_survives_sigkill_restart() {
    restart_keeps_state(Signal::SIGKILL, &[]);
}

#[test]
fn fsync_always_mode_keeps_state() {
    restart_keeps_state(Signal::SIGKILL, &["-f0"]);
    restart_keeps_state(Signal::SIGTERM, &["-f", "0"]);
}

#[test]
fn never_fsync_mode_keeps_state_across_process_crashes() {
    // -F only risks power loss; a killed process keeps its page-cache writes.
    restart_keeps_state(Signal::SIGKILL, &["-F"]);
    restart_keeps_state(Signal::SIGTERM, &["-F"]);
}

#[test]
fn graceful_shutdown_with_a_long_interval_exits_zero_and_keeps_state() {
    // With a 60 s interval nothing is fsynced while serving; SIGTERM must
    // still exit promptly (after its final sync) and keep every job.
    restart_keeps_state(Signal::SIGTERM, &["-f", "60000"]);
}

#[test]
fn second_instance_on_the_same_directory_exits_10() {
    let dir = tempfile::tempdir().unwrap();
    let server = start(dir.path(), &[]);
    let (status, stderr) = run_to_exit(&["-b", dir.path().to_str().unwrap()]);
    assert_eq!(status.code(), Some(10), "stderr: {stderr}");
    assert!(stderr.contains("failed to lock wal dir"), "{stderr}");
    // The first instance is unaffected.
    assert_eq!(server.connect().put(0, 0, 1, b"x"), "INSERTED 1");
}

#[test]
fn unusable_binlog_directory_exits_1() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("not-a-dir");
    std::fs::write(&file, b"x").unwrap();
    let (status, stderr) = run_to_exit(&["-b", file.to_str().unwrap()]);
    assert_eq!(status.code(), Some(1), "stderr: {stderr}");
}

#[test]
fn user_flag_is_rejected() {
    let (status, stderr) = run_to_exit(&["-u", "nobody"]);
    assert_eq!(status.code(), Some(5), "stderr: {stderr}");
}

#[test]
fn put_that_cannot_be_reserved_replies_out_of_memory() {
    // A put whose records cannot fit in one 4096-byte segment cannot be
    // reserved (docs/COMPAT.md D7). The id is still consumed, the
    // connection keeps working and the reservation does not leak.
    let dir = tempfile::tempdir().unwrap();
    let mut server = start(dir.path(), &["-s", "4096", "-z", "10000"]);
    let mut c = server.connect();
    assert_eq!(c.put(0, 0, 60, &[b'a'; 5000]), "OUT_OF_MEMORY");
    for n in 2..=40 {
        assert_eq!(c.put(0, 0, 60, &[b'b'; 1000]), format!("INSERTED {n}"));
    }
    assert_eq!(c.stat("stats", "binlog-records-written"), "39");
    assert_eq!(c.cmd("stats-job 1"), "NOT_FOUND");
    server.stop(Signal::SIGTERM);
    drop(server);
    let server = start(dir.path(), &["-s", "4096", "-z", "10000"]);
    let mut c = server.connect();
    assert_eq!(c.stat("stats", "current-jobs-ready"), "39");
    assert_eq!(c.put(0, 0, 60, b"z"), "INSERTED 41");
}

#[test]
fn acknowledged_changes_survive_an_immediate_kill() {
    // Write before reply: SIGKILL right after the reply arrives, then
    // check that the acknowledged change is on disk.
    let dir = tempfile::tempdir().unwrap();
    let mut server = start(dir.path(), &[]);
    for round in 0..50u64 {
        let mut c = server.connect();
        let id = round + 1;
        assert_eq!(
            c.put(0, 0, 60, format!("r{round}").as_bytes()),
            format!("INSERTED {id}")
        );
        if round % 2 == 1 {
            // Every other round also deletes the previous round's job.
            assert_eq!(c.cmd(&format!("delete {}", id - 1)), "DELETED");
        }
        server.stop(Signal::SIGKILL);
        drop(server);
        server = start(dir.path(), &[]);
        let mut c = server.connect();
        assert_eq!(state(&mut c, id), "ready", "round {round}");
        if round % 2 == 1 {
            assert_eq!(c.cmd(&format!("stats-job {}", id - 1)), "NOT_FOUND");
        }
    }
}
