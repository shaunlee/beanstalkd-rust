//! Shared test helpers: a client-side encoder (the reverse of
//! `parse_line`/`ServerCodec`) and `proptest` strategies for generating
//! arbitrary valid commands. Included by other test files via
//! `#[path = "support.rs"] mod support;`.

#![allow(dead_code, clippy::unwrap_used)]

use bstk_proto::{Command, JobId, TubeName};
use proptest::prelude::*;

/// The exact `NAME_CHARS` alphabet from prot.c.
const NAME_CHARS: &[u8] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-+/;.$_()";

/// A valid tube name (1..=200 bytes of `NAME_CHARS`, not starting with `-`).
pub fn valid_tube_name() -> impl Strategy<Value = String> {
    let first = (0..NAME_CHARS.len())
        .filter(|&i| NAME_CHARS[i] != b'-')
        .map(|i| NAME_CHARS[i] as char)
        .collect::<Vec<_>>();
    (
        proptest::sample::select(first),
        proptest::collection::vec(proptest::sample::select(NAME_CHARS.to_vec()), 0..20),
    )
        .prop_map(|(first, rest)| {
            let mut s = String::new();
            s.push(first);
            for b in rest {
                s.push(b as char);
            }
            s
        })
}

pub fn arb_tube_name() -> impl Strategy<Value = TubeName> {
    valid_tube_name().prop_map(|s| TubeName::new(&s).expect("generated name is valid"))
}

pub fn arb_job_id() -> impl Strategy<Value = JobId> {
    any::<u64>()
}

pub fn arb_command() -> impl Strategy<Value = Command> {
    prop_oneof![
        (
            0u32..=u32::MAX,
            0u32..=1000,
            0u32..=1000,
            proptest::collection::vec(any::<u8>(), 0..2000)
        )
            .prop_map(|(pri, delay, ttr, body)| Command::Put {
                pri,
                delay,
                ttr,
                body: body.into(),
            }),
        arb_tube_name().prop_map(Command::Use),
        Just(Command::Reserve),
        (0u32..=i32::MAX as u32).prop_map(Command::ReserveWithTimeout),
        arb_job_id().prop_map(Command::ReserveJob),
        arb_job_id().prop_map(Command::Delete),
        (arb_job_id(), any::<u32>(), 0u32..=100000)
            .prop_map(|(id, pri, delay)| { Command::Release { id, pri, delay } }),
        (arb_job_id(), any::<u32>()).prop_map(|(id, pri)| Command::Bury { id, pri }),
        arb_job_id().prop_map(Command::Touch),
        arb_tube_name().prop_map(Command::Watch),
        arb_tube_name().prop_map(Command::Ignore),
        arb_job_id().prop_map(Command::Peek),
        Just(Command::PeekReady),
        Just(Command::PeekDelayed),
        Just(Command::PeekBuried),
        any::<u32>().prop_map(Command::Kick),
        arb_job_id().prop_map(Command::KickJob),
        arb_job_id().prop_map(Command::StatsJob),
        arb_tube_name().prop_map(Command::StatsTube),
        Just(Command::Stats),
        Just(Command::ListTubes),
        Just(Command::ListTubeUsed),
        Just(Command::ListTubesWatched),
        Just(Command::Quit),
        (arb_tube_name(), 0u32..=100000)
            .prop_map(|(tube, delay)| Command::PauseTube { tube, delay }),
    ]
}

/// The reverse of `parse_line`/`ServerCodec`: encodes a `Command` exactly
/// as a well-behaved client would send it on the wire.
pub fn encode_command(cmd: &Command) -> Vec<u8> {
    match cmd {
        Command::Put {
            pri,
            delay,
            ttr,
            body,
        } => {
            let mut out = format!("put {pri} {delay} {ttr} {}\r\n", body.len()).into_bytes();
            out.extend_from_slice(body);
            out.extend_from_slice(b"\r\n");
            out
        }
        Command::Use(t) => format!("use {t}\r\n").into_bytes(),
        Command::Reserve => b"reserve\r\n".to_vec(),
        Command::ReserveWithTimeout(t) => format!("reserve-with-timeout {t}\r\n").into_bytes(),
        Command::ReserveJob(id) => format!("reserve-job {id}\r\n").into_bytes(),
        Command::Delete(id) => format!("delete {id}\r\n").into_bytes(),
        Command::Release { id, pri, delay } => {
            format!("release {id} {pri} {delay}\r\n").into_bytes()
        }
        Command::Bury { id, pri } => format!("bury {id} {pri}\r\n").into_bytes(),
        Command::Touch(id) => format!("touch {id}\r\n").into_bytes(),
        Command::Watch(t) => format!("watch {t}\r\n").into_bytes(),
        Command::Ignore(t) => format!("ignore {t}\r\n").into_bytes(),
        Command::Peek(id) => format!("peek {id}\r\n").into_bytes(),
        Command::PeekReady => b"peek-ready\r\n".to_vec(),
        Command::PeekDelayed => b"peek-delayed\r\n".to_vec(),
        Command::PeekBuried => b"peek-buried\r\n".to_vec(),
        Command::Kick(n) => format!("kick {n}\r\n").into_bytes(),
        Command::KickJob(id) => format!("kick-job {id}\r\n").into_bytes(),
        Command::StatsJob(id) => format!("stats-job {id}\r\n").into_bytes(),
        Command::StatsTube(t) => format!("stats-tube {t}\r\n").into_bytes(),
        Command::Stats => b"stats\r\n".to_vec(),
        Command::ListTubes => b"list-tubes\r\n".to_vec(),
        Command::ListTubeUsed => b"list-tube-used\r\n".to_vec(),
        Command::ListTubesWatched => b"list-tubes-watched\r\n".to_vec(),
        Command::Quit => b"quit\r\n".to_vec(),
        Command::PauseTube { tube, delay } => format!("pause-tube {tube} {delay}\r\n").into_bytes(),
    }
}
