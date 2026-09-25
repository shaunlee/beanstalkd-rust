//! Unit tests for `parse_line`, covering all 25 commands (valid cases) and
//! the error cases called out in docs/PLAN.md T1: wrong argument count,
//! non-numeric, negative, u32/u64 overflow, leading/extra whitespace,
//! invalid tube characters, tube length 200/201, names starting with '-',
//! unknown command, wrong case.

#![allow(clippy::unwrap_used)]

use bstk_proto::{Command, JobId, Response, TubeName, parse_line};

fn tube(s: &str) -> TubeName {
    TubeName::new(s).expect("valid tube name in test fixture")
}

// ---- put ----

#[test]
fn put_valid() {
    // parse_line alone cannot read the body; it returns an empty body and
    // leaves body-reading to the codec.
    assert_eq!(
        parse_line(b"put 1 2 3 5"),
        Ok(Command::Put {
            pri: 1,
            delay: 2,
            ttr: 3,
            body: bytes::Bytes::new(),
        })
    );
}

#[test]
fn put_missing_args() {
    assert_eq!(parse_line(b"put 1 2 3"), Err(Response::BadFormat));
    assert_eq!(parse_line(b"put"), Err(Response::UnknownCommand)); // no trailing space: doesn't match "put " prefix at all
    assert_eq!(parse_line(b"put "), Err(Response::BadFormat));
}

#[test]
fn put_non_numeric() {
    assert_eq!(parse_line(b"put x 2 3 5"), Err(Response::BadFormat));
}

#[test]
fn put_negative() {
    assert_eq!(parse_line(b"put -1 2 3 5"), Err(Response::BadFormat));
}

#[test]
fn put_pri_u32_overflow() {
    assert_eq!(
        parse_line(b"put 4294967296 0 0 5"),
        Err(Response::BadFormat)
    );
}

#[test]
fn put_pri_u32_max_ok() {
    assert_eq!(
        parse_line(b"put 4294967295 0 0 5"),
        Ok(Command::Put {
            pri: u32::MAX,
            delay: 0,
            ttr: 0,
            body: bytes::Bytes::new(),
        })
    );
}

#[test]
fn put_extra_whitespace_between_fields_ok() {
    // read_u32 skips leading spaces, so extra spaces between fields are fine.
    assert_eq!(
        parse_line(b"put 1   2  3   5"),
        Ok(Command::Put {
            pri: 1,
            delay: 2,
            ttr: 3,
            body: bytes::Bytes::new(),
        })
    );
}

// Note: trailing garbage after `body_size` on a `put` line is only
// rejected once the codec knows the size is within `max_job_size` (see
// `job_too_big_ignores_trailing_garbage_on_put_line` and
// `put_trailing_garbage_after_size_bad_format` in codec_tests.rs).
// `parse_line` alone always accepts the header, since it doesn't have
// access to `max_job_size`.
#[test]
fn put_trailing_garbage_accepted_by_bare_parse_line() {
    assert_eq!(
        parse_line(b"put 0 0 0 5 extra"),
        Ok(Command::Put {
            pri: 0,
            delay: 0,
            ttr: 0,
            body: bytes::Bytes::new(),
        })
    );
}

// ---- use ----

#[test]
fn use_valid() {
    assert_eq!(parse_line(b"use foo"), Ok(Command::Use(tube("foo"))));
}

#[test]
fn use_missing_name() {
    assert_eq!(parse_line(b"use "), Err(Response::BadFormat));
    assert_eq!(parse_line(b"use"), Err(Response::UnknownCommand));
}

#[test]
fn use_trailing_garbage() {
    assert_eq!(parse_line(b"use foo bar"), Err(Response::BadFormat));
    assert_eq!(parse_line(b"use foo "), Err(Response::BadFormat));
}

#[test]
fn use_leading_extra_space() {
    assert_eq!(parse_line(b"use  foo"), Err(Response::BadFormat));
}

#[test]
fn use_invalid_chars() {
    assert_eq!(parse_line(b"use fo o"), Err(Response::BadFormat));
    assert_eq!(parse_line(b"use fo!o"), Err(Response::BadFormat));
}

#[test]
fn use_starts_with_dash() {
    assert_eq!(parse_line(b"use -foo"), Err(Response::BadFormat));
}

#[test]
fn use_len_200_ok() {
    let name = "a".repeat(200);
    assert_eq!(
        parse_line(format!("use {name}").as_bytes()),
        Ok(Command::Use(tube(&name)))
    );
}

#[test]
fn use_len_201_bad_format() {
    let name = "a".repeat(201);
    assert_eq!(
        parse_line(format!("use {name}").as_bytes()),
        Err(Response::BadFormat)
    );
}

// ---- reserve / reserve-with-timeout / reserve-job ----

#[test]
fn reserve_valid() {
    assert_eq!(parse_line(b"reserve"), Ok(Command::Reserve));
}

#[test]
fn reserve_trailing_garbage() {
    assert_eq!(parse_line(b"reserve x"), Err(Response::BadFormat));
    assert_eq!(parse_line(b"reserve "), Err(Response::BadFormat));
}

#[test]
fn reserve_with_timeout_valid() {
    assert_eq!(
        parse_line(b"reserve-with-timeout 5"),
        Ok(Command::ReserveWithTimeout(5))
    );
}

#[test]
fn reserve_with_timeout_zero_ok() {
    assert_eq!(
        parse_line(b"reserve-with-timeout 0"),
        Ok(Command::ReserveWithTimeout(0))
    );
}

#[test]
fn reserve_with_timeout_int_max_ok() {
    assert_eq!(
        parse_line(b"reserve-with-timeout 2147483647"),
        Ok(Command::ReserveWithTimeout(2147483647))
    );
}

#[test]
fn reserve_with_timeout_over_int_max() {
    assert_eq!(
        parse_line(b"reserve-with-timeout 2147483648"),
        Err(Response::BadFormat)
    );
}

#[test]
fn reserve_with_timeout_non_numeric() {
    assert_eq!(
        parse_line(b"reserve-with-timeout x"),
        Err(Response::BadFormat)
    );
}

#[test]
fn reserve_with_timeout_negative() {
    assert_eq!(
        parse_line(b"reserve-with-timeout -1"),
        Err(Response::BadFormat)
    );
}

#[test]
fn reserve_with_timeout_trailing_garbage_ignored() {
    // Quirk: prot.c never checks trailing garbage after the timeout value.
    assert_eq!(
        parse_line(b"reserve-with-timeout 5 garbage"),
        Ok(Command::ReserveWithTimeout(5))
    );
}

#[test]
fn reserve_job_valid() {
    assert_eq!(parse_line(b"reserve-job 42"), Ok(Command::ReserveJob(42)));
}

#[test]
fn reserve_job_missing_id() {
    assert_eq!(parse_line(b"reserve-job "), Err(Response::BadFormat));
}

#[test]
fn reserve_job_trailing_garbage() {
    assert_eq!(parse_line(b"reserve-job 42 x"), Err(Response::BadFormat));
}

// ---- delete ----

#[test]
fn delete_valid() {
    assert_eq!(parse_line(b"delete 42"), Ok(Command::Delete(42)));
}

#[test]
fn delete_non_numeric() {
    assert_eq!(parse_line(b"delete x"), Err(Response::BadFormat));
}

#[test]
fn delete_negative() {
    assert_eq!(parse_line(b"delete -1"), Err(Response::BadFormat));
}

#[test]
fn delete_trailing_space() {
    assert_eq!(parse_line(b"delete 1 "), Err(Response::BadFormat));
}

#[test]
fn delete_leading_space_ok() {
    assert_eq!(parse_line(b"delete  1"), Ok(Command::Delete(1)));
}

#[test]
fn delete_u64_overflow() {
    assert_eq!(
        parse_line(b"delete 99999999999999999999999999"),
        Err(Response::BadFormat)
    );
}

#[test]
fn delete_u64_max_ok() {
    let id: JobId = u64::MAX;
    assert_eq!(
        parse_line(format!("delete {id}").as_bytes()),
        Ok(Command::Delete(id))
    );
}

#[test]
fn delete_missing_arg() {
    assert_eq!(parse_line(b"delete "), Err(Response::BadFormat));
}

// ---- release ----

#[test]
fn release_valid() {
    assert_eq!(
        parse_line(b"release 1 2 3"),
        Ok(Command::Release {
            id: 1,
            pri: 2,
            delay: 3
        })
    );
}

#[test]
fn release_extra_spaces_ok() {
    assert_eq!(
        parse_line(b"release 1   2   3"),
        Ok(Command::Release {
            id: 1,
            pri: 2,
            delay: 3
        })
    );
}

#[test]
fn release_wrong_arg_count() {
    assert_eq!(parse_line(b"release 1 2"), Err(Response::BadFormat));
}

#[test]
fn release_negative_pri() {
    assert_eq!(parse_line(b"release 1 -2 3"), Err(Response::BadFormat));
}

#[test]
fn release_pri_u32_overflow() {
    assert_eq!(
        parse_line(b"release 1 4294967296 3"),
        Err(Response::BadFormat)
    );
}

#[test]
fn release_trailing_garbage() {
    assert_eq!(parse_line(b"release 1 2 3 x"), Err(Response::BadFormat));
}

// ---- bury ----

#[test]
fn bury_valid() {
    assert_eq!(parse_line(b"bury 1 2"), Ok(Command::Bury { id: 1, pri: 2 }));
}

#[test]
fn bury_wrong_arg_count() {
    assert_eq!(parse_line(b"bury 1"), Err(Response::BadFormat));
}

#[test]
fn bury_negative_pri() {
    assert_eq!(parse_line(b"bury 1 -2"), Err(Response::BadFormat));
}

#[test]
fn bury_trailing_garbage() {
    assert_eq!(parse_line(b"bury 1 2 3"), Err(Response::BadFormat));
}

// ---- touch ----

#[test]
fn touch_valid() {
    assert_eq!(parse_line(b"touch 1"), Ok(Command::Touch(1)));
}

#[test]
fn touch_non_numeric() {
    assert_eq!(parse_line(b"touch x"), Err(Response::BadFormat));
}

// ---- watch / ignore ----

#[test]
fn watch_valid() {
    assert_eq!(parse_line(b"watch foo"), Ok(Command::Watch(tube("foo"))));
}

#[test]
fn watch_invalid_tube() {
    assert_eq!(parse_line(b"watch -foo"), Err(Response::BadFormat));
}

#[test]
fn ignore_valid() {
    assert_eq!(parse_line(b"ignore foo"), Ok(Command::Ignore(tube("foo"))));
}

#[test]
fn ignore_len_201_bad_format() {
    let name = "a".repeat(201);
    assert_eq!(
        parse_line(format!("ignore {name}").as_bytes()),
        Err(Response::BadFormat)
    );
}

// ---- peek / peek-ready / peek-delayed / peek-buried ----

#[test]
fn peek_valid() {
    assert_eq!(parse_line(b"peek 1"), Ok(Command::Peek(1)));
}

#[test]
fn peek_non_numeric() {
    assert_eq!(parse_line(b"peek x"), Err(Response::BadFormat));
}

#[test]
fn peek_ready_valid() {
    assert_eq!(parse_line(b"peek-ready"), Ok(Command::PeekReady));
}

#[test]
fn peek_ready_trailing_garbage() {
    assert_eq!(parse_line(b"peek-readyx"), Err(Response::BadFormat));
    assert_eq!(parse_line(b"peek-ready x"), Err(Response::BadFormat));
}

#[test]
fn peek_delayed_valid() {
    assert_eq!(parse_line(b"peek-delayed"), Ok(Command::PeekDelayed));
}

#[test]
fn peek_buried_valid() {
    assert_eq!(parse_line(b"peek-buried"), Ok(Command::PeekBuried));
}

// ---- kick / kick-job ----

#[test]
fn kick_valid() {
    assert_eq!(parse_line(b"kick 5"), Ok(Command::Kick(5)));
}

#[test]
fn kick_no_digits() {
    assert_eq!(parse_line(b"kick "), Err(Response::BadFormat));
    assert_eq!(parse_line(b"kick"), Err(Response::UnknownCommand));
}

#[test]
fn kick_trailing_garbage_ignored() {
    // Quirk: kick uses strtoul directly, so trailing garbage is ignored.
    assert_eq!(parse_line(b"kick 5 foobar"), Ok(Command::Kick(5)));
}

#[test]
fn kick_negative_wraps() {
    // Quirk: strtoul accepts a sign and wraps within 64 bits, then
    // truncates to u32. -1 -> u64::MAX -> truncated to u32::MAX.
    assert_eq!(parse_line(b"kick -1"), Ok(Command::Kick(u32::MAX)));
}

#[test]
fn kick_plus_sign_ok() {
    assert_eq!(parse_line(b"kick +5"), Ok(Command::Kick(5)));
}

#[test]
fn kick_leading_tab_ok() {
    assert_eq!(parse_line(b"kick \t5"), Ok(Command::Kick(5)));
}

#[test]
fn kick_u32_boundary_truncates() {
    // 2^32 + 2 truncates to 2 (silent wraparound, unlike read_u32).
    assert_eq!(parse_line(b"kick 4294967298"), Ok(Command::Kick(2)));
}

#[test]
fn kick_u64_max_ok_no_overflow() {
    assert_eq!(
        parse_line(b"kick 18446744073709551615"),
        Ok(Command::Kick(u32::MAX))
    );
}

#[test]
fn kick_u64_overflow_bad_format() {
    assert_eq!(
        parse_line(b"kick 18446744073709551616"),
        Err(Response::BadFormat)
    );
}

#[test]
fn kick_job_valid() {
    assert_eq!(parse_line(b"kick-job 5"), Ok(Command::KickJob(5)));
}

#[test]
fn kick_job_non_numeric() {
    assert_eq!(parse_line(b"kick-job x"), Err(Response::BadFormat));
}

// ---- stats / stats-job / stats-tube ----

#[test]
fn stats_valid() {
    assert_eq!(parse_line(b"stats"), Ok(Command::Stats));
}

#[test]
fn stats_trailing_garbage() {
    assert_eq!(parse_line(b"statsx"), Err(Response::BadFormat));
}

#[test]
fn stats_job_valid() {
    assert_eq!(parse_line(b"stats-job 1"), Ok(Command::StatsJob(1)));
}

#[test]
fn stats_job_non_numeric() {
    assert_eq!(parse_line(b"stats-job x"), Err(Response::BadFormat));
}

#[test]
fn stats_tube_valid() {
    assert_eq!(
        parse_line(b"stats-tube foo"),
        Ok(Command::StatsTube(tube("foo")))
    );
}

#[test]
fn stats_tube_missing_name() {
    assert_eq!(parse_line(b"stats-tube "), Err(Response::BadFormat));
    // Quirk: CMD_STATS_TUBE is "stats-tube " (with a trailing space), so
    // without a space this instead matches the bare "stats" prefix
    // (which_cmd tests "stats" after "stats-tube "/"stats-job ", but
    // "stats-tube" without a space is too short to match either of
    // those), landing in the OP_STATS case, which then rejects the
    // leftover "-tube" as trailing garbage.
    assert_eq!(parse_line(b"stats-tube"), Err(Response::BadFormat));
}

#[test]
fn stats_tube_trailing_space() {
    assert_eq!(parse_line(b"stats-tube foo "), Err(Response::BadFormat));
}

// ---- list-tubes / list-tube-used / list-tubes-watched ----

#[test]
fn list_tubes_valid() {
    assert_eq!(parse_line(b"list-tubes"), Ok(Command::ListTubes));
}

#[test]
fn list_tubes_trailing_garbage() {
    assert_eq!(parse_line(b"list-tubesx"), Err(Response::BadFormat));
}

#[test]
fn list_tube_used_valid() {
    assert_eq!(parse_line(b"list-tube-used"), Ok(Command::ListTubeUsed));
}

#[test]
fn list_tubes_watched_valid() {
    assert_eq!(
        parse_line(b"list-tubes-watched"),
        Ok(Command::ListTubesWatched)
    );
}

// ---- quit ----

#[test]
fn quit_valid() {
    assert_eq!(parse_line(b"quit"), Ok(Command::Quit));
}

#[test]
fn quit_trailing_garbage_ignored() {
    // Quirk: prot.c never checks anything after "quit" matches.
    assert_eq!(parse_line(b"quit garbage"), Ok(Command::Quit));
}

// ---- pause-tube ----

#[test]
fn pause_tube_valid() {
    assert_eq!(
        parse_line(b"pause-tube foo 5"),
        Ok(Command::PauseTube {
            tube: tube("foo"),
            delay: 5
        })
    );
}

#[test]
fn pause_tube_no_space_after_command_name() {
    // Quirk: CMD_PAUSE_TUBE has no trailing space in prot.c, so
    // "pause-tubefoo 5" parses as tube "foo", delay 5.
    assert_eq!(
        parse_line(b"pause-tubefoo 5"),
        Ok(Command::PauseTube {
            tube: tube("foo"),
            delay: 5
        })
    );
}

#[test]
fn pause_tube_extra_leading_spaces_before_name_ok() {
    assert_eq!(
        parse_line(b"pause-tube  foo 5"),
        Ok(Command::PauseTube {
            tube: tube("foo"),
            delay: 5
        })
    );
}

#[test]
fn pause_tube_extra_spaces_before_delay_ok() {
    assert_eq!(
        parse_line(b"pause-tube foo   5"),
        Ok(Command::PauseTube {
            tube: tube("foo"),
            delay: 5
        })
    );
}

#[test]
fn pause_tube_invalid_name_is_a_counted_command() {
    // prot.c counts cmd-pause-tube before validating the name span, so an
    // invalid name (leading '-', or > 200 bytes) is not a plain decode
    // error: it is forwarded to the engine, which replies BAD_FORMAT.
    assert_eq!(
        parse_line(b"pause-tube -foo 5"),
        Ok(Command::PauseTubeBadName)
    );
    let long = format!("pause-tube {} 5", "a".repeat(201));
    assert_eq!(parse_line(long.as_bytes()), Ok(Command::PauseTubeBadName));
    let ok = format!("pause-tube {} 5", "a".repeat(200));
    assert!(matches!(
        parse_line(ok.as_bytes()),
        Ok(Command::PauseTube { delay: 5, .. })
    ));
    // Failures before the counter (no name span, bad delay) stay errors.
    assert_eq!(parse_line(b"pause-tube -foo x"), Err(Response::BadFormat));
    assert_eq!(parse_line(b"pause-tube *foo 5"), Err(Response::BadFormat));
}

#[test]
fn pause_tube_missing_delay() {
    assert_eq!(parse_line(b"pause-tube foo"), Err(Response::BadFormat));
}

#[test]
fn pause_tube_trailing_space_after_delay() {
    assert_eq!(parse_line(b"pause-tube foo 5 "), Err(Response::BadFormat));
}

#[test]
fn pause_tube_tab_before_name_invalid() {
    // Quirk: read_tube_name only skips ' ', not '\t'.
    assert_eq!(parse_line(b"pause-tube\tfoo 5"), Err(Response::BadFormat));
}

#[test]
fn pause_tube_no_separator_swallows_digits() {
    // Quirk: digits are NAME_CHARS too, so with no space the whole thing
    // (including what looks like the delay) is greedily consumed as the
    // tube name, leaving nothing for read_duration.
    assert_eq!(parse_line(b"pause-tube foo5"), Err(Response::BadFormat));
}

// ---- unknown command / case sensitivity / malformed input ----

#[test]
fn unknown_command() {
    assert_eq!(parse_line(b"frobnicate"), Err(Response::UnknownCommand));
}

#[test]
fn empty_line_is_unknown() {
    assert_eq!(parse_line(b""), Err(Response::UnknownCommand));
}

#[test]
fn wrong_case_is_unknown() {
    assert_eq!(parse_line(b"USE foo"), Err(Response::UnknownCommand));
    assert_eq!(parse_line(b"Quit"), Err(Response::UnknownCommand));
}

#[test]
fn embedded_nul_is_bad_format() {
    // Quirk: an embedded NUL byte anywhere in the line is rejected before
    // any command-specific parsing, regardless of command.
    assert_eq!(parse_line(b"use fo\0o"), Err(Response::BadFormat));
    assert_eq!(parse_line(b"quit\0"), Err(Response::BadFormat));
}
