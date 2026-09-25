# Compatibility Differences vs. Reference beanstalkd

Reference: beanstalkd commit `25085c5`. Each entry states the observed behavior, the reason, whether it is intentional, and the covering test.

## Known differences

| # | Area | Difference | Reason | Intentional? | Test |
|---|---|---|---|---|---|
| D1 | engine | Reference: when a connection's own reserved job expires in the same event pass in which it is waiting, `process_queue` may re-reserve that job to the same connection, then the queued `RESERVED` reply is discarded and `DEADLINE_SOON` is sent instead. We recompute the DEADLINE_SOON / TIMED_OUT decision after draining expirations, using live state. | Reproducing it would require retracting an already-queued reply; practically unreachable with real client timing. | Yes | engine unit tests (tick) |
| D2 | engine / proto | `time-left` (stats-job) and `pause-time-left` (stats-tube) are signed in the reference and can be briefly negative (job overdue but not yet ticked). We clamp them to 0. | Stats types are unsigned; the window is sub-tick. Revisit if differential tests observe it. | Yes | — |
| D3 | server | A connection blocked awaiting a reply (e.g. `reserve`) stops reading once 64 KiB of pipelined input is buffered. The reference does not read at all while waiting but detects hangup via EPOLLRDHUP, so it notices a half-close even with unread data. We notice it only while under the cap. | Bounded memory per connection; only matters for a client that pipelines > 64 KiB behind a blocking reserve and then half-closes. | Yes | `large_pipeline_behind_blocked_reserve_is_processed_in_order` |
| D4 | server CLI | A `-z` value the reference's `sscanf("%zu")` cannot parse exits with status 2 (clap) instead of 5. `-l` accepts IP literals only, not hostnames. | Standard CLI tooling; hostnames rarely used. | Yes | — |
| D5 | binlog | On a binlog write, fsync or compaction error the server logs it and exits non-zero (fail-stop). The reference silently disables its binlog and keeps serving. | Never acknowledge a change that was not persisted. | Yes | P1-T4 server tests |
| D6 | binlog | bury, release with delay and kick never reply `OUT_OF_MEMORY`. The reference reserves binlog space for every update and can reply `OUT_OF_MEMORY` when the disk is full. The store keeps one spare preallocated segment for updates; only new puts are refused (`OUT_OF_MEMORY`) when a new spare cannot be allocated. | Deciding whether a command will write a record would need engine logic before the engine runs. | Yes | store reservation tests |
| D7 | binlog | A put whose records don't fit in one segment (`-z` larger than `-s`) replies `OUT_OF_MEMORY`. The reference lets the file grow. | Records never straddle segments. | Yes | store unit tests |
| D8 | binlog | On-disk format and file layout are our own. Differential tests mask `file` (stats-job), `binlog-oldest-index`, `binlog-current-index` and `binlog-records-migrated`. Compaction timing differs, so after compaction `binlog-records-written`, buried FIFO order and `list-tubes` order after a restart may differ from the reference. | Format compatibility is a non-goal (DESIGN §1). | Yes | binlog cases stay pre-compaction |
| D9 | binlog | Replay: a bad record in the last segment holding records truncates the log there with a warning; corruption in an earlier segment (followed by valid records in a later one), a bad header or an undecodable record refuses to start. | Unsynced tails may be torn after power loss; earlier corruption means real data loss. | Yes | store recovery tests |
| D10 | server CLI | `-s` above 4 GiB (including a wrapped `-1`) makes startup with `-b` fail with status 1. `-u USER` is rejected with status 5 instead of switching user. A binlog error while serving exits with status 20 (see D5). | Segments are preallocated in full; privilege dropping is left to the service manager. | Yes | server binlog tests, store unit tests |
| D11 | server | Commands arriving on different connections within a fraction of a millisecond may be processed in either order (multi-threaded I/O). The reference processes them in epoll order. | Per-connection order is preserved; clients cannot observe sub-millisecond cross-connection order reliably anyway. | Yes | `reserve_waiter_order.bt` uses a 50 ms gap |
| D12 | engine | The reference sometimes leaves a re-reserved job reserved past its TTR (`time-left` negative) until its connection does I/O (seen in about 3% of runs). We always expire it on time. | Reference bug; not reproducible deterministically. | Yes | `ttr_expiry_two_jobs_same_conn_to_waiters.bt` observes only the first expiry |
| D13 | server | SIGINT / SIGTERM shut down gracefully: stop accepting, finish the message in progress, fsync the binlog unless `-F`, exit 0. The reference has no handler and is killed by the signal (when it runs as pid 1 it calls `exit(143)`). Open connections are closed in both cases. | Service managers expect a clean exit on SIGTERM; one last binlog sync. | Yes | server binlog tests |

## Reference behaviors that differ from protocol.txt (we follow the implementation)

### Framing and parsing (proto)

1. A line ends only at the **first** `\r` in the buffer if it is immediately followed by `\n`; otherwise no line is found, even if a well-formed `\r\n` appears later. A bare `\n` never terminates a line.
2. Lines longer than 224 bytes are discarded in fixed 224-byte windows until the next `\r\n`, then answered with `BAD_FORMAT`. If the first `\r\n` lands inside what looks like the next command, that command is consumed as part of the bad line.
3. `pause-tube` has no trailing space in its command prefix, so `pause-tubefoo 5` parses as tube `foo`. It also skips leading spaces before the tube name. Since digits are valid name characters, a missing separator makes the name swallow the delay, giving `BAD_FORMAT`.
4. `use`, `watch`, `ignore` and `stats-tube` require the tube name to fill the rest of the line: no leading-space skip, no trailing garbage.
5. `kick` parses its bound with raw `strtoul`: skips any whitespace, accepts a sign (a negative value wraps to 64 bits, then truncates to u32), ignores trailing garbage, and fails only when there are no digits or on 64-bit overflow.
6. `reserve-with-timeout` never checks for trailing garbage after the timeout; bare `reserve` requires an exact match.
7. `quit` closes the connection as soon as the 4-byte prefix matches, with no check on the rest of the line.
8. `put`: `JOB_TOO_BIG` is decided from the parsed size before the trailing-garbage check, and the body is skipped. For a size within the limit, trailing garbage gives `BAD_FORMAT` and **no** body bytes are consumed; they are re-parsed as command lines, which can produce extra replies (e.g. `BAD_FORMAT` then `UNKNOWN_COMMAND`). In both cases `cmd-put` has already been incremented.
   The reference applies a put's side effects as soon as its command line parses, before any body byte arrives: `cmd-put` is counted and, unless the job is too big, the connection becomes a producer and a job id is allocated (so a connection that closes mid-body, or a body that fails with EXPECTED_CRLF, still consumes an id, and puts overlapping on two connections get ids in header order). The server's codec emits `Frame::PutStarted` at that point and the engine's `put_started` applies these effects exactly once; the completion arrives later as `Command::Put` or `Frame::PutRejected`. Covered by `put_rejections_side_effects.bt` and the `put_started_*` engine tests.
9. A NUL byte anywhere in a command line gives `BAD_FORMAT` before any command-specific parsing.
10. Numeric fields (`read_u32` / `read_u64`) skip only literal spaces, not tabs; overflow gives `BAD_FORMAT`.
11. `stats-tube` with no name falls through to the `stats` prefix and is rejected as `BAD_FORMAT`, not `UNKNOWN_COMMAND`.
12. `pause-tube` counts `cmd-pause-tube` once the name span and delay parse, before validating the name (leading `-`, over 200 bytes), then replies `BAD_FORMAT`. Parsed as `Command::PauseTubeBadName` so the engine can count it.

### State machine (engine)

1. A `put` rejected with `DRAINING` still consumes a job id (allocated when the put line parses).
2. Tubes are reference-counted by use + watch + jobs. `default` is never destroyed. Other empty, unreferenced tubes are destroyed immediately when the last reference goes away.
3. Tube lists, watch lists and wait queues use swap-remove, so `list-tubes` and `list-tubes-watched` order can change after `ignore` or tube destruction.
4. Waiting connections are taken round-robin (`ms_take`). With an even number of waiters drained without an intervening append, the service order is not FIFO (e.g. 4 waiters are served 1, 2, 4, 3).
5. When deciding whether to send `DEADLINE_SOON`, the reference checks only whether ready jobs exist and ignores tube pause. A reserve can therefore start waiting even though its own job is inside the safety margin; it then receives `DEADLINE_SOON` from the next tick. The server must call `tick(now)` after every `handle()`.
6. TTR expiry requires strictly passing the deadline (`<`); the safety margin uses `>=`.
7. `urgent` counts only ready jobs with pri < 1024.
8. The reference counts kick-job and reserve-job operations internally but never reports them in `stats`.
9. `pause-tube <tube> 0` converts the delay to nanoseconds before bumping 0 to the minimum, so it pauses for 1 ns and `stats-tube` shows `pause: 0`.
10. On disconnect the reference empties the watch list by repeatedly deleting item 0 (moving the last item into its place) and destroys unreferenced tubes in that order, which determines the later `list-tubes` order.
11. `reserve-with-timeout 0` replies `DEADLINE_SOON`, not `TIMED_OUT`, when the connection holds a job inside the safety margin and no ready job can be given to it (e.g. the only ready job is in a paused tube, or another waiter took it).

### Server CLI

1. `-z` is parsed with `sscanf("%zu")`: leading whitespace is skipped, `-1` wraps, values beyond u64 saturate, and the result is clamped to 1 GiB with a warning.

### Binlog (`-b`)

1. Journaled transitions: put, release with a delay > 0, bury, kick / kick-job (one record per job), delete. Not journaled: reserve, reserve-job, touch, release with delay 0, TTR timeout, delay expiry, pause-tube, use / watch, and jobs released by a disconnect.
2. Replay rebuilds each job from its last record: a job reserved at crash time comes back in that state (ready, delayed or buried) with that record's counters. A delayed job whose deadline passed during downtime comes back ready and still reports its `delay`.
3. Replaying a buried job adds one to its `buries` (on top of the last journaled value, so it does not accumulate across restarts).
4. Jobs are replayed in the order of their first record; this sets buried FIFO order after a restart. The tube list order results from replaying every record: a tube is appended at a job's full record and swap-removed when a delete record frees its last job.
5. After a restart the next job id is the highest id in any surviving record + 1. Cumulative server and tube counters (`cmd-*`, `total-jobs`, `cmd-pause-tube`) start at 0; tubes that were only used or watched are gone; pause and drain mode are not persisted.
6. Times are wall-clock based, so `age` and remaining delays continue across downtime.
7. The reference compacts only after a journaled write and only once there are 3 or more files; compaction moves count in `binlog-records-written`.
8. The reference has no SIGTERM handler (unless it is pid 1), so SIGTERM is an abrupt exit. beanstalkd-rs shuts down gracefully and fsyncs the binlog unless `-F`.
9. A second instance on the same binlog directory exits with status 10. `-s` is reported unrounded in `binlog-max-size`; files are preallocated to a multiple of 4096.
10. fsync uses `fdatasync`, like the reference; on macOS this is weaker than `F_FULLFSYNC`.

## beanstalkd-rs extensions (off by default)

None of these change behavior unless enabled in the configuration file; with no config file the server is byte-identical to the reference as described above.

1. **TLS / mTLS listeners**: the protocol is unchanged over TLS. The differential suites run every case over TLS against beanstalkd-rs. A TLS connection enters `stats` (`total-connections`, `current-connections`) only once its handshake completes; failed handshakes and rejected mTLS clients are never counted. A TLS terminator in front of the reference, such as stunnel, makes the reference count every accepted connection.
2. **Token authentication** (`auth = "token"` listeners only): `auth <token>\r\n` replies `AUTHENTICATED\r\n` or `UNAUTHORIZED\r\n` (then close). Before authentication any other input gets `UNAUTHORIZED` and a close. Unauthenticated connections are not counted in `stats`. On other listeners `auth` stays `UNKNOWN_COMMAND`.
3. **Pending-connection limits** (TLS listeners): handshake timeout 10 s, `auth.timeout`, and `server.max_pending_connections`; excess connections are closed at accept.
4. **HTTP monitoring**: `/healthz`, `/readyz`, `/metrics`, `/admin`; values match `stats` / `stats-tube`, and fetching them never changes counters such as `cmd-stats`.
