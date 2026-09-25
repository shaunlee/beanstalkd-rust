# Compatibility Differences vs. Reference beanstalkd

Reference: beanstalkd commit `25085c5`. Each entry states the observed behavior, the reason, whether it is intentional, and the covering test.

## Known differences

| # | Area | Difference | Reason | Intentional? | Test |
|---|---|---|---|---|---|
| D1 | engine | Reference: when a connection's own reserved job expires in the same event pass in which it is waiting, `process_queue` may re-reserve that job to the same connection, then the queued `RESERVED` reply is discarded and `DEADLINE_SOON` is sent instead. We recompute the DEADLINE_SOON / TIMED_OUT decision after draining expirations, using live state. | Reproducing it would require retracting an already-queued reply; practically unreachable with real client timing. | Yes | engine unit tests (tick) |
| D2 | engine / proto | `time-left` (stats-job) and `pause-time-left` (stats-tube) are signed in the reference and can be briefly negative (job overdue but not yet ticked). We clamp them to 0. | Stats types are unsigned; the window is sub-tick. Revisit if differential tests observe it. | Yes | — |

## Reference behaviors that differ from protocol.txt (we follow the implementation)

### Framing and parsing (proto)

1. A line ends only at the **first** `\r` in the buffer if it is immediately followed by `\n`; otherwise no line is found, even if a well-formed `\r\n` appears later. A bare `\n` never terminates a line.
2. Lines longer than 224 bytes are discarded in fixed 224-byte windows until the next `\r\n`, then answered with `BAD_FORMAT`. If the first `\r\n` lands inside what looks like the next command, that command is consumed as part of the bad line.
3. `pause-tube` has no trailing space in its command prefix, so `pause-tubefoo 5` parses as tube `foo`. It also skips leading spaces before the tube name. Since digits are valid name characters, a missing separator makes the name swallow the delay, giving `BAD_FORMAT`.
4. `use`, `watch`, `ignore` and `stats-tube` require the tube name to fill the rest of the line: no leading-space skip, no trailing garbage.
5. `kick` parses its bound with raw `strtoul`: skips any whitespace, accepts a sign (a negative value wraps to 64 bits, then truncates to u32), ignores trailing garbage, and fails only when there are no digits or on 64-bit overflow.
6. `reserve-with-timeout` never checks for trailing garbage after the timeout; bare `reserve` requires an exact match.
7. `quit` closes the connection as soon as the 4-byte prefix matches, with no check on the rest of the line.
8. `put`: `JOB_TOO_BIG` is decided from the parsed size before the trailing-garbage check, and the body is skipped. For a size within the limit, trailing garbage gives `BAD_FORMAT` and **no** body bytes are consumed; they are re-parsed as command lines, which can produce extra replies (e.g. `BAD_FORMAT` then `UNKNOWN_COMMAND`).
9. A NUL byte anywhere in a command line gives `BAD_FORMAT` before any command-specific parsing.
10. Numeric fields (`read_u32` / `read_u64`) skip only literal spaces, not tabs; overflow gives `BAD_FORMAT`.
11. `stats-tube` with no name falls through to the `stats` prefix and is rejected as `BAD_FORMAT`, not `UNKNOWN_COMMAND`.

### State machine (engine)

1. A `put` rejected with `DRAINING` still consumes a job id.
2. Tubes are reference-counted by use + watch + jobs. `default` is never destroyed. Other empty, unreferenced tubes are destroyed immediately when the last reference goes away.
3. Tube lists, watch lists and wait queues use swap-remove, so `list-tubes` and `list-tubes-watched` order can change after `ignore` or tube destruction.
4. Waiting connections are taken round-robin (`ms_take`). With an even number of waiters drained without an intervening append, the service order is not FIFO (e.g. 4 waiters are served 1, 2, 4, 3).
5. When deciding whether to send `DEADLINE_SOON`, the reference checks only whether ready jobs exist and ignores tube pause. A reserve can therefore start waiting even though its own job is inside the safety margin; it then receives `DEADLINE_SOON` from the next tick. The server must call `tick(now)` after every `handle()`.
6. TTR expiry requires strictly passing the deadline (`<`); the safety margin uses `>=`.
7. `urgent` counts only ready jobs with pri < 1024.
8. The reference counts kick-job and reserve-job operations internally but never reports them in `stats`.
