/*
 * pidrusage: prints a target process's RSS, physical footprint and I/O
 * byte counters as one CSV-ish line, for footprint benchmarks (P4-T4).
 *
 * macOS has no per-process write-byte counter in `ps`, `/usr/bin/time -l`
 * or `getrusage`; `proc_pid_rusage(pid, RUSAGE_INFO_V4, ...)` is the
 * smallest supported way to read `ri_diskio_bytesread` /
 * `ri_diskio_byteswritten` (actual block I/O) and `ri_logical_writes`
 * (bytes passed to write(2)/pwrite(2), before the page cache / writeback),
 * which is what bench/footprint.py uses to derive binlog bytes per
 * operation. This is a read-only diagnostic, not part of the server, so
 * it lives under bench/ rather than in a Rust crate (the repo's no-unsafe
 * rule is Rust-only; see docs/PLAN.md P4-T4).
 *
 * Usage: pidrusage <pid>
 * Output: rss=<bytes> phys_footprint=<bytes> logical_writes=<bytes>
 *         diskio_written=<bytes> diskio_read=<bytes>
 * Exit status: 0 on success, 1 if the pid is gone or rusage is unavailable.
 */
#include <errno.h>
#include <libproc.h>
#include <stdio.h>
#include <stdlib.h>

int main(int argc, char **argv) {
    if (argc != 2) {
        fprintf(stderr, "usage: %s <pid>\n", argv[0]);
        return 2;
    }
    pid_t pid = (pid_t)strtol(argv[1], NULL, 10);
    struct rusage_info_v4 ri;
    int rc = proc_pid_rusage(pid, RUSAGE_INFO_V4, (rusage_info_t *)&ri);
    if (rc != 0) {
        fprintf(stderr, "pidrusage: proc_pid_rusage(%d): errno=%d\n", pid, errno);
        return 1;
    }
    printf(
        "rss=%llu phys_footprint=%llu logical_writes=%llu diskio_written=%llu diskio_read=%llu\n",
        (unsigned long long)ri.ri_resident_size,
        (unsigned long long)ri.ri_phys_footprint,
        (unsigned long long)ri.ri_logical_writes,
        (unsigned long long)ri.ri_diskio_byteswritten,
        (unsigned long long)ri.ri_diskio_bytesread);
    return 0;
}
