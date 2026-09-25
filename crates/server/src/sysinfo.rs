//! `SysInfo` implementation: process facts the engine cannot know itself
//! (it does no I/O and reads no clock). Mirrors what `fmt_stats` in prot.c
//! reads via `getrusage`, `uname` and the startup-time random instance id.

use bstk_engine::{SysInfo, SysSnapshot};

/// Facts fixed for the lifetime of the process, captured once at startup:
/// pid, version, random instance id, and `uname()` fields. `getrusage` is
/// re-read on every snapshot, matching the reference calling it fresh in
/// `fmt_stats`.
pub struct ProcessSysInfo {
    pid: u64,
    version: String,
    id: String,
    hostname: String,
    os: String,
    platform: String,
}

impl ProcessSysInfo {
    /// Captures the fixed facts. Best-effort: if `uname()` or the random
    /// source fails (practically never), falls back to empty/zeroed values
    /// rather than crashing the server.
    pub fn collect() -> Self {
        let (hostname, os, platform) = match nix::sys::utsname::uname() {
            Ok(uts) => (
                uts.nodename().to_string_lossy().into_owned(),
                uts.version().to_string_lossy().into_owned(),
                uts.machine().to_string_lossy().into_owned(),
            ),
            Err(e) => {
                tracing::warn!("uname() failed: {e}");
                (String::new(), String::new(), String::new())
            }
        };

        ProcessSysInfo {
            pid: u64::from(std::process::id()),
            version: env!("CARGO_PKG_VERSION").to_owned(),
            id: random_instance_id(),
            hostname,
            os,
            platform,
        }
    }
}

/// 8 random bytes hex-encoded to 16 lowercase characters, matching
/// `instance_hex` (`enum { instance_id_bytes = 8 }`) in prot.c.
fn random_instance_id() -> String {
    let mut buf = [0u8; 8];
    if let Err(e) = getrandom::getrandom(&mut buf) {
        tracing::warn!("getrandom() failed, using a fixed instance id: {e}");
    }
    buf.iter().map(|b| format!("{b:02x}")).collect()
}

impl SysInfo for ProcessSysInfo {
    fn snapshot(&self) -> SysSnapshot {
        let (utime, stime) = read_rusage();
        SysSnapshot {
            pid: self.pid,
            version: self.version.clone(),
            rusage_utime: utime,
            rusage_stime: stime,
            id: self.id.clone(),
            hostname: self.hostname.clone(),
            os: self.os.clone(),
            platform: self.platform.clone(),
        }
    }
}

/// Reads `getrusage(RUSAGE_SELF)` and returns `(utime, stime)` each as
/// `(seconds, microseconds)`, matching `(int) ru.ru_utime.tv_sec, (int)
/// ru.ru_utime.tv_usec` in `fmt_stats`.
fn read_rusage() -> ((u64, u64), (u64, u64)) {
    match nix::sys::resource::getrusage(nix::sys::resource::UsageWho::RUSAGE_SELF) {
        Ok(ru) => {
            let utime = ru.user_time();
            let stime = ru.system_time();
            (
                (
                    tv_sec_as_u64(utime.tv_sec()),
                    tv_usec_as_u64(utime.tv_usec()),
                ),
                (
                    tv_sec_as_u64(stime.tv_sec()),
                    tv_usec_as_u64(stime.tv_usec()),
                ),
            )
        }
        Err(e) => {
            tracing::warn!("getrusage() failed: {e}");
            ((0, 0), (0, 0))
        }
    }
}

fn tv_sec_as_u64(v: nix::libc::time_t) -> u64 {
    u64::try_from(v).unwrap_or(0)
}

fn tv_usec_as_u64(v: nix::libc::suseconds_t) -> u64 {
    u64::try_from(v).unwrap_or(0)
}

/// Best-effort: raises the soft `RLIMIT_NOFILE` limit to the hard limit, so
/// the server (and the 1,000-connection test) doesn't run out of file
/// descriptors. Never fails startup; logs at debug on any error.
pub fn raise_nofile_limit() {
    use nix::sys::resource::{Resource, getrlimit, setrlimit};

    match getrlimit(Resource::RLIMIT_NOFILE) {
        Ok((soft, hard)) => {
            if hard > soft
                && let Err(e) = setrlimit(Resource::RLIMIT_NOFILE, hard, hard)
            {
                tracing::debug!("could not raise RLIMIT_NOFILE from {soft} to {hard}: {e}");
            }
        }
        Err(e) => tracing::debug!("getrlimit(RLIMIT_NOFILE) failed: {e}"),
    }
}
