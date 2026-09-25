//! beanstalkd wire protocol: command parsing, response encoding, stats YAML.
//!
//! INTERFACE CONTRACT (owned by the lead): the public types in this file
//! (`Command`, `Response`, `Frame`, `TubeName`, stats structs) and the public
//! function signatures must not change without lead approval. Implementations
//! live in the submodules.
//!
//! Ground truth for all behavior is `.ref/beanstalkd/prot.c`.

use bytes::Bytes;

mod codec;
mod parse;
mod response;
mod stats;

pub use codec::ServerCodec;
pub use parse::parse_line;
pub use stats::{StatsJob, StatsServer, StatsTube, yaml_list};

/// Maximum tube name length in bytes (`MAX_TUBE_NAME_LEN - 1`).
pub const MAX_TUBE_NAME_LEN: usize = 200;
/// Maximum command line length including `\r\n` (`LINE_BUF_SIZE`).
pub const LINE_BUF_SIZE: usize = 224;
/// Default `-z` value (`JOB_DATA_SIZE_LIMIT_DEFAULT`).
pub const DEFAULT_MAX_JOB_SIZE: u32 = (1 << 16) - 1;
/// Upper bound of `-z` (`JOB_DATA_SIZE_LIMIT_MAX`).
pub const MAX_JOB_SIZE_LIMIT: u32 = 1_073_741_824;
/// Jobs with pri below this count as "urgent" in stats.
pub const URGENT_THRESHOLD: u32 = 1024;

pub type JobId = u64;

/// A validated tube name (1..=200 bytes of `NAME_CHARS`, not starting with '-').
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TubeName(String);

impl TubeName {
    /// Validates per `is_valid_tube` in prot.c. Returns `None` if invalid.
    pub fn new(name: &str) -> Option<Self> {
        parse::validate_tube_name(name).map(|()| TubeName(name.to_owned()))
    }

    pub fn default_tube() -> Self {
        TubeName("default".to_owned())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for TubeName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// A fully-decoded client command. Durations (`delay`, `ttr`, timeouts) are
/// in whole seconds exactly as sent on the wire; `ttr == 0` is NOT adjusted
/// here (the engine bumps it to 1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Put {
        pri: u32,
        delay: u32,
        ttr: u32,
        body: Bytes,
    },
    Use(TubeName),
    Reserve,
    ReserveWithTimeout(u32),
    ReserveJob(JobId),
    Delete(JobId),
    Release {
        id: JobId,
        pri: u32,
        delay: u32,
    },
    Bury {
        id: JobId,
        pri: u32,
    },
    Touch(JobId),
    Watch(TubeName),
    Ignore(TubeName),
    Peek(JobId),
    PeekReady,
    PeekDelayed,
    PeekBuried,
    Kick(u32),
    KickJob(JobId),
    StatsJob(JobId),
    StatsTube(TubeName),
    Stats,
    ListTubes,
    ListTubeUsed,
    ListTubesWatched,
    Quit,
    PauseTube {
        tube: TubeName,
        delay: u32,
    },
}

/// A server reply. `encode` must produce byte-identical output to the
/// reference implementation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response {
    /// `INSERTED <id>`
    Inserted(JobId),
    /// `BURIED <id>` (put that could not be enqueued)
    BuriedId(JobId),
    /// `BURIED`
    Buried,
    /// `USING <tube>`
    Using(TubeName),
    /// `RESERVED <id> <bytes>\r\n<data>`
    Reserved {
        id: JobId,
        body: Bytes,
    },
    /// `FOUND <id> <bytes>\r\n<data>`
    Found {
        id: JobId,
        body: Bytes,
    },
    /// `WATCHING <count>`
    Watching(u64),
    /// `KICKED <count>` (kick)
    Kicked(u64),
    /// `KICKED` (kick-job)
    KickedJob,
    /// `OK <bytes>\r\n<data>` — stats / list payloads (YAML)
    Ok(Bytes),
    DeadlineSoon,
    TimedOut,
    Deleted,
    Released,
    Touched,
    NotFound,
    NotIgnored,
    Paused,
    ExpectedCrlf,
    JobTooBig,
    Draining,
    OutOfMemory,
    InternalError,
    BadFormat,
    UnknownCommand,
}

/// Output of `ServerCodec` decoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    /// A well-formed command ready for the engine.
    Command(Command),
    /// A protocol-level error the connection must reply with directly
    /// (BAD_FORMAT, UNKNOWN_COMMAND, JOB_TOO_BIG, EXPECTED_CRLF, ...).
    /// The connection stays open.
    Error(Response),
}
