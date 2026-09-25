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
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(try_from = "String", into = "String")]
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

impl TryFrom<String> for TubeName {
    type Error = String;

    /// Validating conversion, used when deserializing (P3 log entries).
    fn try_from(name: String) -> Result<Self, String> {
        match parse::validate_tube_name(&name) {
            Some(()) => Ok(TubeName(name)),
            None => Err(format!("invalid tube name {name:?}")),
        }
    }
}

impl From<TubeName> for String {
    fn from(name: TubeName) -> String {
        name.0
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
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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
    /// `pause-tube` whose tube-name span and delay both parsed, but whose
    /// name then failed validation (leading `-`, or longer than 200 bytes).
    /// prot.c has already counted `cmd-pause-tube` at that point, so this
    /// must reach the engine, which counts it and replies `BAD_FORMAT`.
    PauseTubeBadName,
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
    /// `AUTHENTICATED` — token accepted (beanstalkd-rs extension).
    Authenticated,
    /// `UNAUTHORIZED` — wrong token, or a command before authentication
    /// (beanstalkd-rs extension); the connection is then closed.
    Unauthorized,
}

/// Output of `ServerCodec` decoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    /// A well-formed command ready for the engine.
    Command(Command),
    /// A `put` whose numeric fields parsed but which was rejected during
    /// framing. It must still go to the engine: the reference counts it in
    /// `cmd-put` (and, for `ExpectedCrlf`, marks the connection as a producer
    /// and consumes a job id) before rejecting it. The engine emits the reply.
    PutRejected(PutRejection),
    /// A `put` command line was accepted and the codec is now reading its
    /// body (or, when `too_big`, discarding it). Emitted only by a codec
    /// built with [`ServerCodec::emit_put_started`]. prot.c applies a put's
    /// header-time side effects right here, before any body byte arrives:
    /// `cmd-put` is counted and, unless `too_big`, the connection becomes a
    /// producer and a job id is allocated. On the same connection it is
    /// followed by exactly one completion frame (`Command(Put)`,
    /// `PutRejected(ExpectedCrlf)` or `PutRejected(JobTooBig)`), unless the
    /// connection closes first. No reply.
    PutStarted { too_big: bool },
    /// `auth <token>` (everything after the single space, verbatim). Only
    /// produced by a codec built with [`ServerCodec::recognize_auth`]; never
    /// sent to the engine. Tokens are limited by the 224-byte line length.
    Auth(Bytes),
    /// A protocol-level error with no engine side effects (BAD_FORMAT,
    /// UNKNOWN_COMMAND, ...). The connection replies with it directly and
    /// stays open.
    Error(Response),
}

/// Why a `put` was rejected. The first three come from the codec; replies:
/// `JobTooBig` → `JOB_TOO_BIG`, `TrailingGarbage` → `BAD_FORMAT`,
/// `ExpectedCrlf` → `EXPECTED_CRLF`. `OutOfMemory` comes from the server
/// when binlog space for the job cannot be reserved (`OUT_OF_MEMORY`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum PutRejection {
    /// Body size above `max_job_size`; the body has been discarded.
    JobTooBig,
    /// Garbage after the size field; no body bytes were consumed.
    TrailingGarbage,
    /// The body was not followed by `\r\n`.
    ExpectedCrlf,
    /// The body was complete but binlog space could not be reserved
    /// (`walresvput` failing in prot.c).
    OutOfMemory,
}

impl PutRejection {
    /// The reply the client receives for this rejection.
    pub fn response(self) -> Response {
        match self {
            PutRejection::JobTooBig => Response::JobTooBig,
            PutRejection::TrailingGarbage => Response::BadFormat,
            PutRejection::ExpectedCrlf => Response::ExpectedCrlf,
            PutRejection::OutOfMemory => Response::OutOfMemory,
        }
    }
}
