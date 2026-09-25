//! TOML configuration file (`--config PATH`) and its resolution against
//! the command line (docs/PLAN.md §5.3).
//!
//! The file is optional; without it the resolved configuration is exactly
//! what the command line alone gives today (one plaintext listener from
//! `-l` / `-p`, no TLS, no token auth, no HTTP listener). With it:
//!
//! - Every key is optional and unknown keys are errors at every level.
//! - Command-line flags override the file for `-z`, `-b`, `-f` / `-F`,
//!   `-s`, and `-V` (which can only raise the log level). File values are
//!   still validated when a flag overrides them.
//! - `-l` / `-p` apply only when the file defines no `[[listener]]`;
//!   giving them together with `[[listener]]` entries is an error.
//! - Relative paths in the file (`tls.*`, `auth.tokens_file`,
//!   `binlog.dir`) are relative to the file's directory; `-b` stays
//!   relative to the working directory, as today.
//!
//! Loading is split in two: `FileConfig::load` does all the I/O (the
//! configuration file and `auth.tokens_file`), and `resolve` is a pure
//! function from the command line and the loaded file to a validated
//! `ResolvedConfig`. Certificates are not read here, only their paths.
//!
//! Tokens are secrets: they never appear in `Debug` output, error
//! messages or the `--check-config` summary (only their count and the
//! position of an invalid one).

use std::fmt;
use std::io::{self, Write};
use std::net::SocketAddr;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;
use serde::de::{self, Deserializer, SeqAccess, Visitor};

use bstk_proto::{LINE_BUF_SIZE, MAX_JOB_SIZE_LIMIT};
use bstk_store::{SyncPolicy, WalOptions};

use crate::cli::Cli;

/// Default cap on the number of per-tube metric series (`http.max_tube_series`).
pub const DEFAULT_MAX_TUBE_SERIES: usize = 1000;

/// Default `auth.timeout`: time an `auth = "token"` connection has, after
/// its TLS handshake, to authenticate.
pub const DEFAULT_AUTH_TIMEOUT: Duration = Duration::from_secs(10);

/// Default `server.max_pending_connections`: connections still in their
/// TLS handshake or awaiting token authentication, across all listeners.
pub const DEFAULT_MAX_PENDING_CONNECTIONS: usize = 1024;

/// Default `http.snapshot_min_interval`: how old a cached engine snapshot
/// served by `/metrics` and `/admin` may be.
pub const DEFAULT_SNAPSHOT_MIN_INTERVAL: Duration = Duration::from_secs(1);

/// Longest token that fits a protocol line: `auth <token>\r\n` must fit in
/// `LINE_BUF_SIZE` (224) bytes, so 224 - 5 - 2 = 217.
pub const MAX_TOKEN_LEN: usize = LINE_BUF_SIZE - "auth ".len() - "\r\n".len();

/// Largest `binlog.file_size` (the limit `Wal::open` enforces; `-s` is only
/// checked there).
pub const MAX_BINLOG_FILE_SIZE: u64 = bstk_store::MAX_FILE_SIZE;

/// Exit status of `--check-config` for an invalid configuration.
pub const EXIT_CONFIG: u8 = 1;

// ---------------------------------------------------------------------------
// Resolved configuration (what the server consumes)
// ---------------------------------------------------------------------------

/// Everything the server needs, validated and with CLI precedence applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedConfig {
    /// The configuration file this came from, if any.
    pub source: Option<PathBuf>,
    /// Listening sockets, in file order (never empty).
    pub listeners: Vec<Listener>,
    /// Certificate files; `Some` iff `[tls] cert` and `key` are set.
    pub tls: Option<TlsFiles>,
    /// Accepted tokens for `auth = "token"` listeners (deduplicated, in
    /// configuration order: `auth.tokens` first, then `auth.tokens_file`).
    pub tokens: Tokens,
    /// `auth.timeout`: an `auth = "token"` connection that has not
    /// authenticated this long after its TLS handshake is closed.
    pub auth_timeout: Duration,
    /// `-z` / `server.max_job_size`.
    pub max_job_size: u32,
    /// `server.max_pending_connections`: cap on TLS connections still in
    /// their handshake or awaiting token authentication (at least 1).
    pub max_pending_connections: usize,
    pub binlog: BinlogSettings,
    /// `None`: no HTTP listener.
    pub http: Option<HttpSettings>,
    pub log: LogSettings,
    /// Non-fatal problems found while loading (e.g. a tokens file readable
    /// by other users); logged at startup and printed by `--check-config`.
    pub warnings: Vec<String>,
}

/// One listening socket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Listener {
    pub addr: SocketAddr,
    /// Serve TLS (with `ResolvedConfig::tls`) instead of plaintext.
    pub tls: bool,
    pub auth: AuthMode,
}

/// How a listener authenticates clients.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AuthMode {
    /// No authentication (the protocol is unchanged).
    #[default]
    None,
    /// `auth <token>` before any other command (TLS listeners only).
    Token,
    /// A client certificate signed by `tls.client_ca` (TLS listeners only).
    Mtls,
}

impl AuthMode {
    fn as_str(self) -> &'static str {
        match self {
            AuthMode::None => "none",
            AuthMode::Token => "token",
            AuthMode::Mtls => "mtls",
        }
    }
}

/// PEM files for TLS listeners (paths only; P2-T4 loads them).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsFiles {
    /// Certificate chain.
    pub cert: PathBuf,
    /// Private key.
    pub key: PathBuf,
    /// CA bundle for client certificates; set iff some listener uses
    /// `auth = "mtls"`.
    pub client_ca: Option<PathBuf>,
}

/// Write-ahead log settings, in the types `Wal::open` takes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinlogSettings {
    /// `-b` / `binlog.dir`; `None` disables the write-ahead log.
    pub dir: Option<PathBuf>,
    /// `-s` / `binlog.file_size` (reported by `stats` even without a dir).
    pub file_size: u64,
    /// `-f` / `-F` / `binlog.fsync`.
    pub sync: SyncPolicy,
}

impl BinlogSettings {
    /// The options for `Wal::open`, or `None` without a binlog directory.
    pub fn wal_options(&self) -> Option<WalOptions> {
        self.dir.as_ref().map(|dir| WalOptions {
            dir: dir.clone(),
            file_size: self.file_size,
            sync: self.sync,
        })
    }
}

/// The opt-in HTTP listener (`/metrics`, `/healthz`, `/readyz`, `/admin`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HttpSettings {
    pub addr: SocketAddr,
    /// Cap on per-tube metric series (and on the tubes `/admin` lists).
    pub max_tube_series: usize,
    /// Longest time a snapshot is reused by `/metrics` and `/admin`
    /// (zero: never reused).
    pub snapshot_min_interval: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogSettings {
    pub level: LogLevel,
    pub format: LogFormat,
}

/// Log level, ordered from least to most verbose.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl LogLevel {
    /// The level for a `-V` count, as `cli::tracing_level`.
    pub fn from_verbosity(verbose: u8) -> LogLevel {
        match verbose {
            0 => LogLevel::Warn,
            1 => LogLevel::Info,
            2 => LogLevel::Debug,
            _ => LogLevel::Trace,
        }
    }

    pub fn to_tracing(self) -> tracing::Level {
        match self {
            LogLevel::Error => tracing::Level::ERROR,
            LogLevel::Warn => tracing::Level::WARN,
            LogLevel::Info => tracing::Level::INFO,
            LogLevel::Debug => tracing::Level::DEBUG,
            LogLevel::Trace => tracing::Level::TRACE,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            LogLevel::Error => "error",
            LogLevel::Warn => "warn",
            LogLevel::Info => "info",
            LogLevel::Debug => "debug",
            LogLevel::Trace => "trace",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    /// Human-readable lines (the default, as today).
    #[default]
    Text,
    /// One JSON object per line.
    Json,
}

impl LogFormat {
    fn as_str(self) -> &'static str {
        match self {
            LogFormat::Text => "text",
            LogFormat::Json => "json",
        }
    }
}

/// Authentication tokens. `Debug` shows only how many there are.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct Tokens(Vec<String>);

impl Tokens {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The tokens as bytes, for (constant-time) comparison with the
    /// argument of an `auth` command.
    pub fn iter(&self) -> impl Iterator<Item = &[u8]> {
        self.0.iter().map(String::as_bytes)
    }
}

impl fmt::Debug for Tokens {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Tokens(<{} redacted>)", self.0.len())
    }
}

/// `auth.tokens` accepts only an array of strings. Hand-written so that a
/// mistyped value (say, `tokens = "secret"`) is not echoed in the error.
impl<'de> Deserialize<'de> for Tokens {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Tokens, D::Error> {
        struct TokensVisitor;

        impl<'de> Visitor<'de> for TokensVisitor {
            type Value = Tokens;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("an array of token strings")
            }

            fn visit_str<E: de::Error>(self, _: &str) -> Result<Tokens, E> {
                Err(E::custom(
                    "invalid type: string, expected an array of token strings",
                ))
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Tokens, A::Error> {
                let mut tokens = Vec::new();
                while let Some(token) = seq.next_element::<String>()? {
                    tokens.push(token);
                }
                Ok(Tokens(tokens))
            }
        }

        deserializer.deserialize_seq(TokensVisitor)
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Why the configuration cannot be used. Messages never contain tokens.
#[derive(Debug)]
pub enum ConfigError {
    /// A file (the configuration or `auth.tokens_file`) cannot be read.
    Read { path: PathBuf, source: io::Error },
    /// The configuration file is not valid TOML for the schema (syntax,
    /// wrong type, unknown key).
    Parse {
        path: PathBuf,
        line: usize,
        column: usize,
        message: String,
    },
    /// A semantic error; the message names the offending key.
    Invalid(String),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::Read { path, source } => {
                write!(f, "cannot read {}: {source}", path.display())
            }
            ConfigError::Parse {
                path,
                line,
                column,
                message,
            } => write!(f, "{}:{line}:{column}: {message}", path.display()),
            ConfigError::Invalid(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for ConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ConfigError::Read { source, .. } => Some(source),
            _ => None,
        }
    }
}

fn invalid(message: impl Into<String>) -> ConfigError {
    ConfigError::Invalid(message.into())
}

// ---------------------------------------------------------------------------
// The file as written
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawFile {
    server: Option<RawServer>,
    #[serde(default, rename = "listener")]
    listeners: Vec<RawListener>,
    tls: Option<RawTls>,
    auth: Option<RawAuth>,
    binlog: Option<RawBinlog>,
    http: Option<RawHttp>,
    log: Option<RawLog>,
    cluster: Option<RawCluster>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawServer {
    max_job_size: Option<i64>,
    max_pending_connections: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawListener {
    addr: String,
    #[serde(default)]
    tls: bool,
    #[serde(default)]
    auth: AuthMode,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawTls {
    cert: Option<PathBuf>,
    key: Option<PathBuf>,
    client_ca: Option<PathBuf>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawAuth {
    tokens: Option<Tokens>,
    tokens_file: Option<PathBuf>,
    timeout: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawBinlog {
    dir: Option<PathBuf>,
    fsync: Option<String>,
    file_size: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawHttp {
    addr: Option<String>,
    max_tube_series: Option<i64>,
    snapshot_min_interval: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawLog {
    level: Option<LogLevel>,
    format: Option<LogFormat>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawCluster {
    node_id: Option<i64>,
    listen: Option<String>,
    data_dir: Option<PathBuf>,
    node_timeout: Option<String>,
    snapshot_every: Option<i64>,
    heartbeat: Option<String>,
    election_timeout: Option<Vec<String>>,
    insecure_plaintext: Option<bool>,
    tls: Option<RawClusterTls>,
    #[serde(default, rename = "peer")]
    peers: Vec<RawPeer>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawClusterTls {
    cert: Option<PathBuf>,
    key: Option<PathBuf>,
    ca: Option<PathBuf>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPeer {
    id: i64,
    addr: String,
}

/// The contents of `auth.tokens_file`. `Debug` hides them.
struct TokensFile {
    path: PathBuf,
    contents: String,
    /// Why its permissions are too open, if they are.
    mode_warning: Option<String>,
}

impl fmt::Debug for TokensFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TokensFile")
            .field("path", &self.path)
            .field("contents", &"<redacted>")
            .field("mode_warning", &self.mode_warning)
            .finish()
    }
}

/// A parsed (not yet validated) configuration file, with the contents of
/// its `auth.tokens_file` once loaded.
#[derive(Debug)]
pub struct FileConfig {
    path: PathBuf,
    raw: RawFile,
    tokens_file: Option<TokensFile>,
}

impl FileConfig {
    /// Reads and parses the file at `path`, then reads its
    /// `auth.tokens_file` (if any).
    pub fn load(path: &Path) -> Result<FileConfig, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        let mut file = FileConfig::parse(&text, path)?;
        if let Some(name) = file.raw.auth.as_ref().and_then(|a| a.tokens_file.as_ref()) {
            let tokens_path = file.relative(name);
            let contents =
                std::fs::read_to_string(&tokens_path).map_err(|source| ConfigError::Read {
                    path: tokens_path.clone(),
                    source,
                })?;
            let mode_warning = std::fs::metadata(&tokens_path)
                .ok()
                .and_then(|m| tokens_file_mode_warning(&tokens_path, m.permissions().mode()));
            file.tokens_file = Some(TokensFile {
                path: tokens_path,
                contents,
                mode_warning,
            });
        }
        Ok(file)
    }

    /// Parses `text` as the file at `path` (which locates relative paths
    /// and names the file in errors) without reading anything else;
    /// `load` also reads `auth.tokens_file`.
    pub fn parse(text: &str, path: &Path) -> Result<FileConfig, ConfigError> {
        let raw: RawFile = toml::from_str(text).map_err(|e| {
            // `e`'s Display quotes the offending source line, which may
            // hold a token: report its position and message only.
            let (line, column) = e
                .span()
                .map_or((1, 1), |span| line_column(text, span.start));
            ConfigError::Parse {
                path: path.to_path_buf(),
                line,
                column,
                message: e.message().trim_end().to_owned(),
            }
        })?;
        Ok(FileConfig {
            path: path.to_path_buf(),
            raw,
            tokens_file: None,
        })
    }

    /// `name` relative to the file's directory (absolute paths unchanged).
    fn relative(&self, name: &Path) -> PathBuf {
        self.path.parent().unwrap_or(Path::new("")).join(name)
    }
}

/// A warning if a tokens file with Unix permission bits `mode` is
/// accessible to its group or to others (like sshd's check of private
/// keys). Only a warning: the file is still used.
pub fn tokens_file_mode_warning(path: &Path, mode: u32) -> Option<String> {
    let mode = mode & 0o7777;
    (mode & 0o077 != 0).then(|| {
        format!(
            "auth.tokens_file {} is accessible by group or others (mode {mode:04o}); \
             restrict it with chmod 600",
            path.display()
        )
    })
}

/// 1-based line and column (in characters) of byte `offset` in `text`.
fn line_column(text: &str, offset: usize) -> (usize, usize) {
    let before = text.get(..offset).unwrap_or(text);
    let line = before.matches('\n').count() + 1;
    let line_start = before.rfind('\n').map_or(0, |i| i + 1);
    let column = before[line_start..].chars().count() + 1;
    (line, column)
}

// ---------------------------------------------------------------------------
// Resolution
// ---------------------------------------------------------------------------

/// Loads `--config` (if given) and resolves it against the command line,
/// ignoring `[cluster]` (the server uses `load_all`).
#[cfg(test)]
pub fn load(cli: &Cli) -> Result<ResolvedConfig, ConfigError> {
    let file = cli.config.as_deref().map(FileConfig::load).transpose()?;
    resolve(cli, file)
}

/// Validates `file` and merges it with the command line (flags win).
/// Without a file the result is exactly what the command line alone
/// gives. Pure: all I/O happened in `FileConfig::load`.
pub fn resolve(cli: &Cli, file: Option<FileConfig>) -> Result<ResolvedConfig, ConfigError> {
    let Some(file) = file else {
        return Ok(from_cli(cli));
    };
    let raw = &file.raw;

    // [server]
    let file_max_job_size = raw
        .server
        .as_ref()
        .and_then(|s| s.max_job_size)
        .map(|v| {
            u32::try_from(v)
                .ok()
                .filter(|&v| v <= MAX_JOB_SIZE_LIMIT)
                .ok_or_else(|| {
                    invalid(format!(
                        "server.max_job_size = {v}: must be between 0 and {MAX_JOB_SIZE_LIMIT}"
                    ))
                })
        })
        .transpose()?;
    let max_job_size = match file_max_job_size {
        Some(v) if !cli.given.max_job_size => v,
        _ => cli.max_job_size,
    };
    let max_pending_connections = match raw.server.as_ref().and_then(|s| s.max_pending_connections)
    {
        None => DEFAULT_MAX_PENDING_CONNECTIONS,
        Some(v) => usize::try_from(v).ok().filter(|&v| v >= 1).ok_or_else(|| {
            invalid(format!(
                "server.max_pending_connections = {v}: must be at least 1"
            ))
        })?,
    };

    // [binlog]
    let binlog = raw.binlog.as_ref();
    let file_size = binlog
        .and_then(|b| b.file_size)
        .map(|v| {
            u64::try_from(v)
                .ok()
                .filter(|&v| v <= MAX_BINLOG_FILE_SIZE)
                .ok_or_else(|| {
                    invalid(format!(
                        "binlog.file_size = {v}: must be between 0 and {MAX_BINLOG_FILE_SIZE}"
                    ))
                })
        })
        .transpose()?;
    let file_sync = binlog
        .and_then(|b| b.fsync.as_deref())
        .map(|s| {
            parse_fsync(s).ok_or_else(|| {
                invalid(format!(
                    "binlog.fsync = {s:?}: expected \"always\", \"never\" or an interval \
                     such as \"50ms\" or \"1s\""
                ))
            })
        })
        .transpose()?;
    let binlog = BinlogSettings {
        dir: cli.binlog_dir.clone().or_else(|| {
            binlog
                .and_then(|b| b.dir.as_deref())
                .map(|d| file.relative(d))
        }),
        file_size: match file_size {
            Some(v) if !cli.given.binlog_file_size => v,
            _ => cli.binlog_file_size,
        },
        sync: match file_sync {
            Some(s) if !cli.given.sync => s,
            _ => cli.sync,
        },
    };

    // [log]
    let file_level = raw.log.as_ref().and_then(|l| l.level);
    let level = match (file_level, cli.verbose) {
        (Some(level), 0) => level,
        (Some(level), v) => level.max(LogLevel::from_verbosity(v)),
        (None, v) => LogLevel::from_verbosity(v),
    };
    let log = LogSettings {
        level,
        format: raw.log.as_ref().and_then(|l| l.format).unwrap_or_default(),
    };

    // [[listener]]
    let listeners = if raw.listeners.is_empty() {
        vec![cli_listener(cli)]
    } else {
        if cli.given.listen_addr || cli.given.port {
            return Err(invalid(format!(
                "-l / -p cannot be used when {} defines [[listener]] entries: \
                 put every listen address in the configuration file, or remove \
                 its [[listener]] entries to listen on -l / -p",
                file.path.display()
            )));
        }
        raw.listeners
            .iter()
            .enumerate()
            .map(|(i, l)| {
                Ok(Listener {
                    addr: parse_addr(&l.addr, &format!("listener[{i}].addr"))?,
                    tls: l.tls,
                    auth: l.auth,
                })
            })
            .collect::<Result<Vec<_>, ConfigError>>()?
    };
    for (i, a) in listeners.iter().enumerate() {
        if let Some((j, b)) = listeners[..i]
            .iter()
            .enumerate()
            .find(|(_, b)| overlaps(a.addr, b.addr))
        {
            return Err(invalid(format!(
                "listener[{i}].addr = \"{}\" conflicts with listener[{j}].addr = \"{}\" \
                 (same port on the same or a wildcard address)",
                a.addr, b.addr
            )));
        }
    }

    // [tls]
    let tls = match &raw.tls {
        None => None,
        Some(t) => match (&t.cert, &t.key) {
            (Some(cert), Some(key)) => Some(TlsFiles {
                cert: file.relative(cert),
                key: file.relative(key),
                client_ca: t.client_ca.as_deref().map(|p| file.relative(p)),
            }),
            (Some(_), None) => return Err(invalid("tls.key is required with tls.cert")),
            (None, Some(_)) => return Err(invalid("tls.cert is required with tls.key")),
            (None, None) if t.client_ca.is_some() => {
                return Err(invalid("tls.client_ca requires tls.cert and tls.key"));
            }
            (None, None) => None,
        },
    };

    // [auth]
    let mut tokens: Vec<String> = Vec::new();
    let mut add_token = |token: &str, what: &dyn Fn() -> String| {
        check_token(token).map_err(|why| invalid(format!("{}: {why}", what())))?;
        if !tokens.iter().any(|t| t == token) {
            tokens.push(token.to_owned());
        }
        Ok::<(), ConfigError>(())
    };
    if let Some(list) = raw.auth.as_ref().and_then(|a| a.tokens.as_ref()) {
        for (i, token) in list.0.iter().enumerate() {
            add_token(token, &|| format!("auth.tokens[{i}]"))?;
        }
    }
    if raw
        .auth
        .as_ref()
        .and_then(|a| a.tokens_file.as_ref())
        .is_some()
    {
        let Some(tf) = &file.tokens_file else {
            return Err(invalid(
                "auth.tokens_file was not read (use FileConfig::load)",
            ));
        };
        for (n, line) in tf.contents.lines().enumerate() {
            let line = line.trim_matches(|c: char| c.is_ascii_whitespace());
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            add_token(line, &|| {
                format!("auth.tokens_file ({}), line {}", tf.path.display(), n + 1)
            })?;
        }
    }
    let tokens = Tokens(tokens);
    let auth_timeout = match raw.auth.as_ref().and_then(|a| a.timeout.as_deref()) {
        None => DEFAULT_AUTH_TIMEOUT,
        Some(s) => parse_duration(s).filter(|d| !d.is_zero()).ok_or_else(|| {
            invalid(format!(
                "auth.timeout = {s:?}: expected a positive interval such as \"500ms\" or \"10s\""
            ))
        })?,
    };
    let warnings: Vec<String> = file
        .tokens_file
        .as_ref()
        .and_then(|tf| tf.mode_warning.clone())
        .into_iter()
        .collect();

    // Per-listener requirements.
    for (i, l) in listeners.iter().enumerate() {
        let name = format!("listener[{i}] (\"{}\")", l.addr);
        if l.tls && tls.is_none() {
            return Err(invalid(format!(
                "{name}: tls = true requires tls.cert and tls.key"
            )));
        }
        match l.auth {
            AuthMode::None => {}
            AuthMode::Token => {
                if !l.tls {
                    return Err(invalid(format!(
                        "{name}: auth = \"token\" requires tls = true \
                         (the token would travel in clear text)"
                    )));
                }
                if tokens.is_empty() {
                    return Err(invalid(format!(
                        "{name}: auth = \"token\" requires at least one token \
                         (set auth.tokens or auth.tokens_file)"
                    )));
                }
            }
            AuthMode::Mtls => {
                if !l.tls {
                    return Err(invalid(format!(
                        "{name}: auth = \"mtls\" requires tls = true"
                    )));
                }
                if tls.as_ref().is_none_or(|t| t.client_ca.is_none()) {
                    return Err(invalid(format!(
                        "{name}: auth = \"mtls\" requires tls.client_ca"
                    )));
                }
            }
        }
    }

    // [http]
    let http = match &raw.http {
        None => None,
        Some(h) => {
            let Some(addr) = &h.addr else {
                return Err(invalid(
                    "http.addr is required in [http] (an IP:port such as \
                     \"127.0.0.1:9180\"); remove [http] to disable the HTTP listener",
                ));
            };
            let addr = parse_addr(addr, "http.addr")?;
            if let Some((i, l)) = listeners
                .iter()
                .enumerate()
                .find(|(_, l)| overlaps(addr, l.addr))
            {
                return Err(invalid(format!(
                    "http.addr = \"{addr}\" conflicts with listener[{i}] (\"{}\") \
                     (same port on the same or a wildcard address)",
                    l.addr
                )));
            }
            let max_tube_series = match h.max_tube_series {
                None => DEFAULT_MAX_TUBE_SERIES,
                Some(v) => usize::try_from(v).map_err(|_| {
                    invalid(format!("http.max_tube_series = {v}: must not be negative"))
                })?,
            };
            let snapshot_min_interval = match &h.snapshot_min_interval {
                None => DEFAULT_SNAPSHOT_MIN_INTERVAL,
                Some(s) => parse_duration(s).ok_or_else(|| {
                    invalid(format!(
                        "http.snapshot_min_interval = {s:?}: expected an interval such as \
                         \"1s\" or \"500ms\" (\"0s\": no caching)"
                    ))
                })?,
            };
            Some(HttpSettings {
                addr,
                max_tube_series,
                snapshot_min_interval,
            })
        }
    };

    Ok(ResolvedConfig {
        source: Some(file.path.clone()),
        listeners,
        tls,
        tokens,
        auth_timeout,
        max_job_size,
        max_pending_connections,
        binlog,
        http,
        log,
        warnings,
    })
}

/// The configuration the command line alone gives (today's behavior).
fn from_cli(cli: &Cli) -> ResolvedConfig {
    ResolvedConfig {
        source: None,
        listeners: vec![cli_listener(cli)],
        tls: None,
        tokens: Tokens::default(),
        auth_timeout: DEFAULT_AUTH_TIMEOUT,
        max_job_size: cli.max_job_size,
        max_pending_connections: DEFAULT_MAX_PENDING_CONNECTIONS,
        binlog: BinlogSettings {
            dir: cli.binlog_dir.clone(),
            file_size: cli.binlog_file_size,
            sync: cli.sync,
        },
        http: None,
        log: LogSettings {
            level: LogLevel::from_verbosity(cli.verbose),
            format: LogFormat::Text,
        },
        warnings: Vec::new(),
    }
}

fn cli_listener(cli: &Cli) -> Listener {
    Listener {
        addr: SocketAddr::new(cli.listen_addr, cli.port),
        tls: false,
        auth: AuthMode::None,
    }
}

/// `IP:port` with an IP literal (`[v6]:port` for IPv6), like `-l` / `-p`.
fn parse_addr(s: &str, key: &str) -> Result<SocketAddr, ConfigError> {
    s.parse().map_err(|_| {
        invalid(format!(
            "{key} = {s:?}: expected IP:port with an IP literal, such as \
             \"0.0.0.0:11300\" or \"[::1]:11300\""
        ))
    })
}

/// Whether two listening addresses would compete for the same socket:
/// same (non-ephemeral) port, and the same IP or either one a wildcard.
fn overlaps(a: SocketAddr, b: SocketAddr) -> bool {
    a.port() == b.port()
        && a.port() != 0
        && (a.ip() == b.ip() || a.ip().is_unspecified() || b.ip().is_unspecified())
}

/// `binlog.fsync`: `"always"` (`-f0`), `"never"` (`-F`), or `<N>ms` /
/// `<N>s` (`-f MS`; zero means always, like `-f0`).
fn parse_fsync(s: &str) -> Option<SyncPolicy> {
    match s {
        "always" => return Some(SyncPolicy::Always),
        "never" => return Some(SyncPolicy::Never),
        _ => {}
    }
    let d = parse_duration(s)?;
    Some(if d.is_zero() {
        SyncPolicy::Always
    } else {
        SyncPolicy::Interval(d)
    })
}

/// An interval written `<N>ms` or `<N>s` (decimal digits only), as in
/// `binlog.fsync`; at most `i64::MAX` nanoseconds (`-f`'s rate becomes
/// signed nanoseconds in the reference). Zero is returned as is.
fn parse_duration(s: &str) -> Option<Duration> {
    const MAX_NANOS: u64 = i64::MAX as u64;
    let (digits, ms_per_unit) = match s.strip_suffix("ms") {
        Some(d) => (d, 1),
        None => (s.strip_suffix('s')?, 1000),
    };
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let nanos = digits
        .parse::<u64>()
        .ok()?
        .checked_mul(ms_per_unit)?
        .checked_mul(1_000_000)
        .filter(|&n| n <= MAX_NANOS)?;
    Some(Duration::from_nanos(nanos))
}

/// A token must fit `auth <token>\r\n` in one protocol line and contain no
/// separator. The reason never includes the token itself.
fn check_token(token: &str) -> Result<(), String> {
    if token.is_empty() {
        return Err("empty token".to_owned());
    }
    if token.len() > MAX_TOKEN_LEN {
        return Err(format!(
            "token is {} bytes long; the maximum is {MAX_TOKEN_LEN} \
             (\"auth <token>\\r\\n\" must fit in {LINE_BUF_SIZE} bytes)",
            token.len()
        ));
    }
    if token.chars().any(|c| c.is_whitespace() || c.is_control()) {
        return Err("token contains whitespace or control characters".to_owned());
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// --check-config
// ---------------------------------------------------------------------------

/// `--check-config`: loads and resolves the configuration and returns a
/// short summary (no secrets) and the warnings.
pub fn check(cli: &Cli) -> Result<(String, Vec<String>), ConfigError> {
    load_all(cli).map(|(config, cluster)| {
        let mut text = summary(&config);
        if let Some(c) = &cluster {
            text.push_str(&cluster_summary(c));
        }
        (text, config.warnings)
    })
}

/// `--check-config` as a whole: prints the summary to `out` and returns
/// exit status 0, or prints the error to `err` and returns `EXIT_CONFIG`.
pub fn run_check(cli: &Cli, out: &mut dyn Write, err: &mut dyn Write) -> u8 {
    match check(cli) {
        Ok((text, warnings)) => {
            // Nothing sensible to do if stdout is gone; the status says OK.
            let _ = out.write_all(text.as_bytes());
            for w in warnings {
                let _ = writeln!(err, "beanstalkd-rs: warning: {w}");
            }
            0
        }
        Err(e) => {
            let _ = writeln!(err, "beanstalkd-rs: invalid configuration: {e}");
            EXIT_CONFIG
        }
    }
}

/// Human-readable summary of a resolved configuration (no token values).
pub fn summary(config: &ResolvedConfig) -> String {
    use std::fmt::Write as _;

    let mut s = String::new();
    // Writing into a String cannot fail.
    let _ = match &config.source {
        Some(path) => writeln!(s, "configuration OK: {}", path.display()),
        None => writeln!(s, "configuration OK: command line only"),
    };
    for l in &config.listeners {
        let _ = writeln!(
            s,
            "listener {} ({}, auth {})",
            l.addr,
            if l.tls { "tls" } else { "plaintext" },
            l.auth.as_str()
        );
    }
    if let Some(tls) = &config.tls {
        let _ = write!(
            s,
            "tls: cert {}, key {}",
            tls.cert.display(),
            tls.key.display()
        );
        let _ = match &tls.client_ca {
            Some(ca) => writeln!(s, ", client_ca {}", ca.display()),
            None => writeln!(s),
        };
    }
    if !config.tokens.is_empty() {
        let _ = writeln!(
            s,
            "auth: {} token(s), timeout {:?}",
            config.tokens.len(),
            config.auth_timeout
        );
    }
    if config.listeners.iter().any(|l| l.tls) {
        let _ = writeln!(
            s,
            "max pending connections: {}",
            config.max_pending_connections
        );
    }
    let _ = writeln!(s, "max job size: {}", config.max_job_size);
    let sync = match config.binlog.sync {
        SyncPolicy::Always => "always".to_owned(),
        SyncPolicy::Never => "never".to_owned(),
        SyncPolicy::Interval(d) => format!("every {d:?}"),
    };
    let _ = match &config.binlog.dir {
        Some(dir) => writeln!(
            s,
            "binlog: {} (file size {}, fsync {sync})",
            dir.display(),
            config.binlog.file_size
        ),
        None => writeln!(s, "binlog: disabled"),
    };
    let _ = match &config.http {
        Some(h) => writeln!(
            s,
            "http: {} (max tube series {}, snapshot min interval {:?})",
            h.addr, h.max_tube_series, h.snapshot_min_interval
        ),
        None => writeln!(s, "http: disabled"),
    };
    let _ = writeln!(
        s,
        "log: level {}, format {}",
        config.log.level.as_str(),
        config.log.format.as_str()
    );
    s
}

mod cluster;
pub use cluster::*;

#[cfg(test)]
mod tests;
