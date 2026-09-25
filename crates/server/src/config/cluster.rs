//! `[cluster]`: Raft cluster mode (docs/DESIGN.md §8, docs/PLAN.md §6).
//!
//! Resolved separately from the rest of the file (`resolve_cluster`), after
//! `resolve`, because some rules involve the resolved server settings
//! (`-z`, `-b`, the client and HTTP listeners). Absent `[cluster]`, nothing
//! here changes the configuration.
//!
//! Rules:
//! - `node_id` in 1..=65535 and listed in `[[cluster.peer]]`;
//! - 1, 3 or 5 peers (1 only makes sense for tests), with unique ids and
//!   unique addresses;
//! - `[cluster.tls]` (`cert`, `key`, `ca`) is required unless
//!   `insecure_plaintext = true`, and the two exclude each other;
//! - `insecure_plaintext = true` requires `listen` and every peer address to
//!   be loopback (`127.0.0.0/8`, `::1` or `localhost`), unless
//!   `insecure_plaintext_allow_remote = true` as well;
//! - `-b` / `binlog.dir` together with `[cluster]` is an error (the Raft log
//!   replaces the binlog);
//! - `heartbeat` < `election_timeout[0]` <= `election_timeout[1]`;
//! - one log entry holding a job of `max_job_size` bytes, and one snapshot
//!   chunk, must fit in a cluster frame ([`MAX_FRAME`]);
//! - `listen` must not conflict with a client or HTTP listener;
//! - `--cluster-init` needs `[cluster]`.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use super::{
    ConfigError, FileConfig, RawCluster, RawClusterTls, RawPeer, ResolvedConfig, invalid, overlaps,
    parse_addr, parse_duration,
};
use crate::cli::Cli;

/// Default `cluster.node_timeout`.
pub const DEFAULT_NODE_TIMEOUT: Duration = Duration::from_secs(5);
/// Default `cluster.snapshot_every`.
pub const DEFAULT_SNAPSHOT_EVERY: u64 = 100_000;
/// Default `cluster.heartbeat`.
pub const DEFAULT_HEARTBEAT: Duration = Duration::from_millis(50);
/// Default `cluster.election_timeout`.
pub const DEFAULT_ELECTION_TIMEOUT: (Duration, Duration) =
    (Duration::from_millis(150), Duration::from_millis(300));

/// Largest payload of one cluster-port frame (both directions).
pub const MAX_FRAME: usize = bstk_raft::wire::DEFAULT_MAX_FRAME;
/// Room reserved in a frame for everything but a job body: the log entry
/// or forward item around it (ids, command fields, tube names) and the
/// request envelope.
pub const ENTRY_OVERHEAD: usize = 64 << 10;
/// openraft's `snapshot_max_chunk_size` (set explicitly by the server).
pub const SNAPSHOT_CHUNK: usize = 3 << 20;
/// Largest `-z` usable in cluster mode.
pub const MAX_CLUSTER_JOB_SIZE: usize = MAX_FRAME - ENTRY_OVERHEAD;

/// Validated `[cluster]` settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterSettings {
    pub node_id: u64,
    /// Cluster port (Raft RPCs and forwarding).
    pub listen: SocketAddr,
    /// Raft log, vote and snapshots (relative to the configuration file).
    pub data_dir: PathBuf,
    pub node_timeout: Duration,
    pub snapshot_every: u64,
    pub heartbeat: Duration,
    /// `(min, max)`.
    pub election_timeout: (Duration, Duration),
    /// `None` only with `insecure_plaintext = true`.
    pub tls: Option<ClusterTlsFiles>,
    /// Cluster-port address of every member (this node included), by id.
    pub peers: BTreeMap<u64, String>,
    /// `--cluster-init`: bootstrap the membership from `peers`.
    pub init: bool,
}

/// `[cluster.tls]` PEM files (paths only; loaded at startup).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterTlsFiles {
    pub cert: PathBuf,
    pub key: PathBuf,
    pub ca: PathBuf,
}

/// Loads `--config` and resolves it, `[cluster]` included.
pub fn load_all(cli: &Cli) -> Result<(ResolvedConfig, Option<ClusterSettings>), ConfigError> {
    let file = cli.config.as_deref().map(FileConfig::load).transpose()?;
    resolve_all(cli, file)
}

/// `resolve`, then `resolve_cluster`.
pub fn resolve_all(
    cli: &Cli,
    mut file: Option<FileConfig>,
) -> Result<(ResolvedConfig, Option<ClusterSettings>), ConfigError> {
    // `resolve` ignores the section; take it out first.
    let raw = file
        .as_mut()
        .and_then(|f| f.raw.cluster.take().map(|r| (f.path.clone(), r)));
    let config = super::resolve(cli, file)?;
    let cluster = match raw {
        Some((path, raw)) => Some(resolve_cluster(cli, &path, raw, &config)?),
        None => None,
    };
    if cli.cluster_init && cluster.is_none() {
        return Err(invalid(
            "--cluster-init requires a [cluster] section in the configuration file",
        ));
    }
    Ok((config, cluster))
}

fn duration(key: &str, value: Option<&str>, default: Duration) -> Result<Duration, ConfigError> {
    match value {
        None => Ok(default),
        Some(s) => parse_duration(s).filter(|d| !d.is_zero()).ok_or_else(|| {
            invalid(format!(
                "cluster.{key} = {s:?}: expected a positive interval such as \"50ms\" or \"5s\""
            ))
        }),
    }
}

/// A peer address: `IP:port`, or `host:port` with a DNS name.
fn check_peer_addr(i: usize, addr: &str) -> Result<String, ConfigError> {
    if let Ok(a) = addr.parse::<SocketAddr>() {
        return Ok(a.to_string());
    }
    let bad = || {
        invalid(format!(
            "cluster.peer[{i}].addr = {addr:?}: expected host:port, such as \
             \"10.0.0.1:11400\" or \"node1.example:11400\""
        ))
    };
    let (host, port) = addr.rsplit_once(':').ok_or_else(bad)?;
    let valid_host = !host.is_empty()
        && host
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'.');
    if !valid_host || port.parse::<u16>().is_err() {
        return Err(bad());
    }
    Ok(addr.to_ascii_lowercase())
}

fn resolve_cluster(
    cli: &Cli,
    path: &std::path::Path,
    raw: RawCluster,
    config: &ResolvedConfig,
) -> Result<ClusterSettings, ConfigError> {
    let relative = |p: &std::path::Path| path.parent().unwrap_or(std::path::Path::new("")).join(p);

    let node_id = match raw.node_id {
        None => return Err(invalid("cluster.node_id is required in [cluster]")),
        Some(v) => u64::try_from(v)
            .ok()
            .filter(|v| (1..=bstk_raft::MAX_NODE_ID).contains(v))
            .ok_or_else(|| {
                invalid(format!(
                    "cluster.node_id = {v}: must be between 1 and {}",
                    bstk_raft::MAX_NODE_ID
                ))
            })?,
    };
    let Some(listen) = &raw.listen else {
        return Err(invalid(
            "cluster.listen is required in [cluster] (the cluster port, an IP:port such as \
             \"10.0.0.1:11400\")",
        ));
    };
    let listen = parse_addr(listen, "cluster.listen")?;
    let Some(data_dir) = &raw.data_dir else {
        return Err(invalid(
            "cluster.data_dir is required in [cluster] (the Raft log and snapshots)",
        ));
    };
    let data_dir = relative(data_dir);

    // Peers.
    let mut peers = BTreeMap::new();
    let mut addrs: Vec<String> = Vec::new();
    for (i, RawPeer { id, addr }) in raw.peers.iter().enumerate() {
        let id = u64::try_from(*id)
            .ok()
            .filter(|v| (1..=bstk_raft::MAX_NODE_ID).contains(v))
            .ok_or_else(|| {
                invalid(format!(
                    "cluster.peer[{i}].id = {id}: must be between 1 and {}",
                    bstk_raft::MAX_NODE_ID
                ))
            })?;
        let addr = check_peer_addr(i, addr)?;
        if peers.contains_key(&id) {
            return Err(invalid(format!(
                "cluster.peer[{i}].id = {id}: listed more than once"
            )));
        }
        if addrs.contains(&addr) {
            return Err(invalid(format!(
                "cluster.peer[{i}].addr = {addr:?}: used by another peer"
            )));
        }
        addrs.push(addr.clone());
        peers.insert(id, addr);
    }
    if !matches!(peers.len(), 1 | 3 | 5) {
        return Err(invalid(format!(
            "[[cluster.peer]] lists {} node(s): a cluster has 3 or 5 nodes (1 for tests)",
            peers.len()
        )));
    }
    if !peers.contains_key(&node_id) {
        return Err(invalid(format!(
            "cluster.node_id = {node_id} is not listed in [[cluster.peer]]"
        )));
    }

    // Timing.
    let node_timeout = duration(
        "node_timeout",
        raw.node_timeout.as_deref(),
        DEFAULT_NODE_TIMEOUT,
    )?;
    let heartbeat = duration("heartbeat", raw.heartbeat.as_deref(), DEFAULT_HEARTBEAT)?;
    let election_timeout = match &raw.election_timeout {
        None => DEFAULT_ELECTION_TIMEOUT,
        Some(v) => match v.as_slice() {
            [a, b] => (
                duration("election_timeout[0]", Some(a), Duration::ZERO)?,
                duration("election_timeout[1]", Some(b), Duration::ZERO)?,
            ),
            _ => {
                return Err(invalid(
                    "cluster.election_timeout: expected two intervals [min, max], such as \
                     [\"150ms\", \"300ms\"]",
                ));
            }
        },
    };
    if heartbeat >= election_timeout.0 {
        return Err(invalid(format!(
            "cluster.heartbeat ({heartbeat:?}) must be shorter than the minimum election \
             timeout ({:?})",
            election_timeout.0
        )));
    }
    if election_timeout.0 > election_timeout.1 {
        return Err(invalid(format!(
            "cluster.election_timeout: the minimum ({:?}) exceeds the maximum ({:?})",
            election_timeout.0, election_timeout.1
        )));
    }
    // openraft takes milliseconds.
    for (key, d) in [
        ("heartbeat", heartbeat),
        ("election_timeout", election_timeout.0),
        ("election_timeout", election_timeout.1),
    ] {
        if d.subsec_nanos() % 1_000_000 != 0 {
            return Err(invalid(format!(
                "cluster.{key}: {d:?} is not a whole number of milliseconds"
            )));
        }
    }
    let snapshot_every = match raw.snapshot_every {
        None => DEFAULT_SNAPSHOT_EVERY,
        Some(v) => u64::try_from(v)
            .ok()
            .filter(|&v| v >= 1)
            .ok_or_else(|| invalid(format!("cluster.snapshot_every = {v}: must be at least 1")))?,
    };

    // Security.
    let insecure_plaintext = raw.insecure_plaintext.unwrap_or(false);
    let tls = match &raw.tls {
        Some(RawClusterTls { cert, key, ca }) => {
            let need = |v: &Option<PathBuf>, k: &str| {
                v.as_deref()
                    .map(&relative)
                    .ok_or_else(|| invalid(format!("cluster.tls.{k} is required in [cluster.tls]")))
            };
            Some(ClusterTlsFiles {
                cert: need(cert, "cert")?,
                key: need(key, "key")?,
                ca: need(ca, "ca")?,
            })
        }
        None => None,
    };
    let allow_remote = raw.insecure_plaintext_allow_remote.unwrap_or(false);
    if tls.is_none() && !insecure_plaintext {
        return Err(invalid(
            "[cluster.tls] (cert, key, ca) is required: cluster traffic uses mutual TLS \
             (set cluster.insecure_plaintext = true only for tests)",
        ));
    }
    if tls.is_some() && insecure_plaintext {
        return Err(invalid(
            "[cluster.tls] and cluster.insecure_plaintext = true exclude each other: remove \
             one of them",
        ));
    }
    if allow_remote && !insecure_plaintext {
        return Err(invalid(
            "cluster.insecure_plaintext_allow_remote = true requires \
             cluster.insecure_plaintext = true",
        ));
    }
    if insecure_plaintext && !allow_remote {
        let remote = std::iter::once(listen.to_string())
            .chain(peers.values().cloned())
            .find(|a| !is_loopback(a));
        if let Some(addr) = remote {
            return Err(invalid(format!(
                "cluster.insecure_plaintext = true sends unauthenticated, unencrypted cluster \
                 traffic, so it is allowed only when cluster.listen and every peer address are \
                 loopback; {addr:?} is not (use [cluster.tls], or set \
                 cluster.insecure_plaintext_allow_remote = true on a network you trust \
                 completely)"
            )));
        }
    }

    // Interaction with the rest of the configuration.
    if let Some(dir) = &config.binlog.dir {
        return Err(invalid(format!(
            "-b / binlog.dir ({}) cannot be used with [cluster]: in cluster mode the Raft log \
             in cluster.data_dir replaces the binlog",
            dir.display()
        )));
    }
    let z = config.max_job_size as usize;
    if z + ENTRY_OVERHEAD > MAX_FRAME {
        return Err(invalid(format!(
            "max job size {z} is too large for cluster mode: a job must fit in one cluster \
             frame of {MAX_FRAME} bytes with {ENTRY_OVERHEAD} bytes of overhead (at most \
             {MAX_CLUSTER_JOB_SIZE})"
        )));
    }
    const _: () = assert!(SNAPSHOT_CHUNK + ENTRY_OVERHEAD <= MAX_FRAME);
    if let Some((i, l)) = config
        .listeners
        .iter()
        .enumerate()
        .find(|(_, l)| overlaps(listen, l.addr))
    {
        return Err(invalid(format!(
            "cluster.listen = \"{listen}\" conflicts with listener[{i}] (\"{}\")",
            l.addr
        )));
    }
    if let Some(h) = &config.http
        && overlaps(listen, h.addr)
    {
        return Err(invalid(format!(
            "cluster.listen = \"{listen}\" conflicts with http.addr (\"{}\")",
            h.addr
        )));
    }

    Ok(ClusterSettings {
        node_id,
        listen,
        data_dir,
        node_timeout,
        snapshot_every,
        heartbeat,
        election_timeout,
        tls,
        peers,
        init: cli.cluster_init,
    })
}

/// Whether a cluster address (`IP:port` or `host:port`) is loopback.
fn is_loopback(addr: &str) -> bool {
    if let Ok(a) = addr.parse::<SocketAddr>() {
        return a.ip().is_loopback();
    }
    addr.rsplit_once(':')
        .is_some_and(|(host, _)| host.eq_ignore_ascii_case("localhost"))
}

/// `--check-config` lines for `[cluster]`.
pub fn cluster_summary(c: &ClusterSettings) -> String {
    let mut s = String::new();
    let _ = writeln!(
        s,
        "cluster: node {} of {} (listen {}, data_dir {})",
        c.node_id,
        c.peers.len(),
        c.listen,
        c.data_dir.display()
    );
    for (id, addr) in &c.peers {
        let _ = writeln!(s, "cluster peer {id}: {addr}");
    }
    let _ = match &c.tls {
        Some(t) => writeln!(
            s,
            "cluster tls: cert {}, key {}, ca {}",
            t.cert.display(),
            t.key.display(),
            t.ca.display()
        ),
        None => writeln!(
            s,
            "cluster tls: DISABLED (insecure_plaintext: cluster traffic is neither \
             authenticated nor encrypted)"
        ),
    };
    let _ = writeln!(
        s,
        "cluster timing: node_timeout {:?}, heartbeat {:?}, election timeout {:?}..{:?}, \
         snapshot every {} entries",
        c.node_timeout, c.heartbeat, c.election_timeout.0, c.election_timeout.1, c.snapshot_every
    );
    s
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::path::Path;

    use super::*;

    fn cli(args: &[&str]) -> Cli {
        let mut argv = vec!["beanstalkd-rs"];
        argv.extend_from_slice(args);
        Cli::try_parse_args(&argv).expect("valid command line")
    }

    fn resolve(args: &[&str], text: &str) -> Result<Option<ClusterSettings>, ConfigError> {
        let file = FileConfig::parse(text, Path::new("/etc/bstk/node.toml"))?;
        resolve_all(&cli(args), Some(file)).map(|(_, c)| c)
    }

    fn error(args: &[&str], text: &str) -> String {
        resolve(args, text)
            .expect_err("invalid configuration")
            .to_string()
    }

    const PEERS3: &str = "[[cluster.peer]]\nid = 1\naddr = \"10.0.0.1:11400\"\n\
                          [[cluster.peer]]\nid = 2\naddr = \"10.0.0.2:11400\"\n\
                          [[cluster.peer]]\nid = 3\naddr = \"node3.example:11400\"\n";

    fn config(cluster_lines: &str, peers: &str) -> String {
        format!(
            "[cluster]\nnode_id = 2\nlisten = \"0.0.0.0:11400\"\ndata_dir = \"raft\"\n\
             {cluster_lines}\n{peers}"
        )
    }

    const TLS: &str = "[cluster.tls]\ncert = \"n2.pem\"\nkey = \"n2.key\"\nca = \"/pki/ca.pem\"\n";

    #[test]
    fn defaults_and_paths() {
        let c = resolve(&[], &config(TLS, PEERS3)).unwrap().unwrap();
        assert_eq!(
            c,
            ClusterSettings {
                node_id: 2,
                listen: "0.0.0.0:11400".parse().unwrap(),
                data_dir: PathBuf::from("/etc/bstk/raft"),
                node_timeout: DEFAULT_NODE_TIMEOUT,
                snapshot_every: DEFAULT_SNAPSHOT_EVERY,
                heartbeat: DEFAULT_HEARTBEAT,
                election_timeout: DEFAULT_ELECTION_TIMEOUT,
                tls: Some(ClusterTlsFiles {
                    cert: PathBuf::from("/etc/bstk/n2.pem"),
                    key: PathBuf::from("/etc/bstk/n2.key"),
                    ca: PathBuf::from("/pki/ca.pem"),
                }),
                peers: [
                    (1, "10.0.0.1:11400".to_string()),
                    (2, "10.0.0.2:11400".to_string()),
                    (3, "node3.example:11400".to_string()),
                ]
                .into(),
                init: false,
            }
        );
        let c = resolve(&["--cluster-init"], &config(TLS, PEERS3))
            .unwrap()
            .unwrap();
        assert!(c.init);
        // Without [cluster] nothing changes.
        assert_eq!(resolve(&[], "").unwrap(), None);
        let s = cluster_summary(&c);
        assert!(s.contains("cluster: node 2 of 3"), "{s}");
        assert!(s.contains("cluster peer 3: node3.example:11400"), "{s}");
    }

    #[test]
    fn explicit_values() {
        let c = resolve(
            &[],
            &config(
                "node_timeout = \"2s\"\nsnapshot_every = 7\nheartbeat = \"20ms\"\n\
                 election_timeout = [\"100ms\", \"100ms\"]\ninsecure_plaintext = true\n\
                 insecure_plaintext_allow_remote = true",
                PEERS3,
            ),
        )
        .unwrap()
        .unwrap();
        assert_eq!(c.node_timeout, Duration::from_secs(2));
        assert_eq!(c.snapshot_every, 7);
        assert_eq!(c.heartbeat, Duration::from_millis(20));
        assert_eq!(
            c.election_timeout,
            (Duration::from_millis(100), Duration::from_millis(100))
        );
        assert_eq!(c.tls, None);
    }

    #[test]
    fn node_and_peer_rules() {
        let one = "[[cluster.peer]]\nid = 2\naddr = \"10.0.0.2:1\"\n";
        assert!(resolve(&[], &config(TLS, one)).is_ok());
        let e = error(&[], &config(TLS, ""));
        assert!(e.contains("lists 0 node(s)"), "{e}");
        let two =
            "[[cluster.peer]]\nid = 1\naddr = \"a:1\"\n[[cluster.peer]]\nid = 2\naddr = \"b:1\"\n";
        assert!(error(&[], &config(TLS, two)).contains("3 or 5"));
        let five: String = (1..=5)
            .map(|i| format!("[[cluster.peer]]\nid = {i}\naddr = \"10.0.0.{i}:1\"\n"))
            .collect();
        assert!(resolve(&[], &config(TLS, &five)).is_ok());
        let four: String = (1..=4)
            .map(|i| format!("[[cluster.peer]]\nid = {i}\naddr = \"10.0.0.{i}:1\"\n"))
            .collect();
        assert!(error(&[], &config(TLS, &four)).contains("3 or 5"));
        let e = error(&[], &config(TLS, &PEERS3.replace("id = 2", "id = 1")));
        assert!(e.contains("listed more than once"), "{e}");
        let e = error(
            &[],
            &config(TLS, &PEERS3.replace("10.0.0.2:11400", "10.0.0.1:11400")),
        );
        assert!(e.contains("used by another peer"), "{e}");
        let e = error(&[], &config(TLS, &PEERS3.replace("id = 2", "id = 4")));
        assert!(e.contains("not listed"), "{e}");
        for bad in ["\"10.0.0.1\"", "\"x:y\"", "\":1\"", "\"a b:1\""] {
            let e = error(
                &[],
                &config(TLS, &PEERS3.replace("\"10.0.0.1:11400\"", bad)),
            );
            assert!(e.contains("cluster.peer[0].addr"), "{bad}: {e}");
        }
        for (v, what) in [
            ("0", "node_id = 0"),
            ("65536", "node_id = 65536"),
            ("-1", "node_id = -1"),
        ] {
            let text = config(TLS, PEERS3).replace("node_id = 2", &format!("node_id = {v}"));
            assert!(error(&[], &text).contains(what), "{v}");
        }
        let e = error(&[], &config(TLS, &PEERS3.replace("id = 3", "id = 70000")));
        assert!(e.contains("cluster.peer[2].id = 70000"), "{e}");
        let text = config(TLS, PEERS3).replace("listen = \"0.0.0.0:11400\"\n", "");
        assert!(error(&[], &text).contains("cluster.listen is required"));
        let text = config(TLS, PEERS3).replace("data_dir = \"raft\"\n", "");
        assert!(error(&[], &text).contains("cluster.data_dir is required"));
        let text = config(TLS, PEERS3).replace("node_id = 2\n", "");
        assert!(error(&[], &text).contains("cluster.node_id is required"));
    }

    #[test]
    fn security_rules() {
        let e = error(&[], &config("", PEERS3));
        assert!(
            e.contains("[cluster.tls]") && e.contains("insecure_plaintext"),
            "{e}"
        );
        let e = error(
            &[],
            &config("[cluster.tls]\ncert = \"a\"\nkey = \"b\"\n", PEERS3),
        );
        assert!(e.contains("cluster.tls.ca is required"), "{e}");
        let e = error(
            &[],
            &config("[cluster.tls]\ncert = \"a\"\nca = \"b\"\nx = 1\n", PEERS3),
        );
        assert!(e.contains("unknown field"), "{e}");
        // [cluster.tls] and insecure_plaintext exclude each other.
        let e = error(
            &[],
            &config(&format!("insecure_plaintext = true\n{TLS}"), PEERS3),
        );
        assert!(e.contains("exclude each other"), "{e}");
    }

    #[test]
    fn plaintext_is_loopback_only_unless_explicitly_allowed() {
        let loopback = "[[cluster.peer]]\nid = 1\naddr = \"127.0.0.1:1\"\n\
                        [[cluster.peer]]\nid = 2\naddr = \"[::1]:2\"\n\
                        [[cluster.peer]]\nid = 3\naddr = \"localhost:3\"\n";
        let local = |extra: &str, peers: &str| {
            config(&format!("insecure_plaintext = true\n{extra}"), peers)
                .replace("0.0.0.0:11400", "127.0.0.2:11400")
        };
        let c = resolve(&[], &local("", loopback)).unwrap().unwrap();
        assert_eq!(c.tls, None);
        assert!(cluster_summary(&c).contains("DISABLED"));
        // A remote peer, or a listen address on every interface.
        let e = error(&[], &local("", PEERS3));
        assert!(
            e.contains("\"10.0.0.1:11400\" is not") && e.contains("allow_remote"),
            "{e}"
        );
        let e = error(&[], &config("insecure_plaintext = true", loopback));
        assert!(e.contains("\"0.0.0.0:11400\" is not"), "{e}");
        let e = error(
            &[],
            &local("", &loopback.replace("localhost:3", "node3.example:3")),
        );
        assert!(e.contains("node3.example:3"), "{e}");
        // The second opt-in allows it.
        let c = resolve(
            &[],
            &local("insecure_plaintext_allow_remote = true", PEERS3),
        )
        .unwrap()
        .unwrap();
        assert_eq!(c.tls, None);
        // ... but only together with insecure_plaintext.
        let e = error(
            &[],
            &config(
                &format!("insecure_plaintext_allow_remote = true\n{TLS}"),
                PEERS3,
            ),
        );
        assert!(
            e.contains("requires cluster.insecure_plaintext = true"),
            "{e}"
        );
    }

    #[test]
    fn timing_rules() {
        let e = error(
            &[],
            &config(TLS, PEERS3).replace(
                "data_dir = \"raft\"\n",
                "data_dir = \"raft\"\nheartbeat = \"150ms\"\n",
            ),
        );
        assert!(e.contains("must be shorter"), "{e}");
        let text = config(TLS, PEERS3).replace(
            "data_dir = \"raft\"\n",
            "data_dir = \"raft\"\nelection_timeout = [\"300ms\", \"299ms\"]\n",
        );
        assert!(error(&[], &text).contains("exceeds the maximum"));
        let text = config(TLS, PEERS3).replace(
            "data_dir = \"raft\"\n",
            "data_dir = \"raft\"\nelection_timeout = [\"300ms\"]\n",
        );
        assert!(error(&[], &text).contains("two intervals"));
        for (key, v) in [
            ("node_timeout", "\"0s\""),
            ("node_timeout", "\"5\""),
            ("heartbeat", "\"fast\""),
        ] {
            let text = config(TLS, PEERS3).replace(
                "data_dir = \"raft\"\n",
                &format!("data_dir = \"raft\"\n{key} = {v}\n"),
            );
            assert!(
                error(&[], &text).contains(&format!("cluster.{key}")),
                "{key} {v}"
            );
        }
        let text = config(TLS, PEERS3).replace(
            "data_dir = \"raft\"\n",
            "data_dir = \"raft\"\nsnapshot_every = 0\n",
        );
        assert!(error(&[], &text).contains("snapshot_every = 0"));
    }

    #[test]
    fn commented_example_in_docs_is_valid() {
        let path =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/beanstalkd-rs.example.toml");
        let text = std::fs::read_to_string(&path).expect("example file exists");
        let start = text.find("\n# [cluster]\n").expect("a [cluster] example");
        // Uncomment the example (lines "# key = ..."), skipping the
        // explanations inside it ("# # ...").
        let example: String = text[start..]
            .lines()
            .filter(|l| !l.starts_with("# #"))
            .map(|l| {
                l.strip_prefix("# ")
                    .or_else(|| l.strip_prefix('#'))
                    .unwrap_or(l)
            })
            .map(|l| format!("{l}\n"))
            .collect();
        let c = resolve(&[], &example).unwrap().unwrap();
        assert_eq!(c.node_id, 1);
        assert_eq!(c.peers.len(), 3);
        assert!(c.tls.is_some());
        assert_eq!(
            MAX_CLUSTER_JOB_SIZE, 33_488_896,
            "documented in the example"
        );
    }

    #[test]
    fn interactions_with_the_rest() {
        let text = config(TLS, PEERS3);
        let e = error(&["-b", "/var/wal"], &text);
        assert!(e.contains("cannot be used with [cluster]"), "{e}");
        let e = error(&[], &format!("[binlog]\ndir = \"wal\"\n{text}"));
        assert!(e.contains("cannot be used with [cluster]"), "{e}");
        // -s alone is fine (only reported by stats).
        assert!(resolve(&["-s", "4096"], &text).is_ok());
        // Job size against the frame size.
        let max = MAX_CLUSTER_JOB_SIZE.to_string();
        assert!(resolve(&["-z", &max], &text).is_ok());
        let over = (MAX_CLUSTER_JOB_SIZE + 1).to_string();
        let e = error(&["-z", &over], &text);
        assert!(e.contains("too large for cluster mode"), "{e}");
        let e = error(&[], &format!("[server]\nmax_job_size = 1073741824\n{text}"));
        assert!(e.contains("too large for cluster mode"), "{e}");
        // Port conflicts.
        let e = error(
            &[],
            &format!("[[listener]]\naddr = \"127.0.0.1:11400\"\n{text}"),
        );
        assert!(e.contains("conflicts with listener[0]"), "{e}");
        let e = error(&[], &format!("[http]\naddr = \"127.0.0.1:11400\"\n{text}"));
        assert!(e.contains("conflicts with http.addr"), "{e}");
        // --cluster-init needs [cluster].
        let e = resolve(&["--cluster-init"], "").expect_err("needs [cluster]");
        assert!(e.to_string().contains("--cluster-init requires"), "{e}");
    }
}
