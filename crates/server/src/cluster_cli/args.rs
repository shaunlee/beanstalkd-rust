//! Arguments of `beanstalkd-rs cluster` and the settings they resolve to.

use std::path::{Path, PathBuf};
use std::time::Duration;

use bstk_raft::NodeId;
use clap::{Args, Subcommand};
use serde::Deserialize;

use super::link::Auth;

const AFTER_HELP: &str = "\
Exit status:
  0  done (or, for status, answered)
  1  refused by the cluster (a guardrail, a changed membership, a rejected
     identity) or an unexpected answer
  2  usage error, or unreadable certificate or configuration files
  3  no node could be reached (or no leader found) within the timeout
  4  the change was accepted or sent but not confirmed within the timeout; it
     may still complete: run `beanstalkd-rs cluster status`

Authentication: the commands speak the cluster port's admin channel. With mTLS
(the default) present the operator certificate from `scripts/mkcluster-certs.sh
DIR admin` (--cert admin.pem --key admin.key) and the cluster CA (--ca
cluster-ca.pem, or the [cluster.tls] ca of --config); a node certificate is
refused. A cluster running insecure_plaintext is reached with
--insecure-plaintext, from the machine of the node only.

Examples:
  beanstalkd-rs cluster status --config node1.toml --cert admin.pem --key admin.key
  beanstalkd-rs cluster add 4 10.0.0.4:11302 --node 10.0.0.1:11302 --ca ca.pem \\
      --cert admin.pem --key admin.key
";

/// `beanstalkd-rs cluster`: inspect and change the membership of a running
/// cluster. Not a reference-beanstalkd flag set: the flat server flags do
/// not apply here.
#[derive(Args, Debug)]
#[command(
    about = "Inspect and change the membership of a running cluster",
    after_help = AFTER_HELP,
    subcommand_required = true,
    arg_required_else_help = true,
    disable_help_subcommand = true,
    subcommand_value_name = "COMMAND",
    subcommand_help_heading = "Commands"
)]
pub struct ClusterArgs {
    #[command(flatten)]
    pub common: Common,
    #[command(subcommand)]
    pub command: ClusterCmd,
}

#[derive(Args, Debug)]
pub struct Common {
    /// Cluster address (HOST:PORT) of any member; repeatable. The tool asks
    /// the nodes in order until one answers and follows the leader.
    #[arg(long, value_name = "HOST:PORT", value_parser = parse_hostport, global = true)]
    pub node: Vec<String>,

    /// A node's configuration file: its [[cluster.peer]] addresses are used
    /// as --node and its [cluster.tls] ca as --ca
    // The id differs from the server's `--config` on purpose: global options
    // propagate into the parent's matches by id, where `Cli` would take it
    // for a server flag.
    #[arg(
        long = "config",
        id = "node_config",
        value_name = "FILE",
        global = true
    )]
    pub config: Option<PathBuf>,

    /// Cluster CA certificate (PEM) that signed the nodes' certificates
    #[arg(long, value_name = "FILE", global = true)]
    pub ca: Option<PathBuf>,

    /// Operator certificate (PEM, SAN bstk-admin), see `scripts/mkcluster-certs.sh DIR admin`
    #[arg(long, value_name = "FILE", global = true)]
    pub cert: Option<PathBuf>,

    /// Private key (PEM) of --cert
    #[arg(long, value_name = "FILE", global = true)]
    pub key: Option<PathBuf>,

    /// Talk plain TCP, for a cluster that runs insecure_plaintext (tests
    /// only; accepted from loopback only)
    #[arg(long, global = true)]
    pub insecure_plaintext: bool,

    /// How long a command may take in all, e.g. 30s, 500ms, 2m (a bare number
    /// is seconds)
    #[arg(long, value_name = "DURATION", default_value = "30s", value_parser = parse_timeout, global = true)]
    pub timeout: Duration,

    /// Print one JSON object instead of text (errors too)
    #[arg(long, global = true)]
    pub json: bool,
}

#[derive(Subcommand, Debug, Clone, PartialEq, Eq)]
pub enum ClusterCmd {
    /// Show the membership: voters, learners, the leader, and each node's
    /// state and replication lag
    Status,
    /// Add a node as a learner (it replicates but does not vote)
    Add {
        /// Node id: never used before (ids are never reused)
        #[arg(value_name = "ID", value_parser = parse_id)]
        id: NodeId,
        /// The node's cluster address
        #[arg(value_name = "HOST:PORT", value_parser = parse_hostport)]
        addr: String,
    },
    /// Promote a caught-up learner to voter
    Promote {
        #[arg(value_name = "ID", value_parser = parse_id)]
        id: NodeId,
        /// Promote a learner that is not caught up
        #[arg(long)]
        force: bool,
    },
    /// Remove a node (a learner or a voter, the leader too). The node is not
    /// told: stop its process.
    Remove {
        #[arg(value_name = "ID", value_parser = parse_id)]
        id: NodeId,
        /// Go below 3 voters
        #[arg(long)]
        force: bool,
    },
    /// Change a node's cluster address
    #[command(name = "set-addr")]
    SetAddr {
        #[arg(value_name = "ID", value_parser = parse_id)]
        id: NodeId,
        #[arg(value_name = "HOST:PORT", value_parser = parse_hostport)]
        addr: String,
        /// Allow an address off loopback over plaintext cluster traffic
        #[arg(long)]
        force: bool,
    },
}

/// What the commands need, after the files are read.
pub struct Settings {
    pub seeds: Vec<String>,
    pub auth: Auth,
    pub timeout: Duration,
    pub json: bool,
}

/// The parts of a node configuration the tool reads. Unknown keys are
/// ignored: the file is the node's, and it may be newer than this tool.
#[derive(Deserialize, Default)]
struct NodeConfig {
    cluster: Option<ClusterSection>,
}

#[derive(Deserialize, Default)]
struct ClusterSection {
    #[serde(default)]
    peer: Vec<PeerEntry>,
    tls: Option<TlsSection>,
    insecure_plaintext: Option<bool>,
}

#[derive(Deserialize)]
struct PeerEntry {
    addr: String,
}

#[derive(Deserialize)]
struct TlsSection {
    ca: Option<PathBuf>,
}

impl Common {
    /// Reads the files and checks the combination. `Err`: a usage error.
    pub fn resolve(&self) -> Result<Settings, String> {
        let mut seeds = self.node.clone();
        let mut config_ca = None;
        let mut config_plaintext = false;
        if let Some(path) = &self.config {
            let text = std::fs::read_to_string(path)
                .map_err(|e| format!("--config {}: {e}", path.display()))?;
            let cfg: NodeConfig =
                toml::from_str(&text).map_err(|e| format!("--config {}: {e}", path.display()))?;
            let cluster = cfg
                .cluster
                .ok_or_else(|| format!("--config {}: no [cluster] section", path.display()))?;
            for peer in cluster.peer {
                let addr = parse_hostport(&peer.addr)
                    .map_err(|e| format!("--config {}: cluster.peer: {e}", path.display()))?;
                if !seeds.contains(&addr) {
                    seeds.push(addr);
                }
            }
            config_ca = cluster.tls.and_then(|t| t.ca);
            config_plaintext = cluster.insecure_plaintext == Some(true);
        }
        if seeds.is_empty() {
            return Err("no node to ask: give --node HOST:PORT or --config FILE".into());
        }

        let auth = if self.insecure_plaintext {
            if self.ca.is_some() || self.cert.is_some() || self.key.is_some() {
                return Err(
                    "--insecure-plaintext cannot be combined with --ca, --cert or --key".into(),
                );
            }
            Auth::Plain
        } else {
            let Some(cert) = &self.cert else {
                return Err(if config_plaintext {
                    "--cert and --key (the bstk-admin certificate) are required; the configuration \
                     says insecure_plaintext = true, so pass --insecure-plaintext to talk plain TCP"
                        .into()
                } else {
                    "--cert and --key (the bstk-admin certificate) are required; for a cluster \
                     that runs insecure_plaintext pass --insecure-plaintext"
                        .into()
                });
            };
            let key = self.key.as_ref().ok_or("--key is required with --cert")?;
            let ca = self.ca.as_ref().or(config_ca.as_ref()).ok_or(
                "--ca (the cluster CA certificate) is required, or --config with [cluster.tls] ca",
            )?;
            let read = |what: &str, p: &Path| {
                std::fs::read(p).map_err(|e| format!("{what} {}: {e}", p.display()))
            };
            let tls = bstk_raft::tls::admin_client_tls_from_pem(
                &read("--cert", cert)?,
                &read("--key", key)?,
                &read("--ca", ca)?,
            )
            .map_err(|e| e.to_string())?;
            Auth::Tls(tls)
        };
        Ok(Settings {
            seeds,
            auth,
            timeout: self.timeout,
            json: self.json,
        })
    }
}

/// `HOST:PORT` (host a name, an IPv4 address or `[IPv6]`), port 1..=65535.
pub fn parse_hostport(s: &str) -> Result<String, String> {
    let bad = || format!("{s:?} is not HOST:PORT");
    let (host, port) = s.rsplit_once(':').ok_or_else(bad)?;
    if host.is_empty() || host.chars().any(|c| c.is_whitespace() || c.is_control()) {
        return Err(bad());
    }
    if host.contains(':') && !(host.starts_with('[') && host.ends_with(']')) {
        return Err(format!("{s:?}: write an IPv6 address as [ADDR]:PORT"));
    }
    match port.parse::<u16>() {
        Ok(p) if p != 0 => Ok(s.to_owned()),
        _ => Err(format!("{s:?}: the port must be 1..=65535")),
    }
}

fn parse_id(s: &str) -> Result<NodeId, String> {
    match s.parse::<NodeId>() {
        Ok(id) if (1..=bstk_raft::MAX_NODE_ID).contains(&id) => Ok(id),
        _ => Err(format!("node id must be 1..={}", bstk_raft::MAX_NODE_ID)),
    }
}

/// `30`, `30s`, `500ms` or `2m`; at least 1 ms, at most 1 hour.
fn parse_timeout(s: &str) -> Result<Duration, String> {
    let (digits, unit_ms) = if let Some(d) = s.strip_suffix("ms") {
        (d, 1)
    } else if let Some(d) = s.strip_suffix('s') {
        (d, 1000)
    } else if let Some(d) = s.strip_suffix('m') {
        (d, 60_000)
    } else {
        (s, 1000)
    };
    let n: u64 = digits
        .parse()
        .map_err(|_| format!("{s:?} is not a duration (30s, 500ms, 2m)"))?;
    let ms = n
        .checked_mul(unit_ms)
        .filter(|&ms| (1..=3_600_000).contains(&ms))
        .ok_or_else(|| format!("{s:?}: the timeout must be between 1ms and 1h"))?;
    Ok(Duration::from_millis(ms))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hostport_accepts_names_addresses_and_brackets() {
        for ok in [
            "127.0.0.1:1",
            "node-1.example.org:65535",
            "[::1]:11302",
            "a:2",
        ] {
            assert_eq!(parse_hostport(ok).as_deref(), Ok(ok));
        }
        for bad in [
            "",
            "host",
            ":80",
            "host:",
            "host:0",
            "host:65536",
            "::1:80",
            "a b:1",
            "h:x",
        ] {
            assert!(parse_hostport(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn timeouts_have_units_and_bounds() {
        assert_eq!(parse_timeout("30"), Ok(Duration::from_secs(30)));
        assert_eq!(parse_timeout("30s"), Ok(Duration::from_secs(30)));
        assert_eq!(parse_timeout("250ms"), Ok(Duration::from_millis(250)));
        assert_eq!(parse_timeout("2m"), Ok(Duration::from_secs(120)));
        for bad in ["", "0", "0ms", "1h", "61m", "-1", "1.5s", "s"] {
            assert!(parse_timeout(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn ids_are_node_ids() {
        assert_eq!(parse_id("1"), Ok(1));
        assert_eq!(parse_id("65535"), Ok(65535));
        for bad in ["0", "65536", "-1", "x", ""] {
            assert!(parse_id(bad).is_err(), "{bad:?}");
        }
    }

    fn common(args: &[&str]) -> Common {
        use clap::Parser;
        #[derive(Parser)]
        struct W {
            #[command(flatten)]
            c: Common,
        }
        let mut argv = vec!["x"];
        argv.extend_from_slice(args);
        W::try_parse_from(argv).expect("parses").c
    }

    #[test]
    fn settings_need_a_node_and_an_identity() {
        let e = |args: &[&str]| match common(args).resolve() {
            Ok(_) => panic!("{args:?} resolved"),
            Err(e) => e,
        };
        assert!(e(&["--insecure-plaintext"]).contains("no node to ask"));
        assert!(e(&["--node", "h:1"]).contains("--cert and --key"));
        assert!(e(&["--node", "h:1", "--cert", "c"]).contains("--key is required"));
        assert!(e(&["--node", "h:1", "--cert", "c", "--key", "k"]).contains("--ca"));
        assert!(
            e(&["--node", "h:1", "--insecure-plaintext", "--cert", "c"])
                .contains("cannot be combined")
        );
        // Unreadable files are named.
        assert!(
            e(&[
                "--node", "h:1", "--cert", "/nope/c", "--key", "/nope/k", "--ca", "/nope/a"
            ])
            .contains("/nope/c")
        );
        assert!(e(&["--config", "/nope/n.toml"]).contains("/nope/n.toml"));
        let st = common(&["--node", "h:1", "--insecure-plaintext"])
            .resolve()
            .expect("plaintext");
        assert!(matches!(st.auth, Auth::Plain));
        assert_eq!(st.seeds, ["h:1"]);
    }

    #[test]
    fn config_gives_seeds_and_the_ca_but_never_plaintext() {
        let dir = tempfile::tempdir().expect("dir");
        let cfg = dir.path().join("n.toml");
        std::fs::write(
            &cfg,
            "[server]\nfuture_key = 1\n[cluster]\nnode_id = 1\ninsecure_plaintext = true\n\
             [[cluster.peer]]\nid = 1\naddr = \"10.0.0.1:11302\"\n\
             [[cluster.peer]]\nid = 2\naddr = \"10.0.0.2:11302\"\n",
        )
        .expect("write");
        let path = cfg.to_str().expect("utf8");
        // Seeds: --node first, then the peers, without repeats.
        let c = common(&[
            "--config",
            path,
            "--node",
            "10.0.0.2:11302",
            "--insecure-plaintext",
        ]);
        let st = c.resolve().expect("resolves");
        assert_eq!(st.seeds, ["10.0.0.2:11302", "10.0.0.1:11302"]);
        // insecure_plaintext in the file does not switch the tool to plaintext.
        let e = match common(&["--config", path]).resolve() {
            Ok(_) => panic!("resolved without a certificate"),
            Err(e) => e,
        };
        assert!(e.contains("pass --insecure-plaintext"), "{e}");
        std::fs::write(&cfg, "[server]\n").expect("write");
        let e = match common(&["--config", path]).resolve() {
            Ok(_) => panic!("resolved"),
            Err(e) => e,
        };
        assert!(e.contains("no [cluster] section"), "{e}");
    }
}
