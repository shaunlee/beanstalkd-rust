//! Mutual TLS for the cluster port (docs/DESIGN.md §8, `[cluster.tls]`).
//!
//! One certificate per node, signed by the cluster CA and carrying the SAN DNS
//! name `bstk-node-<id>` ([`node_dns_name`]; the CN is not consulted), serves
//! as both the listener's server certificate and the dialer's client
//! certificate. The dialer verifies the listener as `bstk-node-<target id>`.
//! The listener requires a CA-signed client certificate during the handshake,
//! then checks after the hello that it is valid for `bstk-node-<hello id>` and
//! that the id is a configured peer ([`verify_peer_identity`]), so nothing is
//! processed for an unverified identity. rustls 0.23 with the aws-lc-rs
//! provider is passed explicitly, not taken from the process default.
//!
//! Operator tools (the admin channel, protocol version 4) present a client
//! certificate from the same CA whose only SAN DNS name is [`ADMIN_DNS_NAME`]
//! ([`verify_admin_identity`]). The two identities exclude each other: a
//! certificate carrying the admin name is refused as a node and one carrying
//! any other name is refused as an admin, so a node's key cannot change
//! membership and the admin key cannot replicate or forward.

use std::fmt;
use std::path::Path;
use std::sync::Arc;

use rustls::server::WebPkiClientVerifier;
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer, ServerName};

use crate::NodeId;

pub fn node_dns_name(id: NodeId) -> String {
    format!("bstk-node-{id}")
}

/// The SAN DNS name of the operator (admin channel) certificate.
pub const ADMIN_DNS_NAME: &str = "bstk-admin";

/// The TLS configurations of one node: a client config for dialing peers
/// and a server config for the cluster listener. Both present the node's
/// certificate and trust only the cluster CA.
#[derive(Clone)]
pub struct ClusterTls {
    pub client: Arc<ClientConfig>,
    pub server: Arc<ServerConfig>,
}

impl fmt::Debug for ClusterTls {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ClusterTls")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsError(pub String);

impl fmt::Display for TlsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for TlsError {}

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::aws_lc_rs::default_provider())
}

/// Loads `[cluster.tls]` (PEM files) for node `own_id`. Fails if the
/// node's own certificate is not valid for `bstk-node-<own_id>`.
pub fn load_cluster_tls(
    own_id: NodeId,
    cert: &Path,
    key: &Path,
    ca: &Path,
) -> Result<ClusterTls, TlsError> {
    let read = |what: &str, p: &Path| {
        std::fs::read(p).map_err(|e| TlsError(format!("cluster.tls.{what} {}: {e}", p.display())))
    };
    cluster_tls_from_pem(
        own_id,
        &read("cert", cert)?,
        &read("key", key)?,
        &read("ca", ca)?,
    )
}

pub fn cluster_tls_from_pem(
    own_id: NodeId,
    cert_pem: &[u8],
    key_pem: &[u8],
    ca_pem: &[u8],
) -> Result<ClusterTls, TlsError> {
    let certs = parse_certs("cert", cert_pem)?;
    let Some(leaf) = certs.first() else {
        return Err(TlsError("cluster.tls.cert: no certificate found".into()));
    };
    check_name(leaf, own_id).map_err(|e| TlsError(format!("cluster.tls.cert: {e}")))?;
    let key = PrivateKeyDer::from_pem_slice(key_pem)
        .map_err(|e| TlsError(format!("cluster.tls.key: cannot load a private key: {e}")))?;

    let mut roots = RootCertStore::empty();
    for ca in parse_certs("ca", ca_pem)? {
        roots
            .add(ca)
            .map_err(|e| TlsError(format!("cluster.tls.ca: {e}")))?;
    }
    if roots.is_empty() {
        return Err(TlsError("cluster.tls.ca: no certificate found".into()));
    }
    let roots = Arc::new(roots);

    let verifier = WebPkiClientVerifier::builder_with_provider(roots.clone(), provider())
        .build()
        .map_err(|e| TlsError(format!("cluster.tls.ca: {e}")))?;
    let server = ServerConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(|e| TlsError(format!("TLS protocol versions: {e}")))?
        .with_client_cert_verifier(verifier)
        .with_single_cert(certs.clone(), key.clone_key())
        .map_err(|e| TlsError(format!("cluster.tls.cert / key: {e}")))?;

    let client = ClientConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(|e| TlsError(format!("TLS protocol versions: {e}")))?
        .with_root_certificates(roots)
        .with_client_auth_cert(certs, key)
        .map_err(|e| TlsError(format!("cluster.tls.cert / key: {e}")))?;

    Ok(ClusterTls {
        client: Arc::new(client),
        server: Arc::new(server),
    })
}

fn parse_certs(what: &str, pem: &[u8]) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    CertificateDer::pem_slice_iter(pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            TlsError(format!(
                "cluster.tls.{what}: cannot load PEM certificates: {e}"
            ))
        })
}

fn check_name(cert: &CertificateDer<'_>, id: NodeId) -> Result<(), String> {
    let name = node_dns_name(id);
    let server_name =
        ServerName::try_from(name.as_str()).map_err(|e| format!("invalid name {name}: {e}"))?;
    let ee = webpki::EndEntityCert::try_from(cert)
        .map_err(|e| format!("cannot parse certificate: {e}"))?;
    ee.verify_is_valid_for_subject_name(&server_name)
        .map_err(|_| format!("certificate is not valid for {name}"))
}

/// The SAN DNS names of `cert` (webpki's parse; only for the exclusion
/// checks, never instead of `verify_is_valid_for_subject_name`).
fn dns_names(cert: &CertificateDer<'_>) -> Result<Vec<String>, String> {
    let ee = webpki::EndEntityCert::try_from(cert)
        .map_err(|e| format!("cannot parse certificate: {e}"))?;
    Ok(ee.valid_dns_names().map(str::to_string).collect())
}

/// The listener's check after the hello: the (already CA-verified) client
/// certificate chain `certs` must be valid for `bstk-node-<id>` and must
/// not be an admin certificate.
pub fn verify_peer_identity(
    certs: Option<&[CertificateDer<'_>]>,
    id: NodeId,
) -> Result<(), String> {
    let Some(leaf) = certs.and_then(|c| c.first()) else {
        return Err("no client certificate".into());
    };
    check_name(leaf, id)?;
    if dns_names(leaf)?.iter().any(|n| n == ADMIN_DNS_NAME) {
        return Err(format!(
            "certificate carries the admin name {ADMIN_DNS_NAME}: refused as a node"
        ));
    }
    Ok(())
}

/// The listener's check after an admin hello: the (already CA-verified)
/// client certificate must be valid for [`ADMIN_DNS_NAME`] and carry no
/// other DNS name (a node certificate is refused).
pub fn verify_admin_identity(certs: Option<&[CertificateDer<'_>]>) -> Result<(), String> {
    let Some(leaf) = certs.and_then(|c| c.first()) else {
        return Err("no client certificate".into());
    };
    let name = ServerName::try_from(ADMIN_DNS_NAME).map_err(|e| e.to_string())?;
    webpki::EndEntityCert::try_from(leaf)
        .map_err(|e| format!("cannot parse certificate: {e}"))?
        .verify_is_valid_for_subject_name(&name)
        .map_err(|_| format!("certificate is not valid for {ADMIN_DNS_NAME}"))?;
    let names = dns_names(leaf)?;
    if names.iter().any(|n| n != ADMIN_DNS_NAME) {
        return Err(format!(
            "an admin certificate must carry only the name {ADMIN_DNS_NAME}, not {names:?}"
        ));
    }
    Ok(())
}

pub fn server_name_for(target: NodeId) -> Result<ServerName<'static>, String> {
    ServerName::try_from(node_dns_name(target)).map_err(|e| format!("invalid server name: {e}"))
}
