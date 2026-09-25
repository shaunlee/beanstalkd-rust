//! TLS server configurations for TLS listeners (docs/PLAN.md §5.3
//! decision 2): the certificate chain and private key from `[tls]` (PEM),
//! and for `auth = "mtls"` listeners a client-certificate verifier over
//! the `client_ca` bundle.
//!
//! Two configurations are built from the same certificate: one that never
//! requests a client certificate (TLS listeners with `auth = "none"` or
//! `"token"`), and, only when `client_ca` is set, one that requires a
//! client certificate chaining to that CA (`auth = "mtls"` listeners).
//!
//! The crypto provider (aws-lc-rs, rustls' default) is always passed
//! explicitly rather than taken from the process default.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig};
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};

use crate::config::TlsFiles;

/// The server configurations for every kind of TLS listener.
#[derive(Clone)]
pub struct TlsConfigs {
    /// No client certificate is requested.
    pub plain: Arc<ServerConfig>,
    /// A client certificate signed by `client_ca` is required; `None`
    /// without `client_ca` (then no listener uses mTLS).
    pub mtls: Option<Arc<ServerConfig>>,
}

/// Why TLS material could not be loaded; names the file involved.
#[derive(Debug)]
pub struct TlsError(String);

impl fmt::Display for TlsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for TlsError {}

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::aws_lc_rs::default_provider())
}

/// Loads the PEM files in `files` and builds the server configurations.
pub fn load(files: &TlsFiles) -> Result<TlsConfigs, TlsError> {
    let certs = load_certs(&files.cert)?;
    if certs.is_empty() {
        return Err(TlsError(format!(
            "tls.cert {}: no certificate found",
            files.cert.display()
        )));
    }
    let key = PrivateKeyDer::from_pem_file(&files.key).map_err(|e| {
        TlsError(format!(
            "tls.key {}: cannot load a private key: {e}",
            files.key.display()
        ))
    })?;

    let plain = ServerConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(|e| TlsError(format!("TLS protocol versions: {e}")))?
        .with_no_client_auth()
        .with_single_cert(certs.clone(), key.clone_key())
        .map_err(|e| cert_key_error(&files.cert, &files.key, &e))?;

    let mtls = match &files.client_ca {
        None => None,
        Some(ca_path) => {
            let mut roots = RootCertStore::empty();
            for ca in load_certs(ca_path)? {
                roots
                    .add(ca)
                    .map_err(|e| TlsError(format!("tls.client_ca {}: {e}", ca_path.display())))?;
            }
            if roots.is_empty() {
                return Err(TlsError(format!(
                    "tls.client_ca {}: no certificate found",
                    ca_path.display()
                )));
            }
            let verifier = WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider())
                .build()
                .map_err(|e| TlsError(format!("tls.client_ca {}: {e}", ca_path.display())))?;
            let config = ServerConfig::builder_with_provider(provider())
                .with_safe_default_protocol_versions()
                .map_err(|e| TlsError(format!("TLS protocol versions: {e}")))?
                .with_client_cert_verifier(verifier)
                .with_single_cert(certs, key)
                .map_err(|e| cert_key_error(&files.cert, &files.key, &e))?;
            Some(Arc::new(config))
        }
    };

    Ok(TlsConfigs {
        plain: Arc::new(plain),
        mtls,
    })
}

fn load_certs(path: &PathBuf) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    CertificateDer::pem_file_iter(path)
        .and_then(Iterator::collect::<Result<Vec<_>, _>>)
        .map_err(|e| {
            TlsError(format!(
                "{}: cannot load PEM certificates: {e}",
                path.display()
            ))
        })
}

fn cert_key_error(cert: &Path, key: &Path, e: &rustls::Error) -> TlsError {
    TlsError(format!(
        "tls.cert {} / tls.key {}: {e}",
        cert.display(),
        key.display()
    ))
}
