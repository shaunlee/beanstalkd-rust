//! Throwaway TLS material for the harness's TLS mode.
//!
//! [`TlsMaterial::generate`] creates, in a fresh temporary directory, a
//! self-signed CA and a server certificate signed by it (SANs `localhost`
//! and `127.0.0.1`), written as PEM files that a server under test (or
//! stunnel) can load:
//!
//! - `ca.pem`: the CA certificate;
//! - `server.pem`: the server (leaf) certificate;
//! - `server.key`: the server private key (PKCS#8 PEM).
//!
//! It also builds the matching rustls [`ClientConfig`], which trusts only
//! that CA. The material is generated once per run and shared (via `Arc`)
//! by every case; the directory is removed when the last reference is
//! dropped.
//!
//! The crypto provider is always passed explicitly (aws-lc-rs), never taken
//! from the process default, so this keeps working if feature unification
//! across the workspace ever enables a second rustls provider.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};
use rustls::ClientConfig;
use rustls::pki_types::ServerName;

/// The server name the harness's TLS clients connect with (SNI and
/// certificate verification). The TCP address is always `127.0.0.1`.
pub const TLS_SERVER_NAME: &str = "localhost";

/// A throwaway CA plus server certificate, as files and as a client config.
pub struct TlsMaterial {
    dir: PathBuf,
    ca_path: PathBuf,
    cert_path: PathBuf,
    key_path: PathBuf,
    client_config: Arc<ClientConfig>,
}

impl std::fmt::Debug for TlsMaterial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsMaterial")
            .field("dir", &self.dir)
            .finish_non_exhaustive()
    }
}

impl TlsMaterial {
    /// Generate a fresh CA and server certificate in a new temporary
    /// directory and build the client configuration trusting the CA.
    pub fn generate() -> Result<Self, String> {
        let dir = create_temp_dir("bstk-compat-tls")
            .map_err(|e| format!("could not create TLS temp directory: {e}"))?;
        // Remove the directory again if anything below fails.
        let guard = DirGuard(Some(dir.clone()));

        let err = |what: &str, e: &dyn std::fmt::Display| format!("{what}: {e}");

        let mut ca_params =
            CertificateParams::new(Vec::<String>::new()).map_err(|e| err("CA params", &e))?;
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params
            .distinguished_name
            .push(DnType::CommonName, "bstk-compat throwaway test CA");
        ca_params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
            KeyUsagePurpose::DigitalSignature,
        ];
        let ca_key = KeyPair::generate().map_err(|e| err("CA key", &e))?;
        let ca_cert = ca_params
            .self_signed(&ca_key)
            .map_err(|e| err("CA certificate", &e))?;

        let mut server_params =
            CertificateParams::new(vec![TLS_SERVER_NAME.to_string(), "127.0.0.1".to_string()])
                .map_err(|e| err("server params", &e))?;
        server_params
            .distinguished_name
            .push(DnType::CommonName, TLS_SERVER_NAME);
        server_params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyEncipherment,
        ];
        server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let server_key = KeyPair::generate().map_err(|e| err("server key", &e))?;
        let server_cert = server_params
            .signed_by(&server_key, &ca_cert, &ca_key)
            .map_err(|e| err("server certificate", &e))?;

        let ca_path = dir.join("ca.pem");
        let cert_path = dir.join("server.pem");
        let key_path = dir.join("server.key");
        std::fs::write(&ca_path, ca_cert.pem()).map_err(|e| err("write ca.pem", &e))?;
        std::fs::write(&cert_path, server_cert.pem()).map_err(|e| err("write server.pem", &e))?;
        std::fs::write(&key_path, server_key.serialize_pem())
            .map_err(|e| err("write server.key", &e))?;

        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(ca_cert.der().clone())
            .map_err(|e| err("trust CA", &e))?;
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let client_config = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|e| err("client protocol versions", &e))?
            .with_root_certificates(roots)
            .with_no_client_auth();

        let dir = guard.defuse();
        Ok(TlsMaterial {
            dir,
            ca_path,
            cert_path,
            key_path,
            client_config: Arc::new(client_config),
        })
    }

    /// The temporary directory holding the PEM files.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Path of the CA certificate (PEM).
    pub fn ca_path(&self) -> &Path {
        &self.ca_path
    }

    /// Path of the server certificate (PEM).
    pub fn cert_path(&self) -> &Path {
        &self.cert_path
    }

    /// Path of the server private key (PKCS#8 PEM).
    pub fn key_path(&self) -> &Path {
        &self.key_path
    }

    /// Client configuration trusting only the generated CA.
    pub fn client_config(&self) -> Arc<ClientConfig> {
        Arc::clone(&self.client_config)
    }

    /// The server name to verify the server certificate against.
    pub fn server_name() -> ServerName<'static> {
        ServerName::try_from(TLS_SERVER_NAME).expect("TLS_SERVER_NAME is a valid DNS name")
    }
}

impl Drop for TlsMaterial {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Removes a directory on drop unless defused.
struct DirGuard(Option<PathBuf>);

impl DirGuard {
    fn defuse(mut self) -> PathBuf {
        self.0.take().expect("DirGuard is defused only once")
    }
}

impl Drop for DirGuard {
    fn drop(&mut self) {
        if let Some(dir) = self.0.take() {
            let _ = std::fs::remove_dir_all(dir);
        }
    }
}

/// Create a fresh, empty directory `<tmp>/<prefix>-<pid>-<n>`.
pub(crate) fn create_temp_dir(prefix: &str) -> std::io::Result<PathBuf> {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let base = std::env::temp_dir();
    loop {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = base.join(format!("{prefix}-{}-{n}", std::process::id()));
        match std::fs::create_dir(&path) {
            Ok(()) => return Ok(path),
            // Left over from an earlier run that reused our pid.
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn material_files_exist_and_dir_is_removed_on_drop() {
        let m = TlsMaterial::generate().expect("generate");
        let dir = m.dir().to_path_buf();
        for p in [m.ca_path(), m.cert_path(), m.key_path()] {
            let text = std::fs::read_to_string(p).expect("read pem");
            assert!(text.starts_with("-----BEGIN "), "{}", p.display());
        }
        drop(m);
        assert!(!dir.exists());
    }
}
