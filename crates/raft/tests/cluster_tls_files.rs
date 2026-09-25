//! `load_cluster_tls` reads `[cluster.tls]` PEM files.

use std::path::PathBuf;

use bstk_raft::tls::{load_cluster_tls, node_dns_name};
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose,
};

fn dir(name: &str) -> PathBuf {
    let d = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(name);
    std::fs::create_dir_all(&d).expect("mkdir");
    d
}

#[test]
fn loads_pem_files_and_names_the_bad_one() {
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).expect("params");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    let ca_key = KeyPair::generate().expect("key");
    let ca = ca_params.self_signed(&ca_key).expect("ca");

    let mut params = CertificateParams::new(vec![node_dns_name(4)]).expect("params");
    params.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ServerAuth,
        ExtendedKeyUsagePurpose::ClientAuth,
    ];
    let key = KeyPair::generate().expect("key");
    let cert = params.signed_by(&key, &ca, &ca_key).expect("sign");

    let d = dir("cluster_tls_files");
    let (cert_p, key_p, ca_p) = (d.join("node4.pem"), d.join("node4.key"), d.join("ca.pem"));
    std::fs::write(&cert_p, cert.pem()).expect("write");
    std::fs::write(&key_p, key.serialize_pem()).expect("write");
    std::fs::write(&ca_p, ca.pem()).expect("write");

    load_cluster_tls(4, &cert_p, &key_p, &ca_p).expect("loads");

    let e = load_cluster_tls(5, &cert_p, &key_p, &ca_p).expect_err("wrong node");
    assert!(e.to_string().contains("bstk-node-5"), "{e}");

    let missing = d.join("missing.pem");
    let e = load_cluster_tls(4, &cert_p, &key_p, &missing).expect_err("missing ca");
    assert!(e.to_string().contains("cluster.tls.ca"), "{e}");
    assert!(e.to_string().contains("missing.pem"), "{e}");

    let e = load_cluster_tls(4, &cert_p, &ca_p, &ca_p).expect_err("no key");
    assert!(e.to_string().contains("cluster.tls.key"), "{e}");
}
