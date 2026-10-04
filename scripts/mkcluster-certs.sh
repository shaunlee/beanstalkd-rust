#!/usr/bin/env bash
# Generates the mutual-TLS material for a beanstalkd-rs cluster
# ([cluster.tls], docs/OPERATIONS.md "Cluster"):
#
#   cluster-ca.pem / cluster-ca.key   cluster CA (keep the key offline: whoever
#                                     holds it can join the cluster)
#   node<ID>.pem / node<ID>.key       one certificate per node, signed by the
#                                     CA, SAN DNS name "bstk-node-<ID>",
#                                     extended key usage serverAuth and
#                                     clientAuth (each node is both a server
#                                     and a client of its peers)
#
# Usage: scripts/mkcluster-certs.sh DIR ID...
#   DIR is created; an existing cluster-ca.pem / cluster-ca.key pair in it is
#   reused, so a replacement node certificate can be issued later with the
#   same CA (scripts/mkcluster-certs.sh DIR 2). Existing node files for the
#   given IDs are overwritten.
# Environment: DAYS (validity of new certificates, default 825), OPENSSL.
#
# The node id is bound by the SAN only; the subject CN is informational and
# not consulted. Keys are ECDSA P-256 in PKCS#8 PEM, created with mode 0600.
# Works with OpenSSL 3 and LibreSSL.
set -euo pipefail

[ $# -ge 2 ] || { echo "usage: $0 DIR ID..." >&2; exit 2; }
DIR="$1"
shift
for id in "$@"; do
  [[ "$id" =~ ^[1-9][0-9]*$ && "$id" -le 65535 ]] || {
    echo "$0: node id must be 1..65535: $id" >&2
    exit 2
  }
done
DAYS="${DAYS:-825}"
OPENSSL="${OPENSSL:-openssl}"
mkdir -p "$DIR"

key() {
  (umask 077 && "$OPENSSL" genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$1" 2>/dev/null)
}

if [ -f "$DIR/cluster-ca.pem" ] && [ -f "$DIR/cluster-ca.key" ]; then
  echo "reusing $DIR/cluster-ca.pem"
else
  key "$DIR/cluster-ca.key"
  "$OPENSSL" req -x509 -new -key "$DIR/cluster-ca.key" -subj "/CN=beanstalkd-rs cluster CA" \
    -days "$DAYS" -sha256 -extensions v3_ca \
    -config <(printf '%s\n' '[req]' 'distinguished_name=dn' '[dn]' '[v3_ca]' \
      'basicConstraints=critical,CA:TRUE,pathlen:0' 'keyUsage=critical,keyCertSign,cRLSign' \
      'subjectKeyIdentifier=hash') \
    -out "$DIR/cluster-ca.pem" 2>/dev/null
  echo "created $DIR/cluster-ca.pem"
fi

for id in "$@"; do
  name="node$id"
  key "$DIR/$name.key"
  "$OPENSSL" req -new -key "$DIR/$name.key" -subj "/CN=bstk-node-$id" -out "$DIR/$name.csr" 2>/dev/null
  "$OPENSSL" x509 -req -in "$DIR/$name.csr" -CA "$DIR/cluster-ca.pem" -CAkey "$DIR/cluster-ca.key" \
    -set_serial "0x$("$OPENSSL" rand -hex 16)" -days "$DAYS" -sha256 -out "$DIR/$name.pem" \
    -extfile <(printf '%s\n' 'basicConstraints=critical,CA:FALSE' \
      'keyUsage=critical,digitalSignature' 'extendedKeyUsage=serverAuth,clientAuth' \
      "subjectAltName=DNS:bstk-node-$id" 'authorityKeyIdentifier=keyid' \
      'subjectKeyIdentifier=hash') 2>/dev/null
  rm -f "$DIR/$name.csr"
  # A node presents the same certificate as server and as client.
  "$OPENSSL" verify -purpose sslserver -CAfile "$DIR/cluster-ca.pem" "$DIR/$name.pem" >/dev/null
  "$OPENSSL" verify -purpose sslclient -CAfile "$DIR/cluster-ca.pem" "$DIR/$name.pem" >/dev/null
  echo "created $DIR/$name.pem (SAN DNS:bstk-node-$id, valid $DAYS days)"
done
