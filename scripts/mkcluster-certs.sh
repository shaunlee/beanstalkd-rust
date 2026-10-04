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
#   admin.pem / admin.key             with the argument "admin": the operator
#                                     certificate for the cluster port's admin
#                                     channel (`beanstalkd-rs cluster`), signed
#                                     by the CA, SAN DNS name "bstk-admin" and
#                                     nothing else, extended key usage
#                                     clientAuth only (nodes refuse it as a
#                                     peer and refuse node certificates as an
#                                     admin)
#
# Usage: scripts/mkcluster-certs.sh DIR ID|admin...
#   DIR is created; an existing cluster-ca.pem / cluster-ca.key pair in it is
#   reused, so a replacement node certificate (or the admin certificate) can
#   be issued later with the same CA (scripts/mkcluster-certs.sh DIR 2, or
#   DIR admin). Existing files for the given IDs (or admin) are overwritten.
# Environment: DAYS (validity of new certificates, default 825), OPENSSL.
#
# The node id is bound by the SAN only; the subject CN is informational and
# not consulted. Keys are ECDSA P-256 in PKCS#8 PEM, created with mode 0600.
# Works with OpenSSL 3 and LibreSSL.
set -euo pipefail

[ $# -ge 2 ] || { echo "usage: $0 DIR ID|admin..." >&2; exit 2; }
DIR="$1"
shift
for id in "$@"; do
  [ "$id" = admin ] && continue
  [[ "$id" =~ ^[1-9][0-9]*$ && "$id" -le 65535 ]] || {
    echo "$0: node id must be 1..65535 (or \"admin\"): $id" >&2
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

# issue NAME SAN EKU: a certificate signed by the CA.
issue() {
  local name="$1" san="$2" eku="$3"
  key "$DIR/$name.key"
  "$OPENSSL" req -new -key "$DIR/$name.key" -subj "/CN=$san" -out "$DIR/$name.csr" 2>/dev/null
  "$OPENSSL" x509 -req -in "$DIR/$name.csr" -CA "$DIR/cluster-ca.pem" -CAkey "$DIR/cluster-ca.key" \
    -set_serial "0x$("$OPENSSL" rand -hex 16)" -days "$DAYS" -sha256 -out "$DIR/$name.pem" \
    -extfile <(printf '%s\n' 'basicConstraints=critical,CA:FALSE' \
      'keyUsage=critical,digitalSignature' "extendedKeyUsage=$eku" \
      "subjectAltName=DNS:$san" 'authorityKeyIdentifier=keyid' \
      'subjectKeyIdentifier=hash') 2>/dev/null
  rm -f "$DIR/$name.csr"
}

for id in "$@"; do
  if [ "$id" = admin ]; then
    issue admin bstk-admin clientAuth
    # A client only: the server purpose must fail.
    "$OPENSSL" verify -purpose sslclient -CAfile "$DIR/cluster-ca.pem" "$DIR/admin.pem" >/dev/null
    if "$OPENSSL" verify -purpose sslserver -CAfile "$DIR/cluster-ca.pem" "$DIR/admin.pem" \
      >/dev/null 2>&1; then
      echo "$0: $DIR/admin.pem unexpectedly allows server authentication" >&2
      exit 1
    fi
    echo "created $DIR/admin.pem (SAN DNS:bstk-admin, client only, valid $DAYS days)"
    continue
  fi
  name="node$id"
  issue "$name" "bstk-node-$id" serverAuth,clientAuth
  # A node presents the same certificate as server and as client.
  "$OPENSSL" verify -purpose sslserver -CAfile "$DIR/cluster-ca.pem" "$DIR/$name.pem" >/dev/null
  "$OPENSSL" verify -purpose sslclient -CAfile "$DIR/cluster-ca.pem" "$DIR/$name.pem" >/dev/null
  echo "created $DIR/$name.pem (SAN DNS:bstk-node-$id, valid $DAYS days)"
done
