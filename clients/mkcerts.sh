#!/usr/bin/env bash
# Generates throwaway TLS material for the smoke tests and benchmarks:
#
#   ca.pem / ca.key            test CA
#   server.pem / server.key    server leaf signed by the CA, SANs
#                              DNS:localhost and IP:127.0.0.1 (serverAuth)
#   client.pem / client.key    client leaf signed by the CA (clientAuth)
#   rogue-client.pem / .key    client leaf signed by an unrelated CA
#
# Usage: clients/mkcerts.sh DIR   (DIR is created; existing files are
# overwritten). Keys are ECDSA P-256 in PKCS#8 PEM, certificates valid for
# 2 days. Works with OpenSSL 3 and LibreSSL (extensions via -extfile).
set -euo pipefail

[ $# -eq 1 ] || { echo "usage: $0 DIR" >&2; exit 2; }
DIR="$1"
mkdir -p "$DIR"
OPENSSL="${OPENSSL:-openssl}"

key() { "$OPENSSL" genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$1" 2>/dev/null; }

# ca NAME CN
ca() {
  key "$DIR/$1.key"
  "$OPENSSL" req -x509 -new -key "$DIR/$1.key" -subj "/CN=$2" -days 2 -sha256 \
    -extensions v3_ca -config <(printf '%s\n' '[req]' 'distinguished_name=dn' '[dn]' \
      '[v3_ca]' 'basicConstraints=critical,CA:TRUE' 'keyUsage=critical,keyCertSign,cRLSign,digitalSignature' \
      'subjectKeyIdentifier=hash') \
    -out "$DIR/$1.pem" 2>/dev/null
}

# leaf NAME CN CA EXTENSIONS...
leaf() {
  local name="$1" cn="$2" ca="$3"; shift 3
  key "$DIR/$name.key"
  "$OPENSSL" req -new -key "$DIR/$name.key" -subj "/CN=$cn" -out "$DIR/$name.csr" 2>/dev/null
  "$OPENSSL" x509 -req -in "$DIR/$name.csr" -CA "$DIR/$ca.pem" -CAkey "$DIR/$ca.key" \
    -CAcreateserial -days 2 -sha256 -out "$DIR/$name.pem" \
    -extfile <(printf '%s\n' 'basicConstraints=critical,CA:FALSE' \
      'keyUsage=critical,digitalSignature' "$@") 2>/dev/null
  rm -f "$DIR/$name.csr"
}

ca ca "bstk smoke CA"
ca rogue-ca "bstk untrusted CA"
leaf server localhost ca 'extendedKeyUsage=serverAuth' 'subjectAltName=DNS:localhost,IP:127.0.0.1'
leaf client bstk-client ca 'extendedKeyUsage=clientAuth'
leaf rogue-client bstk-client rogue-ca 'extendedKeyUsage=clientAuth'
rm -f "$DIR"/*.srl "$DIR/rogue-ca.key"
"$OPENSSL" verify -CAfile "$DIR/ca.pem" "$DIR/server.pem" "$DIR/client.pem" >/dev/null
