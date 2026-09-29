#!/usr/bin/env bash
# Regenerate the TLS test fixtures: a CA, two server certificates it signed
# (the second one is what a reload test swaps in), a client certificate for
# mTLS, and a second CA with its own client certificate that the first CA must
# refuse. Test-only material — never trust these anywhere else.
#
# Validity is a century so the suites never start failing on a calendar date.
set -euo pipefail

cd "$(dirname "$0")"
DAYS=36500

key() { openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$1" 2>/dev/null; }

ca() {
  local name=$1
  key "$name-key.pem"
  openssl req -x509 -new -key "$name-key.pem" -days "$DAYS" -subj "/CN=flexiq test $name" \
    -addext "basicConstraints=critical,CA:TRUE" \
    -addext "keyUsage=critical,keyCertSign,cRLSign" \
    -out "$name.pem"
}

# leaf <name> <signing ca> <extendedKeyUsage> <subjectAltName or empty>
leaf() {
  local name=$1 signer=$2 usage=$3 san=$4
  key "$name-key.pem"
  openssl req -new -key "$name-key.pem" -subj "/CN=flexiq test $name" -out "$name.csr"
  {
    echo "basicConstraints=critical,CA:FALSE"
    echo "keyUsage=critical,digitalSignature"
    echo "extendedKeyUsage=$usage"
    [ -n "$san" ] && echo "subjectAltName=$san"
  } >"$name.ext"
  openssl x509 -req -in "$name.csr" -CA "$signer.pem" -CAkey "$signer-key.pem" \
    -CAcreateserial -days "$DAYS" -extfile "$name.ext" -out "$name.pem" 2>/dev/null
  rm -f "$name.csr" "$name.ext"
}

SAN="DNS:localhost,IP:127.0.0.1,IP:::1"
ca ca
ca rogue-ca
leaf server ca serverAuth "$SAN"
leaf server-rotated ca serverAuth "$SAN"
leaf client ca clientAuth ""
leaf rogue-client rogue-ca clientAuth ""
rm -f ./*.srl
