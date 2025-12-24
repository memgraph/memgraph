#!/bin/bash
set -euo pipefail

OUT=build/openssl
STAGE=build/debstage
PKGROOT="$STAGE/openssl"

ARCH="$(dpkg --print-architecture)"

rm -rf "$STAGE"
mkdir -p "$PKGROOT/usr/bin" "$PKGROOT/DEBIAN"

cp -a "$OUT/openssl" "$PKGROOT/usr/bin/"
cp -a "$OUT/c_rehash" "$PKGROOT/usr/bin/"
mkdir -p "$PKGROOT/etc/ssl"
cp -a "tools/openssl/openssl.cnf" "$PKGROOT/etc/ssl/openssl.cnf"
mkdir -p "$PKGROOT/etc/ssl/certs"
mkdir -p "$PKGROOT/etc/ssl/private"
mkdir -p "$PKGROOT/usr/lib"
ln -s /etc/ssl/openssl.cnf "$PKGROOT/usr/lib/openssl.cnf"
ln -s /etc/ssl/certs "$PKGROOT/usr/lib/certs"
ln -s /etc/ssl/private "$PKGROOT/usr/lib/private"
ln -s /etc/ssl/certs/ca-certificates.crt "$PKGROOT/usr/lib/cert.pem"

cat > "$PKGROOT/DEBIAN/control" <<EOF
Package: openssl
Version: 3.5.4-0ubuntu0custom1
Section: utils
Priority: optional
Architecture: $ARCH
Maintainer: Matt James <matthew.james@memgraph.io>
Conflicts: openssl
Replaces: openssl
Provides: openssl
Depends: libssl3t64
Description: Custom OpenSSL CLI from OpenSSL 3.5.4 (Conan build)
EOF

dpkg-deb --build "$PKGROOT" "build/openssl_3.5.4-0ubuntu0custom1_${ARCH}.deb"
