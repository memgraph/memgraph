#!/bin/bash
set -euo pipefail

OUT=build/openssl
STAGE=build/debstage
PKGROOT="$STAGE/openssl"

ARCH="$(dpkg --print-architecture)"

rm -rf "$STAGE"
mkdir -p "$PKGROOT/usr/bin" "$PKGROOT/DEBIAN"

cp -a "$OUT/openssl" "$PKGROOT/usr/bin/"

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
