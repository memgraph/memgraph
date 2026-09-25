#!/bin/bash
set -euo pipefail

VERSION="$1"
OUT=build/openssl
STAGE=build/debstage
PKGROOT="$STAGE/libssl3"

ARCH="$(dpkg --print-architecture)"
MULTIARCH="$(dpkg-architecture -qDEB_HOST_MULTIARCH)"

rm -rf "$STAGE"
mkdir -p "$PKGROOT/usr/lib/$MULTIARCH" "$PKGROOT/DEBIAN"

# Copy libs from your Conan deploy output; adjust if needed
cp -a "$OUT/"libssl.so.3* "$PKGROOT/usr/lib/$MULTIARCH/"
cp -a "$OUT/"libcrypto.so.3* "$PKGROOT/usr/lib/$MULTIARCH/"
cp -a "$OUT/"engines-3 "$PKGROOT/usr/lib/$MULTIARCH/"
cp -a "$OUT/"ossl-modules "$PKGROOT/usr/lib/$MULTIARCH/"

strip --strip-unneeded "$PKGROOT/usr/lib/$MULTIARCH/"libssl.so.3 "$PKGROOT/usr/lib/$MULTIARCH/"libcrypto.so.3 || true

# The Provides must be versioned: an unversioned one cannot satisfy a versioned
# dependency, and Ubuntu's libxmlsec1t64-openssl needs "libssl3 (>= 3.0.0)".
# With a bare "Provides: libssl3" apt calls that unsatisfiable and refuses to
# install it alongside this package. Stock libssl3t64 declares "libssl3 (= ...)".
# (DEBIAN/control takes no comment lines, hence this note living out here.)
cat > "$PKGROOT/DEBIAN/control" <<EOF
Package: libssl3t64
Version: $VERSION-0ubuntu0custom1
Section: libs
Priority: required
Architecture: $ARCH
Maintainer: Matt James <matthew.james@memgraph.io>
Conflicts: libssl3t64
Replaces: libssl3t64
Provides: libssl3 (= $VERSION-0ubuntu0custom1)
Description: Custom libssl/libcrypto from OpenSSL $VERSION (Conan build)
EOF

cat > "$PKGROOT/DEBIAN/postinst" <<'EOF'
#!/bin/sh
set -e
ldconfig
EOF
chmod 0755 "$PKGROOT/DEBIAN/postinst"

dpkg-deb --build "$PKGROOT" "build/libssl3t64_${VERSION}-0ubuntu0custom1_${ARCH}.deb"
