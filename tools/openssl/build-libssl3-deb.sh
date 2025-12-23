#!/bin/bash
set -euo pipefail

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
cp -a "$OUT/engines-3 /usr/lib/$MULTIARCH/"
cp -a "$OUT/ossl_modules /usr/lib/$MULTIARCH/"

strip --strip-unneeded "$PKGROOT/usr/lib/$MULTIARCH/"libssl.so.3 "$PKGROOT/usr/lib/$MULTIARCH/"libcrypto.so.3 || true

cat > "$PKGROOT/DEBIAN/control" <<EOF
Package: libssl3t64
Version: 3.5.4-0ubuntu0custom1
Section: libs
Priority: required
Architecture: $ARCH
Maintainer: Matt James <matthew.james@memgraph.io>
Conflicts: libssl3t64
Replaces: libssl3t64
Provides: libssl3
Description: Custom libssl/libcrypto from OpenSSL 3.5.4 (Conan build)
EOF

cat > "$PKGROOT/DEBIAN/postinst" <<'EOF'
#!/bin/sh
set -e
ldconfig
EOF
chmod 0755 "$PKGROOT/DEBIAN/postinst"

dpkg-deb --build "$PKGROOT" "build/libssl3t64_3.5.4-0ubuntu0custom1_${ARCH}.deb"
