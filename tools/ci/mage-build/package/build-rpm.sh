#!/bin/bash
set -euo pipefail

# Builds installable rpm package(s) for MAGE. RPM counterpart of build-deb.sh:
# stages the payload, renders rpm/memgraph-mage.spec.in, and runs rpmbuild.

SCRIPT_DIR=$(dirname $(realpath $0))

ARCH=$1            # rpm arch: x86_64 / aarch64
PKG_ARCH=$2        # dpkg-style arch passed to install_python_requirements.sh: amd64 / arm64
BUILD_TYPE=$3
VERSION=$4
MALLOC=$5
CUDA=$6
CUGRAPH=$7
PACKAGE_DIR=${8:-$HOME/mage.tar.gz}

# The RPM Version field must not contain '-' (it delimits version-release), so
# normalise it to '.', then strip anything preceding the version number.
CLEAN_VERSION=$(echo "$VERSION" | sed 's/-/./g')
CLEAN_VERSION=$(echo "$CLEAN_VERSION" | sed 's/^[^0-9]*//')

# Variant suffix, encoded in the output filename only (the package Name stays
# memgraph-mage), matching build-deb.sh's PACKAGE_NAME convention.
NAME_SUFFIX=""
if [[ "$MALLOC" == true ]]; then
    NAME_SUFFIX="${NAME_SUFFIX}-malloc"
fi
if [[ "$CUGRAPH" == true ]]; then
    NAME_SUFFIX="${NAME_SUFFIX}-cugraph"
    CUDA=true
elif [[ "$CUDA" == true ]]; then
    NAME_SUFFIX="${NAME_SUFFIX}-cuda"
fi

PACKAGE_NAME="memgraph-mage-${CLEAN_VERSION}-1.${ARCH}${NAME_SUFFIX}.rpm"
DEBUGINFO_PACKAGE_NAME="memgraph-mage-debuginfo-${CLEAN_VERSION}-1.${ARCH}${NAME_SUFFIX}.rpm"

echo "Building package: $PACKAGE_NAME"

# Fresh rpmbuild tree under the package dir; STAGE_ROOT is the payload tree
# rooted at "/" that the spec's %install copies into the buildroot.
TOPDIR="$SCRIPT_DIR/rpmbuild"
STAGE_ROOT="$SCRIPT_DIR/build-rpm-root"
DEBUGINFO_STAGE_ROOT="$SCRIPT_DIR/build-rpm-debuginfo-root"
rm -rf "$TOPDIR" "$STAGE_ROOT" "$DEBUGINFO_STAGE_ROOT"
mkdir -pv "$TOPDIR"/{BUILD,RPMS,SOURCES,SPECS,SRPMS}
mkdir -pv "$STAGE_ROOT/usr/lib/memgraph/query_modules"

if [[ "$CUDA" == true ]]; then
    cp -v ../../../../mage/python/requirements-gpu.txt "$STAGE_ROOT/usr/lib/memgraph/mage-requirements.txt"
else
    cp -v ../../../../mage/python/requirements.txt "$STAGE_ROOT/usr/lib/memgraph/mage-requirements.txt"
fi
cp ../../../../mage/install_python_requirements.sh "$STAGE_ROOT/usr/lib/memgraph/install_python_requirements.sh"

tar -xvzf "$PACKAGE_DIR" -C "$STAGE_ROOT/usr/lib/memgraph/"

# Detect the split-debug sidecar tarball produced alongside mage.tar.gz by
# compress-query-modules.sh. Read the listing into a variable before grepping:
# `tar -tzf … | grep -q` lets grep close the pipe early and the resulting
# SIGPIPE on tar is fatal under `set -o pipefail`. (Same dance as build-deb.sh.)
DEBUG_PACKAGE_DIR="$(dirname "$PACKAGE_DIR")/mage-debug.tar.gz"
echo "build-rpm.sh: user=$(id -un) HOME=${HOME:-} PACKAGE_DIR=$PACKAGE_DIR"
ls -la "$(dirname "$PACKAGE_DIR")"/*.tar.gz 2>&1 || true

HAS_DEBUGINFO=false
if [[ -f "$DEBUG_PACKAGE_DIR" ]]; then
    debug_listing="$(tar -tzf "$DEBUG_PACKAGE_DIR" 2>/dev/null || true)"
    if grep -qm1 '\.debug$' <<<"$debug_listing"; then
        HAS_DEBUGINFO=true
    fi
fi

WITH_DEBUGINFO=0
if [[ "$HAS_DEBUGINFO" == "true" ]]; then
    WITH_DEBUGINFO=1
    mkdir -pv "$DEBUGINFO_STAGE_ROOT/usr/lib/memgraph/query_modules"
    tar -xvzf "$DEBUG_PACKAGE_DIR" -C "$DEBUGINFO_STAGE_ROOT/usr/lib/memgraph"
else
    # No sidecars: build-mage ran without --split-debug (optional for MAGE, as
    # it is for Memgraph). The spec drops the debuginfo subpackage when
    # with_debuginfo is 0.
    echo "::warning::No .debug sidecars at $DEBUG_PACKAGE_DIR — skipping memgraph-mage-debuginfo rpm ($BUILD_TYPE build)."
fi

# Render the spec from the template.
SPEC="$TOPDIR/SPECS/memgraph-mage.spec"
cp "$SCRIPT_DIR/rpm/memgraph-mage.spec.in" "$SPEC"
sed -i "s/@VERSION@/$CLEAN_VERSION/g" "$SPEC"
sed -i "s/@ARCH@/$ARCH/g" "$SPEC"
sed -i "s/@PKG_ARCH@/$PKG_ARCH/g" "$SPEC"
sed -i "s/@CUDA@/$CUDA/g" "$SPEC"
sed -i "s/@WITH_DEBUGINFO@/$WITH_DEBUGINFO/g" "$SPEC"

rpmbuild -bb \
    --define "_topdir $TOPDIR" \
    --define "stage_root $STAGE_ROOT" \
    --define "debug_stage_root $DEBUGINFO_STAGE_ROOT" \
    --target "$ARCH" \
    "$SPEC"

# rpmbuild drops the artefacts under RPMS/<arch>/; rename them to the variant
# filenames build-rpm.sh promises (parallel to build-deb.sh's final mv).
built_main="$(ls "$TOPDIR"/RPMS/*/memgraph-mage-[0-9]*.rpm | grep -v -- '-debuginfo' | head -n 1)"
mv "$built_main" "$SCRIPT_DIR/$PACKAGE_NAME"
echo "Package: $SCRIPT_DIR/$PACKAGE_NAME"

if [[ "$HAS_DEBUGINFO" == "true" ]]; then
    built_debuginfo="$(ls "$TOPDIR"/RPMS/*/memgraph-mage-debuginfo-*.rpm | head -n 1)"
    mv "$built_debuginfo" "$SCRIPT_DIR/$DEBUGINFO_PACKAGE_NAME"
    echo "Package: $SCRIPT_DIR/$DEBUGINFO_PACKAGE_NAME"
fi
