#!/bin/bash
set -euo pipefail

# Builds installable deb package for MAGE

SCRIPT_DIR=$(dirname $(realpath $0))

ARCH=$1
BUILD_TYPE=$2
VERSION=$3
MALLOC=$4
CUDA=$5
CUGRAPH=$6
PACKAGE_DIR=${7:-$HOME/mage.tar.gz}

# replace illegal characters in version string for packager:
CLEAN_VERSION=$(echo $VERSION | sed 's/_/+/g')
# and replace anythin preceding the version number
CLEAN_VERSION=$(echo $CLEAN_VERSION | sed 's/^[^0-9]*//')

PACKAGE_NAME="memgraph-mage_${VERSION}-1_${ARCH}"
if [[ "$MALLOC" == true ]]; then
    PACKAGE_NAME="${PACKAGE_NAME}-malloc"
fi
if [[ "$CUGRAPH" == true ]]; then
    PACKAGE_NAME="${PACKAGE_NAME}-cugraph"
    CUDA=true
elif [[ "$CUDA" == true ]]; then
    PACKAGE_NAME="${PACKAGE_NAME}-cuda"
fi
PACKAGE_NAME="${PACKAGE_NAME}.deb"

echo "Building package: $PACKAGE_NAME"
mkdir -pv $SCRIPT_DIR/build/usr/lib/memgraph/query_modules

if [[ "$CUDA" == true ]]; then
    cp -v ../../../../mage/python/requirements-gpu.txt $SCRIPT_DIR/build/usr/lib/memgraph/mage-requirements.txt
else
    cp -v ../../../../mage/python/requirements.txt $SCRIPT_DIR/build/usr/lib/memgraph/mage-requirements.txt
fi
cp ../../../../mage/install_python_requirements.sh $SCRIPT_DIR/build/usr/lib/memgraph/install_python_requirements.sh

tar -xvzf $PACKAGE_DIR -C $SCRIPT_DIR/build/usr/lib/memgraph/

# memgraph-mage-debuginfo.install picks up .debug sidecars from build-debuginfo/.
# Sidecars live in a sibling mage-debug.tar.gz produced alongside mage.tar.gz
# by compress-query-modules.sh (mage.tar.gz itself is built with
# --exclude='*.debug'). Extract it into build-debuginfo/usr/lib/memgraph/ so
# the .install glob's relative paths line up.
DEBUG_PACKAGE_DIR="$(dirname "$PACKAGE_DIR")/mage-debug.tar.gz"
DEBUGINFO_STAGE_ROOT="$SCRIPT_DIR/build-debuginfo/usr/lib/memgraph"
mkdir -pv "$DEBUGINFO_STAGE_ROOT/query_modules"

# Diagnostic: who we are and which tarballs actually sit next to mage.tar.gz.
# A missing or misdetected mage-debug.tar.gz is then obvious in the build log.
echo "build-deb.sh: user=$(id -un) HOME=${HOME:-} PACKAGE_DIR=$PACKAGE_DIR"
ls -la "$(dirname "$PACKAGE_DIR")"/*.tar.gz 2>&1 || true

# Detect the split-debug sidecar. Read the listing into a variable before
# grepping: `tar -tzf … | grep -q` lets grep close the pipe early, and the
# resulting SIGPIPE on tar is fatal under `set -o pipefail`.
HAS_DEBUGINFO=false
if [[ -f "$DEBUG_PACKAGE_DIR" ]]; then
    debug_listing="$(tar -tzf "$DEBUG_PACKAGE_DIR" 2>/dev/null || true)"
    if grep -qm1 '\.debug$' <<<"$debug_listing"; then
        HAS_DEBUGINFO=true
    fi
fi

if [[ "$HAS_DEBUGINFO" == "true" ]]; then
    tar -xvzf "$DEBUG_PACKAGE_DIR" -C "$DEBUGINFO_STAGE_ROOT"
else
    # No sidecars: build-mage ran without --split-debug (which stays optional
    # for MAGE, as it is for Memgraph). Skip the debuginfo deb gracefully.
    echo "::warning::No .debug sidecars at $DEBUG_PACKAGE_DIR — skipping memgraph-mage-debuginfo deb ($BUILD_TYPE build)."
    # Drop the debuginfo stanza from debian/control. Paragraph mode (RS="")
    # treats each stanza as one record; anchor on embedded newlines because
    # plain ^/$ match the whole record, not the Package: line within it.
    awk 'BEGIN{RS="";ORS="\n\n"} $0 !~ /(^|\n)Package: memgraph-mage-debuginfo(\n|$)/' \
        "$SCRIPT_DIR/debian/control" > "$SCRIPT_DIR/debian/control.tmp"
    mv "$SCRIPT_DIR/debian/control.tmp" "$SCRIPT_DIR/debian/control"
    # debhelper's dh_install processes every debian/<pkg>.install file it
    # finds, independent of debian/control's package list — leaving the
    # debuginfo .install in place makes it error out on the missing
    # build-debuginfo/... glob. Remove it so dh_install ignores the
    # (now-absent) debuginfo package.
    rm -f "$SCRIPT_DIR/debian/memgraph-mage-debuginfo.install"
fi

# Replace template variables in Debian control files
sed -i "s/@ARCH@/$ARCH/g" $SCRIPT_DIR/debian/control
sed -i "s/@VERSION@/$CLEAN_VERSION/g" $SCRIPT_DIR/debian/changelog

# set cuda and arch in postinst
sed -i "s/@CUDA@/$CUDA/g" $SCRIPT_DIR/debian/postinst
sed -i "s/@ARCH@/$ARCH/g" $SCRIPT_DIR/debian/postinst

dpkg-buildpackage -us -uc -b

old_name="$(ls ../memgraph-mage_*.deb | grep -v -- '-debuginfo' | head -n 1)"
mv "$old_name" "$PACKAGE_NAME"

if [[ "$HAS_DEBUGINFO" == "true" ]]; then
    debuginfo_old="$(ls ../memgraph-mage-debuginfo_*.deb | head -n 1)"
    debuginfo_new="memgraph-mage-debuginfo_${PACKAGE_NAME#memgraph-mage_}"
    mv "$debuginfo_old" "$debuginfo_new"
fi
