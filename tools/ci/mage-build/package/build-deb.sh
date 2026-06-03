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

# Split the .debug sidecars out of the main package and into a sibling
# staging tree that the memgraph-mage-debuginfo deb's .install file picks up.
# Match both `<name>.so.debug` (no SOVERSION) and `<name>.so.<N>.debug` (with
# SOVERSION — what mage's add_query_module produces). After the move,
# build/ holds the stripped .so files for the main package; build-debuginfo/
# holds the corresponding .debug sidecars for the debuginfo package.
DEBUGINFO_STAGE="$SCRIPT_DIR/build-debuginfo/usr/lib/memgraph/query_modules"
mkdir -pv "$DEBUGINFO_STAGE"
if find "$SCRIPT_DIR/build/usr/lib/memgraph/query_modules" -maxdepth 1 -name '*.so*.debug' -print -quit | grep -q .; then
    HAS_DEBUGINFO=true
    (cd "$SCRIPT_DIR/build/usr/lib/memgraph/query_modules" && \
        find . -name '*.so*.debug' -print | while read -r f; do
            mkdir -p "$DEBUGINFO_STAGE/$(dirname "$f")"
            mv -v "$f" "$DEBUGINFO_STAGE/$f"
        done)
else
    HAS_DEBUGINFO=false
    echo "::warning::No .debug sidecars in $SCRIPT_DIR/build/usr/lib/memgraph/query_modules — skipping memgraph-mage-debuginfo deb. Pass --split-debug to produce it."
    awk 'BEGIN{RS=""; ORS="\n\n"} !/^Package: memgraph-mage-debuginfo$/' \
        "$SCRIPT_DIR/debian/control" > "$SCRIPT_DIR/debian/control.tmp"
    mv "$SCRIPT_DIR/debian/control.tmp" "$SCRIPT_DIR/debian/control"
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
