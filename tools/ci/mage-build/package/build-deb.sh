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

# IMAGE_FLAVOUR controls whether the -relwithdebinfo suffix is applied:
# - "prod" (default) produces the stripped package customers install (no suffix).
# - "debug" produces the symbols-embedded package used for interactive debugging.
# The suffix used to be gated on BUILD_TYPE=RelWithDebInfo alone; now we split
# the RWD build type into two packaging flavours via MG_SPLIT_DEBUG.
IMAGE_FLAVOUR="${IMAGE_FLAVOUR:-prod}"

PACKAGE_NAME="memgraph-mage_${VERSION}-1_${ARCH}"
if [[ "$BUILD_TYPE" == "RelWithDebInfo" && "$IMAGE_FLAVOUR" == "debug" ]]; then
    PACKAGE_NAME="${PACKAGE_NAME}-relwithdebinfo"
fi
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

# For the prod flavour, strip any .debug sidecars that were bundled into the
# tarball by mage/setup. They're only relevant to the debug flavour (symbols
# embedded alongside the .so) or to the external symbol archive.
if [[ "$IMAGE_FLAVOUR" == "prod" ]]; then
    find $SCRIPT_DIR/build/usr/lib/memgraph -name '*.so.debug' -print -delete
fi

# Replace template variables in Debian control files
sed -i "s/@ARCH@/$ARCH/g" $SCRIPT_DIR/debian/control
sed -i "s/@VERSION@/$CLEAN_VERSION/g" $SCRIPT_DIR/debian/changelog

# set cuda in postinst
sed -i "s/@CUDA@/$CUDA/g" $SCRIPT_DIR/debian/postinst

dpkg-buildpackage -us -uc -b

old_name="$(ls ../memgraph-mage*.deb)"
mv $old_name $PACKAGE_NAME
