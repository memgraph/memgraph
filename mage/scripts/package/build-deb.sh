#!/bin/bash
set -euo pipefail

SCRIPT_DIR=$(dirname $(realpath $0))

ARCH=$1
BUILD_TYPE=$2
VERSION=$3
MALLOC=$4
CUDA=$5
PACKAGE_DIR=${6:-$HOME/mage.tar.gz}

# replace illegal characters in version string for packager:
CLEAN_VERSION=$(echo $VERSION | sed 's/_/+/g')
# and replace anythin preceding the version number
CLEAN_VERSION=$(echo $CLEAN_VERSION | sed 's/^[^0-9]*//')

PACKAGE_NAME="memgraph-mage_${VERSION}-1_${ARCH}"
if [[ "$BUILD_TYPE" == "RelWithDebInfo" ]]; then
    PACKAGE_NAME="${PACKAGE_NAME}-relwithdebinfo"
fi
if [[ "$MALLOC" == true ]]; then
    PACKAGE_NAME="${PACKAGE_NAME}-malloc"
fi
if [[ "$CUDA" == true ]]; then
    PACKAGE_NAME="${PACKAGE_NAME}-cuda"
fi
PACKAGE_NAME="${PACKAGE_NAME}.deb"

echo "Building package: $PACKAGE_NAME"
mkdir -pv $SCRIPT_DIR/build/usr/lib/memgraph/query_modules

if [[ "$CUDA" == true ]]; then
    cp -v ../../python/requirements-gpu.txt $SCRIPT_DIR/build/usr/lib/memgraph/mage-requirements.txt
else
    cp -v ../../python/requirements.txt $SCRIPT_DIR/build/usr/lib/memgraph/mage-requirements.txt
fi
cp ../install_python_requirements.sh $SCRIPT_DIR/build/usr/lib/memgraph/install_python_requirements.sh

tar -xvzf $PACKAGE_DIR -C $SCRIPT_DIR/build/usr/lib/memgraph/

# Replace template variables in Debian control files
sed -i "s/@ARCH@/$ARCH/g" $SCRIPT_DIR/debian/control
sed -i "s/@VERSION@/$CLEAN_VERSION/g" $SCRIPT_DIR/debian/changelog

# set cuda in postinst
sed -i "s/@CUDA@/$CUDA/g" $SCRIPT_DIR/debian/postinst

dpkg-buildpackage -us -uc -b

old_name="$(ls ../memgraph-mage*.deb)"
mv $old_name $PACKAGE_NAME
