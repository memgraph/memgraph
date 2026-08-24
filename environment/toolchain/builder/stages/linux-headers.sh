#!/bin/bash
# linux-headers: build commands moved verbatim from the v8 build script. Only the
# orchestration around them is new -- edit the recipe here, not its shape.
set -euo pipefail
source /tc/lib/common.sh
source "$TC_VERSIONS/linux-headers.env"

pushd "$TC_ARCHIVES"
if [[ ! -f linux-$LINUX_HEADERS_VERSION.tar.xz ]]; then
    wget --https-only https://cdn.kernel.org/pub/linux/kernel/v5.x/linux-$LINUX_HEADERS_VERSION.tar.xz
    LINUX_HEADERS_SHA256="ae6a3207f12aa4d6cfb0fa793ec9da4a6fcdfdcb57d869d63d6b77e3a8c1423d"
    echo "$LINUX_HEADERS_SHA256  linux-$LINUX_HEADERS_VERSION.tar.xz" | sha256sum -c -
fi
popd

pushd "$TC_BUILD"
# Host deps (apt): gcc, make (headers_install compiles scripts/unifdef), rsync.
log_tool_name "Linux kernel headers $LINUX_HEADERS_VERSION"
if [[ ! -d "$SYSROOT/usr/include/linux" ]]; then
    if [[ -d "linux-$LINUX_HEADERS_VERSION" ]]; then
        rm -rf linux-$LINUX_HEADERS_VERSION
    fi
    tar -xf ../archives/linux-$LINUX_HEADERS_VERSION.tar.xz
    pushd "linux-$LINUX_HEADERS_VERSION"
    if [[ "$for_arm" = true ]]; then
        kernel_arch=arm64
    else
        kernel_arch=x86_64
    fi
    make ARCH=$kernel_arch INSTALL_HDR_PATH=$SYSROOT/usr headers_install
    popd
fi

popd
