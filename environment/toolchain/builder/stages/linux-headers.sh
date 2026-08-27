#!/bin/bash
set -euo pipefail
source /tc/lib/common.sh
source "$TC_VERSIONS/linux-headers.env"

fetch https://cdn.kernel.org/pub/linux/kernel/v5.x/linux-$LINUX_HEADERS_VERSION.tar.xz "$LINUX_HEADERS_SHA256"

# Host deps (apt): gcc, make (headers_install compiles scripts/unifdef), rsync.
log_tool_name "Linux kernel headers $LINUX_HEADERS_VERSION"
enter_source linux-$LINUX_HEADERS_VERSION.tar.xz linux-$LINUX_HEADERS_VERSION
if [[ "$for_arm" = true ]]; then
    kernel_arch=arm64
else
    kernel_arch=x86_64
fi
make ARCH=$kernel_arch INSTALL_HDR_PATH=$SYSROOT/usr headers_install
leave_source
