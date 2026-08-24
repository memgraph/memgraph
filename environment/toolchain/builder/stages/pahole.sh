#!/bin/bash
# pahole: build commands moved verbatim from the v8 build script. Only the
# orchestration around them is new -- edit the recipe here, not its shape.
set -euo pipefail
source /tc/lib/common.sh
source "$TC_VERSIONS/pahole.env"

pushd "$TC_ARCHIVES"
if [[ ! -f pahole-gdb-master.zip ]]; then
    wget --https-only https://github.com/PhilArmstrong/pahole-gdb/archive/master.zip -O pahole-gdb-master.zip
    echo "$PAHOLE_SHA256 pahole-gdb-master.zip" | sha256sum -c -
fi
popd

pushd "$TC_BUILD"
# Host deps (apt): unzip.
log_tool_name "install pahole"
if [[ ! -d "$PREFIX/share/pahole-gdb" ]]; then
    unzip ../archives/pahole-gdb-master.zip
    mv pahole-gdb-master $PREFIX/share/pahole-gdb
fi

popd
