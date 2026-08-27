#!/bin/bash
set -euo pipefail
source /tc/lib/common.sh
source "$TC_VERSIONS/pahole.env"

fetch https://github.com/PhilArmstrong/pahole-gdb/archive/master.zip "$PAHOLE_SHA256" pahole-gdb-master.zip

pushd "$TC_BUILD"
# Host deps (apt): unzip.
log_tool_name "install pahole"
unzip ../archives/pahole-gdb-master.zip
mv pahole-gdb-master $PREFIX/share/pahole-gdb

popd
