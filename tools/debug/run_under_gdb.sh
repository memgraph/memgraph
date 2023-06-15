#!/bin/bash -e
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

# Make sure Memgraph is compiled in RelWithDebInfo to get representative runs
# while also having debug symbols
# cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo ..

source /opt/toolchain-v4/activate

cd $DIR/../../build
gdb --args ./memgraph \
  --bolt-port 7687 \
  --storage-properties-on-edges=true \
  --storage-snapshot-interval-sec=60 \
  --storage-wal-enabled=true \
  --storage-recover-on-startup=true \
  --storage-snapshot-on-exit=true "$1" # additional flags like telemetry
