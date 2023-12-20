#!/usr/bin/env bash

set -eo pipefail

DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

MG_BUILD_TYPE=${MG_BUILD_TYPE:-RelWithDebInfo}
MG_THREADS=${MG_THREADS:-24}

source /opt/toolchain-v4/activate
cd ../../../build
cmake -DCMAKE_BUILD_TYPE=$MG_BUILD_TYPE -DMG_ENTERPRISE=OFF ..
make -j$MG_THREADS
