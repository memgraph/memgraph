#!/usr/bin/env bash

set -eo

MG_BUILD_TYPE=${MG_BUILD_TYPE:-RelWithDebInfo}
MG_THREADS=${MG_THREADS:-24}

echo -e "\n-----BUILD_TYPE=$MG_BUILD_TYPE-----\n"
echo -e "\n-----THREADS=$MG_THREADS-----\n"

cd memgraph
source /opt/toolchain-v4/activate

echo -e "\n---INIT---\n"
./init

echo -e "\n---CMAKE---\n"
cd build
cmake -DCMAKE_BUILD_TYPE=$MG_BUILD_TYPE -DMG_ENTERPRISE=OFF ..

echo -e "\n---MAKE---\n"
make -j$MG_THREADS
