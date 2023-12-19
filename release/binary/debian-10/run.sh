#!/bin/bash -eo

MG_BUILD_TYPE=${MG_BUILD_TYPE:-RelWithDebInfo}
MG_THREADS=${MG_THREADS:-24}

echo -e "\n-----BUILD_TYPE=$MG_BUILD_TYPE-----\n"
echo -e "\n-----THREADS=$MG_THREADS-----\n"

cd memgraph
source /opt/toolchain-v4/activate
./init
cd build
cmake -DCMAKE_BUILD_TYPE=$MG_BUILD_TYPE -DMG_ENTERPRISE=OFF ..
make -j$MG_THREADS

sleep eternity
