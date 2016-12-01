#!/bin/bash

# Initial version of script that is going to be used for release build.

# TODO: enable options related to lib

echo "Memgraph Release Building..."

cd ../build
# get most recent version of memgraph exe
exe_name=`ls -t memgraph_* | head -1`

cd ../release
# create libs dir
mkdir -p libs

# initialize all libs
# cp ../libs/setup.sh libs/setup.sh
# ./libs/setup.sh

# just copy all libs
cp -r ../libs ./

# compile memgraph
cd ../build
# rm -rf ./*
# cmake -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE:String=debug ..
# make -j 4

# create dst directory
mkdir -p ../release/${exe_name}

# copy all relevant files
cp ${exe_name} ../release/${exe_name}/memgraph
cp libmemgraph_pic.a ../release/${exe_name}/libmemgraph_pic.a
cp -r include ../release/${exe_name}/include
cp -r template ../release/${exe_name}/template
cp -r ../config ../release/${exe_name}/config

# create compiled folder and copy hard coded queries
mkdir -p ../release/${exe_name}/compiled/cpu/hardcode
cp ../tests/integration/hardcoded_query/*.cpp ../release/${exe_name}/compiled/cpu/hardcode
cp ../tests/integration/hardcoded_query/*.hpp ../release/${exe_name}/compiled/cpu/hardcode

echo "Memgraph Release Building DONE"


