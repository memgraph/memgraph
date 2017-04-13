#!/bin/bash

# Initial version of script that is going to be used for release builds.

# NOTE: do not run this script as a super user

echo "Memgraph Release Building..."

# compile memgraph
cd ../build
rm -rf ./*
cmake -DCMAKE_BUILD_TYPE:String=debug ..
make -j8
make copy_hardcoded_queries

# get the most recent version of memgraph exe
exe_name=`ls -t memgraph_* | head -1`

# create dst directory
mkdir -p ../release/${exe_name}

# copy all relevant files
cp ${exe_name} ../release/${exe_name}/memgraph
cp libmemgraph_pic.a ../release/${exe_name}/libmemgraph_pic.a
rm -rf ../release/${exe_name}/include
cp -r include ../release/${exe_name}/include
cp -r template ../release/${exe_name}/template
cp -r ../config ../release/${exe_name}/config
cp -r ../libs ../release/${exe_name}/libs

# copy the hardcoded query plan
# TODO: minimise the header files
cp -r compiled ../release/${exe_name}/

echo "Memgraph Release Building DONE"
