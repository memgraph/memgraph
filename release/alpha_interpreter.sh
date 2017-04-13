#!/bin/bash

# Initial version of script that is going to be used for release builds.

# NOTE: do not run this script as a super user

echo "Memgraph Release Building..."

# compile memgraph
cd ../build
rm -rf ./*
cmake -DCMAKE_BUILD_TYPE:String=debug ..
make -j8

# get the most recent version of memgraph exe
exe_name=`ls -t memgraph_* | head -1`

# create dst directory
mkdir -p ../release/${exe_name}

# copy all relevant files
cp ${exe_name} ../release/${exe_name}/memgraph
cp -r ../config ../release/${exe_name}/config

echo "Memgraph Release Building DONE"
