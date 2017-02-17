#!/bin/bash

if ! type "wget" > /dev/null; then
    echo >&2 "Please install wget or set it in your path. Aborting."; exit 1;
fi

if ! type "git" > /dev/null; then
    echo >&2 "Please install git or set it in your path. Aborting."; exit 1;
fi

if ! type "cmake" > /dev/null; then
    echo >&2 "Please install cmake or set it in your path. Aborting."; exit 1;
fi

cd cmake
./setup.sh
cd ..

cd libs
./setup.sh
cd ..

cd build
wget http://www.antlr.org/download/antlr-4.6-complete.jar
cd ..

echo "DONE"
