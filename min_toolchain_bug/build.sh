#!/bin/bash

rm -rf build
conan install . --build=missing
source ./build/Release/generators/conanbuild.sh
cmake --preset conan-release
cmake --build --preset conan-release
