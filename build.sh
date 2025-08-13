#!/bin/bash

# Default values
BUILD_TYPE="Release"
TARGET=""
CMAKE_ARGS=""

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --build-type)
            BUILD_TYPE="$2"
            shift 2
            ;;
        --target)
            TARGET="$2"
            shift 2
            ;;
        *)
            # Capture any other arguments to pass to cmake
            CMAKE_ARGS="$CMAKE_ARGS $1"
            shift
            ;;
    esac
done

# Validate build type
if [[ "$BUILD_TYPE" != "Release" && "$BUILD_TYPE" != "RelWithDebInfo" && "$BUILD_TYPE" != "Debug" ]]; then
    echo "Error: --build-type must be either 'Release', 'RelWithDebInfo', or 'Debug'"
    exit 1
fi

# run check for operating system dependencies
./environment/os/install_deps.sh check TOOLCHAIN_RUN_DEPS
./environment/os/install_deps.sh check MEMGRAPH_BUILD_DEPS

echo "Creating virtual environment and installing conan"
python3 -m venv env
source ./env/bin/activate
pip install conan

# check if a conan profile exists
if [ ! -f "$HOME/.conan2/profiles/default" ]; then
    echo "Creating conan profile"
    conan profile detect
fi

# fetch libs that aren't provided by conan yet
./init

# install conan dependencies
export MG_TOOLCHAIN_ROOT=/opt/toolchain-v7/
conan install . --build=missing -pr ./memgraph_template_profile -s build_type=$BUILD_TYPE
source build/generators/conanbuild.sh

# Determine preset name based on build type (Conan generates this automatically)
if [[ "$BUILD_TYPE" == "Release" ]]; then
    PRESET="conan-release"
elif [[ "$BUILD_TYPE" == "RelWithDebInfo" ]]; then
    PRESET="conan-relwithdebinfo"
elif [[ "$BUILD_TYPE" == "Debug" ]]; then
    PRESET="conan-debug"
else
    echo "Error: Unsupported build type: $BUILD_TYPE"
    exit 1
fi

cmake --preset $PRESET

# Build command with optional target and additional cmake arguments
if [[ -n "$TARGET" ]]; then
    cmake --build --preset $PRESET --target $TARGET $CMAKE_ARGS
else
    cmake --build --preset $PRESET $CMAKE_ARGS
fi

deactivate
