#!/bin/bash

# Default values
BUILD_TYPE="Release"
TARGET=""
CMAKE_ARGS=""
skip_init=false
config_only=false

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
        --skip-init)
            skip_init=true
            shift
            ;;
        --config-only)
            config_only=true
            shift
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

# delete existing build directory
if [ -d "build" ]; then
    echo "Deleting existing build directory"
    rm -rf build
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
if [ "$skip_init" = false ]; then
    ./init
fi

# Use the new combined profile template
PROFILE_TEMPLATE="./memgraph_template_profile"
MG_SANITIZERS=""

# Check if DASAN=ON is in CMAKE_ARGS
if [[ "$CMAKE_ARGS" == *"DASAN=ON"* ]]; then
    MG_SANITIZERS="address"
fi

# Check if DUBSAN=ON is in CMAKE_ARGS
if [[ "$CMAKE_ARGS" == *"DUBSAN=ON"* ]]; then
    if [[ -n "$MG_SANITIZERS" ]]; then
        # If we already have address sanitizer, add undefined to the list
        MG_SANITIZERS="address,undefined"
    else
        MG_SANITIZERS="undefined"
    fi
fi

echo "Using profile template: $PROFILE_TEMPLATE"
if [[ -n "$MG_SANITIZERS" ]]; then
    echo "Sanitizers enabled: $MG_SANITIZERS"
    export MG_SANITIZERS="$MG_SANITIZERS"
else
    echo "No sanitizers enabled"
fi

# install conan dependencies
export MG_TOOLCHAIN_ROOT=/opt/toolchain-v7
conan install . --build=missing -pr $PROFILE_TEMPLATE -s build_type=$BUILD_TYPE
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

# Configure cmake with additional arguments
if [[ -n "$CMAKE_ARGS" ]]; then
    # If we have additional CMake arguments, we need to configure manually with Conan toolchain
    cmake -S . -B build -G Ninja -DCMAKE_TOOLCHAIN_FILE=build/generators/conan_toolchain.cmake -DCMAKE_BUILD_TYPE=$BUILD_TYPE $CMAKE_ARGS
else
    # Use preset if no additional arguments
    cmake --preset $PRESET
fi

if [[ "$config_only" = true ]]; then
    exit 0
fi

# Build command with optional target
if [[ -n "$TARGET" ]]; then
    cmake --build build --target $TARGET -j $(nproc)
else
    cmake --build build -j $(nproc)
fi

deactivate
