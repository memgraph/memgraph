#!/bin/bash

# Default values
BUILD_TYPE="Release"
TARGET=""
CMAKE_ARGS=""
skip_init=false

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

# Determine profile template based on sanitizer flags
PROFILE_TEMPLATE="./memgraph_template_profile"
DASAN_ENABLED=false
DUBSAN_ENABLED=false

# Check if DASAN=ON is in CMAKE_ARGS
if [[ "$CMAKE_ARGS" == *"DASAN=ON"* ]]; then
    DASAN_ENABLED=true
fi

# Check if DUBSAN=ON is in CMAKE_ARGS
if [[ "$CMAKE_ARGS" == *"DUBSAN=ON"* ]]; then
    DUBSAN_ENABLED=true
fi

# Select appropriate profile template
if [[ "$DASAN_ENABLED" == true && "$DUBSAN_ENABLED" == true ]]; then
    PROFILE_TEMPLATE="./memgraph_template_profile_asan_ubsan"
elif [[ "$DASAN_ENABLED" == true ]]; then
    PROFILE_TEMPLATE="./memgraph_template_profile_asan"
elif [[ "$DUBSAN_ENABLED" == true ]]; then
    PROFILE_TEMPLATE="./memgraph_template_profile_ubsan"
fi

# install conan dependencies
export MG_TOOLCHAIN_ROOT=/opt/toolchain-v7/
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

# Build command with optional target
if [[ -n "$TARGET" ]]; then
    cmake --build build --target $TARGET -j $(nproc)
else
    cmake --build build -j $(nproc)
fi

deactivate
