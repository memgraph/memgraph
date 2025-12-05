#!/bin/bash
set -euo pipefail

# Help function
show_help() {
    cat << EOF
Usage: ./build.sh [OPTIONS] [CMAKE_ARGS...]

Build script for Memgraph using Conan 2 and CMake.

OPTIONS:
    --build-type TYPE       Build type: Release, RelWithDebInfo, or Debug (default: Release)
    --target TARGET         Specific CMake target to build (default: all targets)
    --skip-init             Skip running ./init to fetch non-Conan dependencies
    --skip-os-deps          Skip OS dependency checks
    --keep-build            Keep existing build directory for incremental builds
    --config-only           Only configure CMake, don't build
    --dev                   Developer mode: enables --skip-init --skip-os-deps --keep-build
    --help                  Show this help message

ENVIRONMENT VARIABLES:
    VENV_DIR                Path to Python virtual environment (default: env)

CMAKE_ARGS:
    Any additional arguments are passed directly to CMake configuration.
    Common examples:
        -DASAN=ON               Enable Address Sanitizer
        -DUBSAN=ON              Enable Undefined Behavior Sanitizer
        -DCMAKE_CXX_FLAGS=...   Additional compiler flags

EXAMPLES:
    # Standard release build
    ./build.sh

    # Fast developer rebuild (incremental)
    ./build.sh --dev

    # Debug build with sanitizers
    ./build.sh --build-type Debug -DASAN=ON -DUBSAN=ON

    # Build specific target
    ./build.sh --target memgraph

    # Configure only, don't build
    ./build.sh --config-only

    # Keep build directory for faster rebuilds
    ./build.sh --keep-build --skip-os-deps

EOF
    exit 0
}

# Default values
BUILD_TYPE="Release"
TARGET=""
CMAKE_ARGS=""
skip_init=false
config_only=false
keep_build=false
skip_os_deps=false
VENV_DIR="${VENV_DIR:-env}"
offline=false
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
        --keep-build)
            keep_build=true
            shift
            ;;
        --skip-os-deps)
            skip_os_deps=true
            shift
            ;;
        --dev)
            # Developer mode: skip init, os deps checks, and keep build directory
            skip_init=true
            skip_os_deps=true
            keep_build=true
            shift
            ;;
        --offline)
            skip_os_deps=true
            offline=true
            shift
            ;;
        --help|-h)
            show_help
            ;;
        *)
            # Capture any other arguments to pass to cmake
            CMAKE_ARGS="$CMAKE_ARGS $1"
            shift
            ;;
    esac
done

# Detect distro
source environment/util.sh
DISTRO="$(operating_system)"
echo "Distro: $DISTRO"

# Validate build type
if [[ "$BUILD_TYPE" != "Release" && "$BUILD_TYPE" != "RelWithDebInfo" && "$BUILD_TYPE" != "Debug" ]]; then
    echo "Error: --build-type must be either 'Release', 'RelWithDebInfo', or 'Debug'"
    exit 1
fi

# Initialize arrays for arguments
INIT_ARGS=()
CONAN_INSTALL_ARGS=(
  .
  --build=missing
  -pr:h memgraph_template_profile
  -pr:b memgraph_build_profile
  -s build_type="$BUILD_TYPE"
  -s os.distro="$DISTRO"
)

if [ "$offline" = true ]; then
    INIT_ARGS+=("--offline")
    CONAN_INSTALL_ARGS+=("--no-remote")
fi

# delete existing build directory
if [ "$keep_build" = false ]; then
    if [ -d "build" ]; then
        echo "Deleting existing build directory"
        rm -rf build
    fi
else
    echo "Keeping existing build directory"
fi

# run check for operating system dependencies
if [ "$skip_os_deps" = false ]; then
    if ! ./environment/os/install_deps.sh check TOOLCHAIN_RUN_DEPS; then
        echo "Error: Dependency check failed for TOOLCHAIN_RUN_DEPS"
        exit 1
    fi
    if ! ./environment/os/install_deps.sh check MEMGRAPH_BUILD_DEPS; then
        echo "Error: Dependency check failed for MEMGRAPH_BUILD_DEPS"
        exit 1
    fi
else
    echo "Skipping OS dependency checks"
fi

if [[ -f "$VENV_DIR/bin/activate" ]]; then
    echo "Using existing virtual environment at $VENV_DIR"
    source "$VENV_DIR/bin/activate"
    trap 'deactivate 2>/dev/null' EXIT ERR
else
    echo "Creating virtual environment and installing conan"
    python3 -m venv "$VENV_DIR"
    source "$VENV_DIR/bin/activate"
    trap 'deactivate 2>/dev/null' EXIT ERR
    pip install conan
fi

# check if a conan profile exists
if [ ! -f "$HOME/.conan2/profiles/default" ]; then
    echo "Creating conan profile"
    conan profile detect
fi

# Install custom conan settings
conan config install conan_config

# fetch libs that aren't provided by conan yet
if [ "$skip_init" = false ]; then
    ./init "${INIT_ARGS[@]}"
fi

# Function to check if a CMake boolean variable is enabled
# Handles various CMake boolean formats: ON, TRUE, YES, 1 (case insensitive)
# Supports both -DVAR=VALUE and -D VAR=VALUE formats
cmake_var_enabled() {
    local var_name="$1"
    local args="$2"
    # Match patterns like -DVAR=ON, -D VAR=ON, -DVAR:BOOL=TRUE, etc.
    # Accepts ON, TRUE, YES, 1 as true values (case insensitive)
    if [[ "$args" =~ -D[[:space:]]*${var_name}([[:space:]]*:[[:alnum:]]+)?[[:space:]]*=[[:space:]]*(ON|TRUE|YES|1) ]]; then
        return 0
    fi
    return 1
}

# Build sanitizer list from CMAKE_ARGS
MG_SANITIZERS=""
declare -A sanitizer_map=(
    ["ASAN"]="address"
    ["UBSAN"]="undefined"
    ["TSAN"]="thread"
)

for cmake_var in "${!sanitizer_map[@]}"; do
    if cmake_var_enabled "$cmake_var" "$CMAKE_ARGS"; then
        MG_SANITIZERS="${MG_SANITIZERS:+$MG_SANITIZERS,}${sanitizer_map[$cmake_var]}"
    fi
done

if [[ -n "$MG_SANITIZERS" ]]; then
    echo "Sanitizers enabled: $MG_SANITIZERS"
    export MG_SANITIZERS="$MG_SANITIZERS"
else
    echo "No sanitizers enabled"
fi

# install conan dependencies
MG_TOOLCHAIN_ROOT="/opt/toolchain-v7" conan install "${CONAN_INSTALL_ARGS[@]}"

export CLASSPATH=
export LD_LIBRARY_PATH=
export DYLD_LIBRARY_PATH=
source build/generators/conanbuild.sh

# Determine preset name based on build type (Conan generates this automatically)
# Convert to lowercase for preset name: Release -> conan-release
PRESET="conan-$(echo "$BUILD_TYPE" | tr '[:upper:]' '[:lower:]')"

# Filter out sanitizer flags from CMAKE_ARGS since conanfile.py handles them automatically
# via compiler settings (compiler.asan, compiler.ubsan, compiler.tsan)
FILTERED_CMAKE_ARGS=""
for arg in $CMAKE_ARGS; do
    if [[ ! "$arg" =~ ^-D(ASAN|UBSAN|TSAN)(=|:|$) ]]; then
        FILTERED_CMAKE_ARGS="$FILTERED_CMAKE_ARGS $arg"
    fi
done

# Configure cmake with additional arguments (sanitizer flags automatically set by Conan)
cmake --preset $PRESET $FILTERED_CMAKE_ARGS

if [[ "$config_only" = true ]]; then
    exit 0
fi

# Build command with optional target
cmake \
  --build build \
  --preset $PRESET \
  ${TARGET:+--target $TARGET} \
  -j $(nproc)
