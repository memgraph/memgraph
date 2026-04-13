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
    --reserve-cores N       Reserve N cores for other tasks (default: 0, uses all cores)
    --skip-os-deps          Skip OS dependency checks
    --keep-build            Keep existing build directory for incremental builds
    --config-only           Only configure CMake, don't build
    --dev                   Developer mode: enables --skip-os-deps --keep-build
    --update-lockfile       Update conan.lock before installing dependencies
    --graph-info            Generate dependency graph as graph.html and exit
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
config_only=false
keep_build=false
skip_os_deps=false
VENV_DIR="${VENV_DIR:-env}"
offline=false
update_lockfile=false
graph_info=false
RESERVE_CORES=0
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
            # Developer mode: skip os deps checks and keep build directory
            skip_os_deps=true
            keep_build=true
            shift
            ;;
        --offline)
            skip_os_deps=true
            offline=true
            shift
            ;;
        --update-lockfile)
            update_lockfile=true
            shift
            ;;
        --graph-info)
            graph_info=true
            shift
            ;;
        --reserve-cores)
            RESERVE_CORES="$2"
            shift 2
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
HOST_PROFILES=("-pr:h" "memgraph_toolchain_v7")
CONAN_COMMON_ARGS=(
  -pr:b memgraph_build_profile
  -s build_type="$BUILD_TYPE"
  -s os.distro="$DISTRO"
)

if [[ "$offline" = true ]]; then
    CONAN_COMMON_ARGS+=("--no-remote")
fi

# delete existing build directory
if [[ "$keep_build" = false ]]; then
    if [ -d "build" ]; then
        echo "Deleting existing build directory"
        rm -rf build
    fi
else
    echo "Keeping existing build directory"
fi

# run check for operating system dependencies
if [[ "$skip_os_deps" = false ]]; then
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

DEV_SETUP_ARGS=()
if [[ -n "${CI:-}" ]]; then
    DEV_SETUP_ARGS+=("--ci")
fi
bash ./init-dev "${DEV_SETUP_ARGS[@]}"

if [[ -f "$VENV_DIR/bin/activate" ]]; then
    echo "Using existing virtual environment at $VENV_DIR"
    source "$VENV_DIR/bin/activate"
    trap 'deactivate 2>/dev/null' EXIT ERR
else
    echo "Creating virtual environment and installing conan"
    python3 -m venv "$VENV_DIR"
    source "$VENV_DIR/bin/activate"
    trap 'deactivate 2>/dev/null' EXIT ERR
    pip install "conan>=2.26.0"
fi

# check if a conan profile exists
if [[ ! -f "$HOME/.conan2/profiles/default" ]]; then
    echo "Creating conan profile"
    conan profile detect
fi

# Install custom conan settings
conan config install conan_config

# Register vendored recipes as a local-recipes-index remote
# NOTE: also registered in release/package/mgbuild.sh — keep in sync
conan remote add memgraph-recipes "$(pwd)/conan_recipes" -t local-recipes-index --force

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

# Add sanitizer profiles based on CMAKE_ARGS
if cmake_var_enabled "ASAN" "$CMAKE_ARGS"; then
    HOST_PROFILES+=("-pr:h" "add_asan")
    echo "ASAN enabled"
fi
if cmake_var_enabled "UBSAN" "$CMAKE_ARGS"; then
    HOST_PROFILES+=("-pr:h" "add_ubsan")
    echo "UBSAN enabled"
fi
if cmake_var_enabled "TSAN" "$CMAKE_ARGS"; then
    HOST_PROFILES+=("-pr:h" "add_tsan")
    echo "TSAN enabled"
fi

# generate dependency graph and exit early
if [[ "$graph_info" = true ]]; then
    echo "Generating dependency graph -> graph.html"
    MG_TOOLCHAIN_ROOT="/opt/toolchain-v7" conan graph info . \
      "${HOST_PROFILES[@]}" "${CONAN_COMMON_ARGS[@]}" \
      --format=html > graph.html
    echo "Open graph.html in a browser to view the dependency graph"
    exit 0
fi

# update lockfile if requested
if [[ "$update_lockfile" = true ]]; then
    echo "Updating conan.lock"
    rm -f conan.lock
    MG_TOOLCHAIN_ROOT="/opt/toolchain-v7" conan lock create . \
      "${HOST_PROFILES[@]}" "${CONAN_COMMON_ARGS[@]}" \
      --lockfile="" \
      --lockfile-out=conan.lock
fi

# install conan dependencies
MG_TOOLCHAIN_ROOT="/opt/toolchain-v7" conan install . --build=missing \
  "${HOST_PROFILES[@]}" "${CONAN_COMMON_ARGS[@]}"

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
# Determine number of parallel jobs (reserve cores for system responsiveness)
BUILD_JOBS=$(( $(nproc) - RESERVE_CORES ))
if [[ $BUILD_JOBS -lt 1 ]]; then
    BUILD_JOBS=1
fi

cmake \
  --build build \
  --preset $PRESET \
  ${TARGET:+--target $TARGET} \
  -j "$BUILD_JOBS"
