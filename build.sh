#!/bin/bash
set -euo pipefail

# Help function
show_help() {
    cat << EOF
Usage: ./build.sh [OPTIONS] [CMAKE_ARGS...]

Build script for Memgraph using Conan 2 and CMake.

OPTIONS:
    --build-type TYPE       Build type: Release, RelWithDebInfo, or Debug (default: Release)
    --toolchain NAME        Bundled toolchain to build against, named by its conan
                            profile suffix (e.g. v7, v8). Defaults to the value in
                            environment/toolchain/default_toolchain.sh. Run without a
                            valid NAME to list what is available.
    --target TARGET...      CMake target(s) to build (default: all targets). Accepts
                            multiple targets in one sequence, e.g.
                            --target memgraph memgraph__unit
    --reserve-cores N       Leave N cores free for other work (default: 0, uses all cores)
    --compile-jobs N        Pin concurrent compile steps (default: derived from memory)
    --link-jobs N           Pin concurrent link steps (default: derived from memory)
    --no-job-memory-cap     Do not cap concurrency by memory; -j alone decides
    --skip-os-deps          Skip OS dependency checks
    --keep-build            Keep existing build directory for incremental builds
    --config-only           Only configure CMake, don't build
    --dev                   Developer mode: enables --skip-os-deps --keep-build
    --update-lockfile       Update conan.lock before installing dependencies
    --graph-info            Generate dependency graph as graph.html and exit
    --split-debug           Extract debug info into sidecar .debug files (requires RelWithDebInfo/Debug)
    --lto                   Optimize across translation units; for packaged builds, and
                            several times slower to build
    --mage MODE             MAGE query modules (C++, Python, Rust). MODE is one of:
                              off  = no MAGE (default)
                              on   = build MAGE together with Memgraph
                              only = build just MAGE, not Memgraph itself
                                     (trims the conan dependency graph)
    --cugraph               Also build MAGE cuGraph GPU modules (implies --mage on)
    --profiling MODES       Comma-separated profiling build modes (e.g. --profiling fp,mem):
                              fp  = retain frame pointers for low-overhead 'perf' (MG_PROFILE)
                              mem = memory-profiling build, disables jemalloc (MG_MEMORY_PROFILE)
    --help                  Show this help message

ENVIRONMENT VARIABLES:
    MG_TOOLCHAIN            Same as --toolchain.
    MG_TOOLCHAIN_ROOT       Where the selected toolchain is installed. Only needed
                            when it is not at the location its profile expects.
    VENV_DIR                Path to Python virtual environment (default: env)
    MG_PYTHON               Python interpreter to use (must be >= 3.10). By
                            default the newest suitable python3/python3.X on
                            PATH is picked automatically.

CMAKE_ARGS:
    Any additional arguments are passed directly to CMake configuration.
    Common examples:
        -DASAN=ON               Enable Address Sanitizer
        -DUBSAN=ON              Enable Undefined Behavior Sanitizer
        -DCMAKE_CXX_FLAGS=...   Additional compiler flags

    Compile and link steps are capped so their peak memory fits the machine (or
    the container's cgroup limit), whatever -j is used. --compile-jobs,
    --link-jobs and --no-job-memory-cap override the derived caps; the memory
    budgeted per step is retunable too:
        -DMG_MEMORY_PER_COMPILE_JOB_MB=N
        -DMG_MEMORY_PER_LINK_JOB_MB=N

EXAMPLES:
    # Standard release build
    ./build.sh

    # Fast developer rebuild (incremental)
    ./build.sh --dev

    # Debug build with sanitizers
    ./build.sh --build-type Debug -DASAN=ON -DUBSAN=ON

    # Build specific target
    ./build.sh --target memgraph

    # Build multiple targets at once
    ./build.sh --target memgraph memgraph__unit

    # Build Memgraph together with the MAGE query modules
    ./build.sh --mage on

    # Build just the MAGE query modules (no Memgraph)
    ./build.sh --mage only

    # MAGE-only with GPU modules against a prebuilt cuGraph
    ./build.sh --mage only --cugraph -DMG_CUGRAPH_ROOT=/opt/conda

    # Build against a different bundled toolchain
    ./build.sh --toolchain v7

    # Configure only, don't build
    ./build.sh --config-only

    # Keep build directory for faster rebuilds
    ./build.sh --keep-build --skip-os-deps

EOF
    exit 0
}

# Which bundled toolchain to build against. The default lives in one place;
# MG_TOOLCHAIN and --toolchain override it, and MG_TOOLCHAIN_ROOT overrides
# the install path the selected profile would otherwise pick.
source "$(dirname "${BASH_SOURCE[0]}")/environment/toolchain/default_toolchain.sh"
MG_TOOLCHAIN="${MG_TOOLCHAIN:-$MG_TOOLCHAIN_DEFAULT}"

# Default values
BUILD_TYPE="Release"
TARGETS=()
CMAKE_ARGS=""
config_only=false
keep_build=false
skip_os_deps=false
VENV_DIR="${VENV_DIR:-env}"
offline=false
update_lockfile=false
graph_info=false
RESERVE_CORES=0
COMPILE_JOBS=""
LINK_JOBS=""
JOB_MEMORY_CAP=on
SPLIT_DEBUG=off
LTO=off
PROFILING=""
MAGE=off
CUGRAPH=off
# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --build-type)
            BUILD_TYPE="$2"
            shift 2
            ;;
        --toolchain)
            MG_TOOLCHAIN="$2"
            shift 2
            ;;
        --target)
            shift
            # Consume all following args until the next flag as target names.
            while [[ $# -gt 0 && "$1" != -* ]]; do
                TARGETS+=("$1")
                shift
            done
            if [[ ${#TARGETS[@]} -eq 0 ]]; then
                echo "Error: --target requires at least one target name" >&2
                exit 1
            fi
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
        --compile-jobs)
            COMPILE_JOBS="$2"
            shift 2
            ;;
        --link-jobs)
            LINK_JOBS="$2"
            shift 2
            ;;
        --no-job-memory-cap)
            JOB_MEMORY_CAP=off
            shift
            ;;
        --split-debug)
            SPLIT_DEBUG=on
            shift
            ;;
        --lto)
            LTO=on
            shift
            ;;
        --mage)
            MAGE="$2"
            shift 2
            ;;
        --cugraph)
            CUGRAPH=on
            shift
            ;;
        --profiling)
            PROFILING="$2"
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

# Job pools are cache variables, so an omitted flag keeps whatever the previous
# configure left behind; pass 0 to go back to the memory-derived default.
for jobs in "$COMPILE_JOBS" "$LINK_JOBS"; do
    if [[ -n "$jobs" && ! "$jobs" =~ ^[0-9]+$ ]]; then
        echo "Error: --compile-jobs and --link-jobs take a non-negative integer (got '$jobs')" >&2
        exit 1
    fi
done
if [[ -n "$COMPILE_JOBS" ]]; then
    CMAKE_ARGS="$CMAKE_ARGS -DMG_COMPILE_JOBS=$COMPILE_JOBS"
fi
if [[ -n "$LINK_JOBS" ]]; then
    CMAKE_ARGS="$CMAKE_ARGS -DMG_LINK_JOBS=$LINK_JOBS"
fi
if [[ ! "$RESERVE_CORES" =~ ^[0-9]+$ ]]; then
    echo "Error: --reserve-cores takes a non-negative integer (got '$RESERVE_CORES')" >&2
    exit 1
fi
if [[ ! "$CMAKE_ARGS" =~ MG_RESERVE_CORES ]]; then
    CMAKE_ARGS="$CMAKE_ARGS -DMG_RESERVE_CORES=$RESERVE_CORES"
fi

# Stated explicitly either way, so dropping --no-job-memory-cap restores the
# cap instead of inheriting the previous configure's OFF. A raw -D for the same
# variable stays authoritative.
if [[ ! "$CMAKE_ARGS" =~ MG_LIMIT_PARALLELISM_BY_MEMORY ]]; then
    if [[ "$JOB_MEMORY_CAP" == "off" ]]; then
        CMAKE_ARGS="$CMAKE_ARGS -DMG_LIMIT_PARALLELISM_BY_MEMORY=OFF"
    else
        CMAKE_ARGS="$CMAKE_ARGS -DMG_LIMIT_PARALLELISM_BY_MEMORY=ON"
    fi
fi

if [[ "$SPLIT_DEBUG" == "on" ]]; then
    CMAKE_ARGS="$CMAKE_ARGS -DMG_SPLIT_DEBUG=ON"
fi

if [[ "$LTO" == "on" ]]; then
    CMAKE_ARGS="$CMAKE_ARGS -DMG_ENABLE_LTO=ON"
fi

if [[ "$MAGE" != "off" && "$MAGE" != "on" && "$MAGE" != "only" ]]; then
    echo "Error: --mage must be 'off', 'on', or 'only' (got '$MAGE')" >&2
    exit 1
fi

# conan.lock must always be generated from the FULL dependency graph; a
# lockfile created from the trimmed MAGE-only graph would be missing the
# memgraph dependencies and break every full build. (The reverse is fine:
# MAGE-only builds consume the full lockfile as a superset.)
if [[ "$update_lockfile" = true && "$MAGE" == "only" ]]; then
    echo "Error: --update-lockfile requires the full dependency graph; drop '--mage only'" >&2
    exit 1
fi

# cuGraph modules are part of MAGE, so --cugraph implies at least --mage on
# (an explicit --mage only is respected).
if [[ "$CUGRAPH" == "on" ]]; then
    CMAKE_ARGS="$CMAKE_ARGS -DMG_ENABLE_CUGRAPH=ON"
    if [[ "$MAGE" == "off" ]]; then
        MAGE=on
    fi
fi

if [[ "$MAGE" == "on" ]]; then
    CMAKE_ARGS="$CMAKE_ARGS -DMG_BUILD_MEMGRAPH=ON -DMG_BUILD_MAGE=ON"
elif [[ "$MAGE" == "off" ]]; then
    CMAKE_ARGS="$CMAKE_ARGS -DMG_BUILD_MEMGRAPH=ON -DMG_BUILD_MAGE=OFF"
fi

# Map comma-separated --profiling tokens to CMake options.
if [[ -n "$PROFILING" ]]; then
    IFS=',' read -ra _profiling_modes <<< "$PROFILING"
    for mode in "${_profiling_modes[@]}"; do
        case "$mode" in
            fp)  CMAKE_ARGS="$CMAKE_ARGS -DMG_PROFILE=ON" ;;
            mem) CMAKE_ARGS="$CMAKE_ARGS -DMG_MEMORY_PROFILE=ON" ;;
            "")  ;;
            *)   echo "Error: unknown --profiling mode '$mode' (valid: fp, mem)"; exit 1 ;;
        esac
    done
fi

# Detect distro
source environment/util.sh
DISTRO="$(operating_system)"
echo "Distro: $DISTRO"

# Resolve a Python >= 3.10 (the floor for every python invocation here and in
# init-dev). Handles distros whose default python3 is older but ship a newer
# versioned binary (e.g. centos-9: python3 = 3.9, python3.12 installed).
PYTHON="$(resolve_python)" || exit 1
export MG_PYTHON="$PYTHON"
echo "Python: $PYTHON ($("$PYTHON" --version 2>&1))"

# Rust (mgcxx) is installed via rustup into ~/.cargo. Login shells get it on
# PATH from ~/.cargo/env via the shell profile, but CI / non-login shells
# don't — source it here so `cargo` resolves in both.
if ! command -v cargo >/dev/null 2>&1 && [[ -f "$HOME/.cargo/env" ]]; then
    source "$HOME/.cargo/env"
fi

# Validate build type
if [[ "$BUILD_TYPE" != "Release" && "$BUILD_TYPE" != "RelWithDebInfo" && "$BUILD_TYPE" != "Debug" ]]; then
    echo "Error: --build-type must be either 'Release', 'RelWithDebInfo', or 'Debug'"
    exit 1
fi

# Resolve the toolchain profile, failing here rather than inside conan where a
# missing profile reports only the generated name.
TOOLCHAIN_PROFILE="memgraph_toolchain_$MG_TOOLCHAIN"
if [[ ! -f "conan_config/profiles/$TOOLCHAIN_PROFILE" ]]; then
    echo "Error: unknown toolchain '$MG_TOOLCHAIN' (no conan_config/profiles/$TOOLCHAIN_PROFILE)" >&2
    echo "Available:" >&2
    for p in conan_config/profiles/memgraph_toolchain_*; do
        echo "    ${p##*/memgraph_toolchain_}" >&2
    done
    exit 1
fi

# Each toolchain profile knows where its own toolchain is installed, so the
# root is only exported when the caller is overriding that.
if [[ -n "${MG_TOOLCHAIN_ROOT:-}" ]]; then
    export MG_TOOLCHAIN_ROOT
    echo "Toolchain: $MG_TOOLCHAIN (root $MG_TOOLCHAIN_ROOT)"
else
    echo "Toolchain: $MG_TOOLCHAIN"
fi

# The profile states the floors the toolchain targets, and they are part of
# every dependency's package id. A toolchain built for different floors than
# its profile claims would produce binaries filed under the wrong id and reused
# for builds they do not suit, so check the two agree where the toolchain says.
# Toolchains predating floors.env do not report, and are not checked.
_tc_root="${MG_TOOLCHAIN_ROOT:-/opt/toolchain-$MG_TOOLCHAIN}"
if [[ -f "$_tc_root/floors.env" ]]; then
    # shellcheck disable=SC1091
    source "$_tc_root/floors.env"
    for _f in glibc kernel; do
        _declared=$(sed -n "s/^os\.$_f=//p" "conan_config/profiles/$TOOLCHAIN_PROFILE")
        _var="MG_TOOLCHAIN_$(echo "$_f" | tr '[:lower:]' '[:upper:]')_FLOOR"
        _actual="${!_var}"
        if [[ -n "$_declared" && "$_declared" != "$_actual" ]]; then
            echo "Error: $TOOLCHAIN_PROFILE says os.$_f=$_declared, but the toolchain at" >&2
            echo "       $_tc_root was built for $_actual." >&2
            echo "       Dependencies would be cached under a floor they were not built for." >&2
            exit 1
        fi
    done
fi

# Initialize arrays for arguments
HOST_PROFILES=("-pr:h" "$TOOLCHAIN_PROFILE")
CONAN_COMMON_ARGS=(
  -pr:b memgraph_build_profile
  -s build_type="$BUILD_TYPE"
  -s os.distro="$DISTRO"
)

if [[ "$offline" = true ]]; then
    CONAN_COMMON_ARGS+=("--no-remote")
fi

# MAGE-only mode: trim the conan dependency graph to what MAGE needs. The
# conanfile also flips MG_BUILD_MEMGRAPH=OFF / MG_BUILD_MAGE=ON via the
# generated CMake toolchain (see conanfile.py generate()).
if [[ "$MAGE" == "only" ]]; then
    CONAN_COMMON_ARGS+=("-o" "&:mage_only=True")
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
    # Hard requirements: without these the build itself fails.
    for deps_group in TOOLCHAIN_RUN_DEPS MEMGRAPH_BUILD_DEPS; do
        if ! ./environment/os/install_deps.sh check "$deps_group"; then
            echo "Error: Dependency check failed for $deps_group"
            exit 1
        fi
    done
    # Not needed to compile — only to run the test suites / the built
    # memgraph. Warn so the gap is visible, but don't block the build.
    for deps_group in MEMGRAPH_TEST_DEPS MEMGRAPH_RUN_DEPS; do
        if ! ./environment/os/install_deps.sh check "$deps_group"; then
            echo "Warning: missing $deps_group packages (needed to run tests / memgraph itself);"
            echo "         install with: sudo ./environment/os/install_deps.sh install $deps_group"
        fi
    done
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
    # A venv created by an older interpreter keeps that version forever —
    # reject it rather than fail later in subtler ways.
    if ! "$VENV_DIR/bin/python" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)' 2>/dev/null; then
        echo "Error: $VENV_DIR uses $("$VENV_DIR/bin/python" --version 2>&1), but >= 3.10 is required." >&2
        echo "Delete it (rm -rf $VENV_DIR) and re-run to recreate it with $PYTHON." >&2
        exit 1
    fi
    source "$VENV_DIR/bin/activate"
    trap 'deactivate 2>/dev/null' EXIT ERR
else
    echo "Creating virtual environment and installing conan"
    "$PYTHON" -m venv "$VENV_DIR"
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
    conan graph info . \
      "${HOST_PROFILES[@]}" "${CONAN_COMMON_ARGS[@]}" \
      --format=html > graph.html
    echo "Open graph.html in a browser to view the dependency graph"
    exit 0
fi

# update lockfile if requested
if [[ "$update_lockfile" = true ]]; then
    echo "Updating conan.lock"
    # Resolve recipe revisions from remotes (including local-recipes-index),
    # not from any stale local cache export, so lockfiles stay portable.
    conan lock create . \
      "${HOST_PROFILES[@]}" "${CONAN_COMMON_ARGS[@]}" \
      --update \
      --lockfile="" \
      --lockfile-out=conan.lock
fi

# install conan dependencies
conan install . --build=missing \
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
# Ninja's ceiling for steps no job pool covers, such as code generation.
BUILD_JOBS=$(( $(nproc) - RESERVE_CORES ))
if [[ $BUILD_JOBS -lt 1 ]]; then
    BUILD_JOBS=1
fi

TARGET_ARGS=()
if [[ ${#TARGETS[@]} -gt 0 ]]; then
    TARGET_ARGS=(--target "${TARGETS[@]}")
fi

cmake \
  --build build \
  --preset $PRESET \
  "${TARGET_ARGS[@]}" \
  -j "$BUILD_JOBS"
