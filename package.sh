#!/bin/bash
set -euo pipefail

show_help() {
    cat << EOF
Usage: ./package.sh [OPTIONS] TARGET FORMAT

Package Memgraph and/or MAGE from an existing build directory using CPack.

TARGET:
    memgraph                Package memgraph + memgraph-debuginfo
    mage                    Package memgraph-mage (+ memgraph-mage-debuginfo
                            when the build used --split-debug)
    all                     Package everything the build was configured for

FORMAT:
    deb                     Debian package(s)
    rpm                     RPM package(s)

OPTIONS:
    --build-dir DIR         Build directory to package from (default: build)
    --help                  Show this help message

The build directory must already be configured and built, e.g.:
    ./build.sh --split-debug --build-type RelWithDebInfo --mage on   # everything
    ./build.sh --split-debug --build-type RelWithDebInfo --mage only # just MAGE

Packages are written to <build-dir>/output/.

EXAMPLES:
    ./package.sh memgraph deb
    ./package.sh mage rpm
    ./package.sh all deb
    ./package.sh --build-dir mybuild mage deb
EOF
    exit 0
}

BUILD_DIR="build"
POSITIONAL=()
while [[ $# -gt 0 ]]; do
    case $1 in
        --build-dir)
            BUILD_DIR="$2"
            shift 2
            ;;
        --help|-h)
            show_help
            ;;
        -*)
            echo "Error: unknown option '$1' (see --help)"
            exit 1
            ;;
        *)
            POSITIONAL+=("$1")
            shift
            ;;
    esac
done

if [[ ${#POSITIONAL[@]} -ne 2 ]]; then
    echo "Error: expected exactly TARGET and FORMAT arguments (see --help)"
    exit 1
fi
TARGET="${POSITIONAL[0]}"
FORMAT="${POSITIONAL[1]}"

case "$FORMAT" in
    deb) GENERATOR="DEB" ;;
    rpm) GENERATOR="RPM" ;;
    *)
        echo "Error: FORMAT must be 'deb' or 'rpm' (got '$FORMAT')"
        exit 1
        ;;
esac

CACHE="$BUILD_DIR/CMakeCache.txt"
CPACK_CONFIG="$BUILD_DIR/CPackConfig.cmake"
if [[ ! -f "$CACHE" || ! -f "$CPACK_CONFIG" ]]; then
    echo "Error: '$BUILD_DIR' is not a configured build directory (missing"
    echo "       CMakeCache.txt / CPackConfig.cmake). Run ./build.sh first."
    exit 1
fi

# Read an ON/OFF-ish cache entry (option() or cache var; any type tag).
cache_enabled() {
    grep -Eiq "^$1:[A-Za-z]*=(ON|TRUE|YES|1)\$" "$CACHE"
}

require_configured() {
    if ! cache_enabled "$1"; then
        echo "Error: this build was configured with $1=OFF; rebuild with:"
        echo "       $2"
        exit 1
    fi
}

CPACK_EXTRA_ARGS=()
COMPONENTS=""
case "$TARGET" in
    memgraph)
        require_configured MG_BUILD_MEMGRAPH "./build.sh (without --mage only)"
        COMPONENTS="memgraph;debuginfo"
        ;;
    mage)
        require_configured MG_BUILD_MAGE "./build.sh --mage on|only"
        COMPONENTS="mage"
        if cache_enabled MG_SPLIT_DEBUG; then
            COMPONENTS="$COMPONENTS;mage_debuginfo"
        fi
        # Don't build mgconsole (CPACK_INSTALL_SCRIPT) for a MAGE-only run.
        CPACK_EXTRA_ARGS+=("-D" "CPACK_INSTALL_SCRIPT=")
        ;;
    all)
        COMPONENTS=""
        if cache_enabled MG_BUILD_MEMGRAPH; then
            COMPONENTS="memgraph;debuginfo"
        fi
        if cache_enabled MG_BUILD_MAGE; then
            COMPONENTS="${COMPONENTS:+$COMPONENTS;}mage"
            if cache_enabled MG_SPLIT_DEBUG; then
                COMPONENTS="$COMPONENTS;mage_debuginfo"
            fi
        fi
        if [[ -z "$COMPONENTS" ]]; then
            echo "Error: nothing to package in '$BUILD_DIR'"
            exit 1
        fi
        if ! cache_enabled MG_BUILD_MEMGRAPH; then
            CPACK_EXTRA_ARGS+=("-D" "CPACK_INSTALL_SCRIPT=")
        fi
        ;;
    *)
        echo "Error: TARGET must be 'memgraph', 'mage', or 'all' (got '$TARGET')"
        exit 1
        ;;
esac

# cpack ships with the conan-provided cmake; pick up the build's tool env.
if [[ -f "$BUILD_DIR/generators/conanbuild.sh" ]]; then
    # shellcheck disable=SC1091
    source "$BUILD_DIR/generators/conanbuild.sh"
fi
if ! command -v cpack >/dev/null; then
    echo "Error: cpack not found on PATH (and no conan env in $BUILD_DIR/generators)"
    exit 1
fi

OUTPUT_DIR="$BUILD_DIR/output"
mkdir -p "$OUTPUT_DIR"

echo "Packaging components [$COMPONENTS] as $GENERATOR into $OUTPUT_DIR"
CPACK_LOG="$OUTPUT_DIR/cpack.log"
(
    cd "$OUTPUT_DIR"
    cpack -G "$GENERATOR" \
        --config ../CPackConfig.cmake \
        -D "CPACK_COMPONENTS_ALL=$COMPONENTS" \
        "${CPACK_EXTRA_ARGS[@]}"
) 2>&1 | tee "$CPACK_LOG"

# CPack can drop a component whose rpmbuild/dpkg-deb run fails (logging the
# error only at debug verbosity) and still exit 0 — e.g. the memgraph rpm's
# `BuildRequires: systemd` is unsatisfiable on a Debian-family host, which
# silently yields only the debuginfo rpm. One component maps to one package
# here, so require exactly that many.
expected=$(awk -F';' '{print NF}' <<< "$COMPONENTS")
generated=$(grep -c "CPack: - package: .* generated\." "$CPACK_LOG" || true)
if [[ "$generated" -ne "$expected" ]]; then
    echo "Error: expected $expected package(s) for components [$COMPONENTS] but cpack"
    echo "       generated $generated — check the rpmbuild/dpkg logs referenced in $CPACK_LOG"
    exit 1
fi
