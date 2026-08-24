# Shared environment for every stage script. Sourced, never executed.
#
# The layout deliberately mirrors the original build script: archives live in
# a sibling directory of the build directory, so the recipes' relative
# "../archives/..." references work unchanged.

pushd () { command pushd "$@" > /dev/null; }
popd () { command popd "$@" > /dev/null; }

log_tool_name () {
    echo ""
    echo ""
    echo "#### $1 ####"
    echo ""
    echo ""
}

CPUS=$( grep -c processor < /proc/cpuinfo )

TC_ROOT=/tc
TC_VERSIONS="$TC_ROOT/versions"
TC_FILES="$TC_ROOT/files"
TC_WORK=/work
TC_ARCHIVES="$TC_WORK/archives"
TC_BUILD="$TC_WORK/build"
TC_OUTPUT="$TC_WORK/output"
mkdir -p "$TC_ARCHIVES" "$TC_BUILD"

# Fixed rather than "now", so the archive does not change on every build.
# Overridable for a release that wants to record its own date.
export SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH:-0}"
export TZ=UTC
export LC_ALL=C

# In the original script $DIR was the directory that held activate.in,
# toolchain.cmake, archives/ and build/ all at once, and the recipes reach
# across it: gmp and mpfr build out of gcc's unpacked source tree, and LLVM
# runs the swig that swig installed under build/. Keeping that one directory
# is what lets those references work unchanged.
DIR="$TC_WORK"

source "$TC_VERSIONS/toolchain.env"

NAME=toolchain-v$TOOLCHAIN_VERSION
PREFIX=/opt/$NAME
SYSROOT=$PREFIX/sysroot
mkdir -p "$PREFIX"

# Cross-target selection. Passed in as a build argument rather than detected,
# so a stage's cache key reflects what it was built for.
for_arm="${TC_FOR_ARM:-false}"
if [[ "$for_arm" = "true" ]]; then
    ARCHIVE_ARCH_TAG="aarch64"
else
    ARCHIVE_ARCH_TAG="x86_64"
fi

TOOLCHAIN_STDCXX="${TOOLCHAIN_STDCXX:-libstdc++}"

# Names the build environment, so it has to describe the base image: binutils
# keys a gprofng workaround off it, and the generated README points at that
# distro's run-dependency list.
DISTRO="${TC_DISTRO:-ubuntu-20.04}"
ENV_SCRIPT_RELATIVE="environment/os/$DISTRO.sh"
