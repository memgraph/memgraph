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

# The build does not know which toolchain version it is producing, and must
# not: the version would have to reach every stage, which means the base layer
# they all descend from, and then bumping it rebuilds all of them even though
# one tool changed. The prefix is fixed here and the packaging stage renames
# the tree, which is safe because the relocate stage leaves every runpath
# $ORIGIN-relative and nothing generated names the prefix.
PREFIX=/opt/toolchain
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

# Activate the toolchain, once it exists.
#
# In the original script this block sat inline just after GCC was installed and
# then applied to everything that followed, because it was all one shell. Each
# stage here is its own process, so it has to live somewhere every stage sees.
#
# The guard reproduces the original ordering exactly: the kernel headers and
# glibc stages run before GCC exists and must use the host compiler, so they
# get none of this; every stage after GCC does. Without it, configure scripts
# fall back to the host cc and host pkg-config, and quietly link host-glibc
# symbols into libraries that are supposed to target the sysroot.
if [[ -x "$PREFIX/bin/gcc" ]]; then
    export PATH=$PREFIX/bin:$PATH
    export LD_LIBRARY_PATH=$PREFIX/lib64
    # Pin CC/CXX so subsequent configure runs (gmp, mpfr, gdb, ...) don't fall
    # back to the host /usr/bin/cc.
    export CC=$PREFIX/bin/gcc
    export CXX=$PREFIX/bin/g++
    # Point pkg-config at the sysroot so configure and cmake resolve the
    # sysroot's .pc files rather than the host's, which would otherwise drag in
    # /usr/include and host-glibc deps. PKG_CONFIG_LIBDIR _replaces_ the search
    # path (no host fallback); PKG_CONFIG_SYSROOT_DIR rewrites -I/-L in them.
    export PKG_CONFIG_LIBDIR=$SYSROOT/usr/lib/pkgconfig:$SYSROOT/usr/lib64/pkgconfig:$SYSROOT/usr/share/pkgconfig
    export PKG_CONFIG_SYSROOT_DIR=$SYSROOT
fi

# Names the build environment, so it has to describe the base image: binutils
# keys a gprofng workaround off it, and the generated README points at that
# distro's run-dependency list.
DISTRO="${TC_DISTRO:-ubuntu-20.04}"
ENV_SCRIPT_RELATIVE="environment/os/$DISTRO.sh"
