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

# Fetch a source archive into the download cache and check it against a digest.
#
# The digest is checked every time, not only when the file is downloaded: the
# cache is a mount that outlives the build, so an archive already sitting there
# has not been looked at since it arrived. The algorithm follows from the length
# of the digest, so there is no third argument to get wrong.
#
#   fetch <url> <digest> [filename]
fetch () {
    local url="$1" digest="$2" file="${3:-}" rc=0
    [[ -n "$file" ]] || file="${url##*/}"
    pushd "$TC_ARCHIVES"
    # The status is carried rather than left to the last command: popd runs
    # after the check and would otherwise report success over a bad digest.
    [[ -f "$file" ]] || wget --https-only -O "$file" "$url" || rc=1
    if (( rc == 0 )); then
        case "${#digest}" in
            64)  echo "$digest  $file" | sha256sum -c - || rc=1 ;;
            128) echo "$digest  $file" | sha512sum -c - || rc=1 ;;
            *)   echo "fetch: $file: $digest is neither a sha256 nor a sha512" >&2
                 rc=1 ;;
        esac
    fi
    popd
    return $rc
}

# Unpack an archive from the cache and leave the shell inside the tree it
# unpacked, ready for the recipe. Paired with leave_source.
#
# The tar flag follows from the extension. Choosing it by hand is how one
# archive came to be unpacked with -xvf and the next with -xzf for no reason,
# and how a recipe can end up unpacking nothing and building the last tree that
# happened to be there.
#
#   enter_source <archive> <directory it unpacks to>
enter_source () {
    local file="$1" dir="$2" rc=0
    pushd "$TC_BUILD"
    case "$file" in
        *.tar.gz|*.tgz) tar -xzf "$TC_ARCHIVES/$file" || rc=1 ;;
        *.tar.xz)       tar -xJf "$TC_ARCHIVES/$file" || rc=1 ;;
        *.tar.bz2)      tar -xjf "$TC_ARCHIVES/$file" || rc=1 ;;
        *.zip)          unzip -q "$TC_ARCHIVES/$file" || rc=1 ;;
        *) echo "enter_source: $file: unknown archive type" >&2; rc=1 ;;
    esac
    if (( rc == 0 )) && [[ ! -d "$dir" ]]; then
        echo "enter_source: $file did not unpack to $dir" >&2
        rc=1
    fi
    # On failure the build directory is left as it was found, so a recipe that
    # ignores the status does not carry on inside the wrong tree.
    if (( rc != 0 )); then
        popd
        return $rc
    fi
    pushd "$dir"
}

# Leave the tree enter_source entered, and the build directory with it.
leave_source () {
    popd
    popd
}

# A git tag is a movable pointer, so cloning one says what the source was
# called rather than what it was. Checking the commit we actually got turns a
# moved tag into a failed build instead of a toolchain quietly built from
# something else. Run from inside the clone.
require_commit () {
    local want="$1" got
    got="$(git rev-parse HEAD)"
    if [[ "$got" != "$want" ]]; then
        echo "  expected commit $want" >&2
        echo "  got               $got" >&2
        echo "  the tag has moved, or the pin is wrong" >&2
        exit 1
    fi
}

# The runtimes LLVM builds depend on which standard library was asked for. Both
# the llvm stage and the packaging stage need the answer -- one to build it and
# one to write it in the README -- so the rule is applied here rather than in
# each of them. Reads LLVM_RUNTIMES, so the caller must have sourced llvm.env.
mg_llvm_runtimes () {
    if [[ "$TOOLCHAIN_STDCXX" = "libc++" ]]; then
        echo "$LLVM_RUNTIMES;libcxx;libcxxabi"
    else
        echo "$LLVM_RUNTIMES"
    fi
}

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
