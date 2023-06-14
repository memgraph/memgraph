#!/bin/bash

set -Eeuo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source "$DIR/../util.sh"

check_operating_system "fedora-36"
check_architecture "x86_64"

TOOLCHAIN_BUILD_DEPS=(
    coreutils-common gcc gcc-c++ make # generic build tools
    wget # used for archive download
    gnupg2 # used for archive signature verification
    tar gzip bzip2 xz unzip # used for archive unpacking
    zlib-devel # zlib library used for all builds
    expat-devel xz-devel python3-devel texinfo libbabeltrace-devel # for gdb
    curl libcurl-devel # for cmake
    readline-devel # for cmake and llvm
    libffi-devel libxml2-devel # for llvm
    libedit-devel pcre-devel automake bison # for swig
    file
    openssl-devel
    gmp-devel
    gperf
    diffutils
    libipt libipt-devel # intel
    patch
    perl # for openssl
)

TOOLCHAIN_RUN_DEPS=(
    make # generic build tools
    tar gzip bzip2 xz # used for archive unpacking
    zlib # zlib library used for all builds
    expat xz-libs python3 # for gdb
    readline # for cmake and llvm
    libffi libxml2 # for llvm
    openssl-devel
)

MEMGRAPH_BUILD_DEPS=(
    git # source code control
    make pkgconf-pkg-config # build system
    wget # for downloading libs
    libuuid-devel java-11-openjdk # required by antlr
    readline-devel # for memgraph console
    python3-devel # for query modules
    openssl-devel
    libseccomp-devel
    python3 python3-pip python3-virtualenv python3-virtualenvwrapper python3-pyyaml nmap-ncat # for tests
    libcurl-devel # mg-requests
    rpm-build rpmlint # for RPM package building
    doxygen graphviz # source documentation generators
    which nodejs golang zip unzip java-11-openjdk-devel # for driver tests
    sbcl # for custom Lisp C++ preprocessing
    autoconf # for jemalloc code generation
    libtool  # for protobuf code generation
)

list() {
    echo "$1"
}

check() {
    local missing=""
    # On Fedora yum/dnf and python10 use newer glibc which is not compatible
    # with ours, so we need to momentarily disable env
    local OLD_LD_LIBRARY_PATH=${LD_LIBRARY_PATH}
    LD_LIBRARY_PATH=""
    for pkg in $1; do
        if ! dnf list installed "$pkg" >/dev/null 2>/dev/null; then
            missing="$pkg $missing"
        fi
    done
    if [ "$missing" != "" ]; then
        echo "MISSING PACKAGES: $missing"
        exit 1
    fi
    LD_LIBRARY_PATH=${OLD_LD_LIBRARY_PATH}
}

install() {
    cd "$DIR"
    if [ "$EUID" -ne 0 ]; then
        echo "Please run as root."
        exit 1
    fi
    # If GitHub Actions runner is installed, append LANG to the environment.
    # Python related tests don't work without the LANG export.
    if [ -d "/home/gh/actions-runner" ]; then
        echo "LANG=en_US.utf8" >> /home/gh/actions-runner/.env
    else
        echo "NOTE: export LANG=en_US.utf8"
    fi
    dnf update -y
    for pkg in $1; do
        dnf install -y "$pkg"
    done
}

deps=$2"[*]"
"$1" "${!deps}"
