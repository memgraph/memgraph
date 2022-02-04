#!/bin/bash

set -Eeuo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source "$DIR/../util.sh"

TOOLCHAIN_BUILD_DEPS=(
    coreutils gcc gcc-c++ make # generic build tools
    wget # used for archive download
    gnupg2 # used for archive signature verification
    tar gzip bzip2 xz unzip # used for archive unpacking
    zlib-devel # zlib library used for all builds
    expat-devel xz-devel python3-devel texinfo # for gdb
    libcurl-devel # for cmake
    curl # snappy
    readline-devel # for cmake and llvm
    libffi-devel libxml2-devel # for llvm
    libedit-devel pcre-devel automake bison # for swig
    file
    openssl-devel
    gmp-devel
    gperf
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
    curl wget # for downloading libs
    libuuid-devel java-11-openjdk # required by antlr
    readline-devel # for memgraph console
    python3-devel # for query modules
    openssl-devel
    libseccomp-devel
    python3 python3-virtualenv python3-pip nmap-ncat # for qa, macro_benchmark and stress tests
    #
    # IMPORTANT: python3-yaml does NOT exist on CentOS
    # Install it manually using `pip3 install PyYAML`
    #
    PyYAML # Package name here does not correspond to the yum package!
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
    for pkg in $1; do
        if [ "$pkg" == "PyYAML" ]; then
            if ! python3 -c "import yaml" >/dev/null 2>/dev/null; then
                missing="$pkg $missing"
            fi
            continue
        fi
        if [ "$pkg" == "python3-virtualenv" ]; then
            continue
        fi
        if [ "$pkg" == sbcl ]; then
            if ! sbcl --version &> /dev/null; then
	        missing="$pkg $missing"
            fi
            continue
        fi
        if ! yum list installed "$pkg" >/dev/null 2>/dev/null; then
            missing="$pkg $missing"
        fi
    done
    if [ "$missing" != "" ]; then
        echo "MISSING PACKAGES: $missing"
        exit 1
    fi
}

install() {
    cd "$DIR"
    if [ "$EUID" -ne 0 ]; then
        echo "Please run as root."
        exit 1
    fi
    # If GitHub Actions runner is installed, append LANG to the environment.
    # Python related tests doesn't work the LANG export.
    if [ -d "/home/gh/actions-runner" ]; then
        echo "LANG=en_US.utf8" >> /home/gh/actions-runner/.env
    else
        echo "NOTE: export LANG=en_US.utf8"
    fi
    yum update -y
    yum install -y wget git python3 python3-pip
    for pkg in $1; do
        if [ "$pkg" == sbcl ]; then
            if ! sbcl --version &> /dev/null; then
	        curl -s https://altushost-swe.dl.sourceforge.net/project/sbcl/sbcl/1.4.2/sbcl-1.4.2-arm64-linux-binary.tar.bz2 -o /tmp/sbcl-arm64.tar.bz2
		tar xvjf /tmp/sbcl-arm64.tar.bz2 -C /tmp
		pushd /tmp/sbcl-1.4.2-arm64-linux
		INSTALL_ROOT=/usr/local sh install.sh
		popd
            fi
            continue
        fi
        if [ "$pkg" == PyYAML ]; then
            if [ -z ${SUDO_USER+x} ]; then # Running as root (e.g. Docker).
                pip3 install --user PyYAML
            else # Running using sudo.
                sudo -H -u "$SUDO_USER" bash -c "pip3 install --user PyYAML"
            fi
            continue
        fi
        if [ "$pkg" == python3-virtualenv ]; then
            if [ -z ${SUDO_USER+x} ]; then # Running as root (e.g. Docker).
                pip3 install --user virtualenv
            else # Running using sudo.
                sudo -H -u "$SUDO_USER" bash -c "pip3 install --user virtualenv"
            fi
            continue
        fi
        yum install -y "$pkg"
    done
}

deps=$2"[*]"
"$1" "${!deps}"
