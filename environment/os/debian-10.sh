#!/bin/bash

set -Eeuo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source "$DIR/../util.sh"

TOOLCHAIN_BUILD_DEPS=(
    coreutils gcc g++ build-essential make # generic build tools
    wget # used for archive download
    gnupg # used for archive signature verification
    tar gzip bzip2 xz-utils unzip # used for archive unpacking
    zlib1g-dev # zlib library used for all builds
    libexpat1-dev libipt-dev libbabeltrace-dev liblzma-dev python3-dev texinfo # for gdb
    libcurl4-openssl-dev # for cmake
    libreadline-dev # for cmake and llvm
    libffi-dev libxml2-dev # for llvm
    curl # snappy
    file # for libunwind
    libssl-dev # for libevent
    libgmp-dev # for gdb
    gperf # for proxygen
    git # for fbthrift
    libedit-dev libpcre3-dev automake bison # for swig
)

TOOLCHAIN_RUN_DEPS=(
    make # generic build tools
    tar gzip bzip2 xz-utils # used for archive unpacking
    zlib1g # zlib library used for all builds
    libexpat1 libipt2 libbabeltrace1 liblzma5 python3 # for gdb
    libcurl4 # for cmake
    libreadline7 # for cmake and llvm
    libffi6 libxml2 # for llvm
    libssl-dev # for libevent
)

MEMGRAPH_BUILD_DEPS=(
    git # source code control
    make pkg-config # build system
    curl wget # for downloading libs
    uuid-dev default-jre-headless # required by antlr
    libreadline-dev # for memgraph console
    libpython3-dev python3-dev # for query modules
    libssl-dev
    libseccomp-dev
    netcat # tests are using nc to wait for memgraph
    python3 virtualenv python3-virtualenv python3-pip # for qa, macro_benchmark and stress tests
    python3-yaml # for the configuration generator
    libcurl4-openssl-dev # mg-requests
    sbcl # for custom Lisp C++ preprocessing
    doxygen graphviz # source documentation generators
    mono-runtime mono-mcs zip unzip default-jdk-headless # for driver tests
    dotnet-sdk-3.1 golang nodejs npm
    autoconf # for jemalloc code generation
    libtool  # for protobuf code generation
)

list() {
    echo "$1"
}

check() {
    check_all_dpkg "$1"
}

install() {
    cat >/etc/apt/sources.list <<EOF
deb http://deb.debian.org/debian/ buster main non-free contrib
deb-src http://deb.debian.org/debian/ buster main non-free contrib
deb http://deb.debian.org/debian/ buster-updates main contrib non-free
deb-src http://deb.debian.org/debian/ buster-updates main contrib non-free
deb http://security.debian.org/debian-security buster/updates main contrib non-free
deb-src http://security.debian.org/debian-security buster/updates main contrib non-free
EOF
    cd "$DIR"
    apt --allow-releaseinfo-change update
    # If GitHub Actions runner is installed, append LANG to the environment.
    # Python related tests doesn't work the LANG export.
    if [ -d "/home/gh/actions-runner" ]; then
        echo "LANG=en_US.utf8" >> /home/gh/actions-runner/.env
    else
        echo "NOTE: export LANG=en_US.utf8"
    fi
    apt install -y wget
    for pkg in $1; do
        if [ "$pkg" == dotnet-sdk-3.1 ]; then
            if ! dpkg -s "$pkg" 2>/dev/null >/dev/null; then
                wget -nv https://packages.microsoft.com/config/debian/10/packages-microsoft-prod.deb -O packages-microsoft-prod.deb
                dpkg -i packages-microsoft-prod.deb
                apt-get update
                apt-get install -y apt-transport-https dotnet-sdk-3.1
            fi
            continue
        fi
        apt install -y "$pkg"
    done
}

deps=$2"[*]"
"$1" "${!deps}"
