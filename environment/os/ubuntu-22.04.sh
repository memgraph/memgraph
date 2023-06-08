#!/bin/bash

set -Eeuo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source "$DIR/../util.sh"

check_operating_system "ubuntu-22.04"
check_architecture "x86_64"

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
    file
    git # for thrift
    libgmp-dev # for gdb
    gperf # for proxygen
    libssl-dev
    libedit-dev libpcre3-dev automake bison # for swig
)

TOOLCHAIN_RUN_DEPS=(
    make # generic build tools
    tar gzip bzip2 xz-utils # used for archive unpacking
    zlib1g # zlib library used for all builds
    libexpat1 libipt2 libbabeltrace1 liblzma5 python3 # for gdb
    libcurl4 # for cmake
    libreadline8 # for cmake and llvm
    libffi7 libxml2 # for llvm
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
    python3 python3-virtualenv python3-pip # for qa, macro_benchmark and stress tests
    python3-yaml # for the configuration generator
    libcurl4-openssl-dev # mg-requests
    sbcl # for custom Lisp C++ preprocessing
    doxygen graphviz # source documentation generators
    mono-runtime mono-mcs zip unzip default-jdk-headless openjdk-17-jdk-headless # for driver tests
    dotnet-sdk-6.0 golang nodejs npm
    autoconf # for jemalloc code generation
    libtool  # for protobuf code generation
)

MEMGRAPH_RUN_DEPS=(
    logrotate openssl python3 libseccomp2
)

NEW_DEPS=(
    wget curl tar gzip default-jdk-headless openjdk-17-jdk-headless custom-golang1.18.9 maven
)

list() {
    echo "$1"
}

check() {
    local missing=""
    for pkg in $1; do
        if [ "$pkg" == custom-golang1.18.9 ]; then
            if [ ! -f "/opt/go1.18.9/go/bin/go" ]; then
              missing="$pkg $missing"
            fi
            continue
        fi
        if ! dpkg -s "$pkg" >/dev/null 2>/dev/null; then
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
    apt update -y
    apt install -y wget
    # If GitHub Actions runner is installed, append LANG to the environment.
    # Python related tests doesn't work the LANG export.
    if [ -d "/home/gh/actions-runner" ]; then
        echo "LANG=en_US.utf8" >> /home/gh/actions-runner/.env
    else
        echo "NOTE: export LANG=en_US.utf8"
    fi

    for pkg in $1; do
        if [ "$pkg" == custom-golang1.18.9 ]; then
            install_custom_golang "1.18.9"
            continue
        fi
        if [ "$pkg" == dotnet-sdk-6.0 ]; then
            if ! dpkg -s dotnet-sdk-6.0 2>/dev/null >/dev/null; then
                wget -nv https://packages.microsoft.com/config/ubuntu/22.04/packages-microsoft-prod.deb -O packages-microsoft-prod.deb
                dpkg -i packages-microsoft-prod.deb
                apt-get update
                apt-get install -y apt-transport-https dotnet-sdk-6.0
            fi
            continue
        fi
        apt install -y "$pkg"
    done
}

deps=$2"[*]"
"$1" "${!deps}"
