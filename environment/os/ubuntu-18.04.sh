#!/bin/bash

set -Eeuo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source "$DIR/../util.sh"

TOOLCHAIN_BUILD_DEPS=(
    coreutils gcc g++ build-essential make # generic build tools
    wget # archive download
    gnupg # archive signature verification
    tar gzip bzip2 xz-utils unzip # archive unpacking
    zlib1g-dev # zlib library used for all builds
    libexpat1-dev libipt-dev libbabeltrace-dev liblzma-dev python3-dev # gdb
    texinfo # gdb
    libcurl4-openssl-dev # cmake
    libreadline-dev # cmake and llvm
    libffi-dev libxml2-dev # llvm
    curl # snappy
    file
    git # for thrift
    libgmp-dev # for gdb
    gperf # for proxygen
    libssl-dev
    libedit-dev libpcre3-dev automake bison # swig
)

TOOLCHAIN_RUN_DEPS=(
    make # generic build tools
    tar gzip bzip2 xz-utils # used for archive unpacking
    zlib1g # zlib library used for all builds
    libexpat1 libipt1 libbabeltrace1 liblzma5 python3 # for gdb
    libcurl4 # for cmake
    libreadline7 # for cmake and llvm
    libffi6 libxml2 # for llvm
    libssl-dev # for libevent
)

MEMGRAPH_BUILD_DEPS=(
    git # source code control
    make pkg-config # build system
    curl wget # downloading libs
    uuid-dev default-jre-headless # required by antlr
    libreadline-dev # memgraph console
    libpython3-dev python3-dev # for query modules
    libssl-dev
    libseccomp-dev
    python3 virtualenv python3-virtualenv python3-pip # qa, macro bench and stress tests
    python3-yaml # the configuration generator
    libcurl4-openssl-dev # mg-requests
    sbcl # custom Lisp C++ preprocessing
    doxygen graphviz # source documentation generators
    mono-runtime mono-mcs nodejs zip unzip default-jdk-headless # driver tests
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
    apt install -y $1
}

deps=$2"[*]"
"$1" "${!deps}"
