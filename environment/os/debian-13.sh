#!/bin/bash
set -Eeuo pipefail
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source "$DIR/lib.sh"

TOOLCHAIN_BUILD_DEPS=(
    coreutils gcc g++ build-essential make binutils binutils-gold # generic build tools
    wget # used for archive download
    gnupg # used for archive signature verification
    tar gzip bzip2 xz-utils unzip # used for archive unpacking
    zlib1g-dev # zlib library used for all builds
    libexpat1-dev libipt-dev libbabeltrace-dev liblzma-dev python3-dev texinfo # for gdb
    libcurl4-openssl-dev # for cmake
    libreadline-dev # for cmake and llvm
    libffi-dev libxml2-dev # for llvm
    libedit-dev libpcre2-dev automake bison # for swig
    curl # snappy
    file # for libunwind
    libssl-dev # for libevent
    libgmp-dev
    gperf # for proxygen
    git # for fbthrift
    custom-rust
    libtool # for protobuf
    libssl-dev pkg-config # for pulsar
    libsasl2-dev # for librdkafka
    python3-pip # for conan
)

TOOLCHAIN_RUN_DEPS=(
    make # generic build tools
    tar gzip bzip2 xz-utils # used for archive unpacking
    zlib1g # zlib library used for all builds
    python3 # llvm helper scripts; gdb ships its own python via the sysroot
)

MEMGRAPH_BUILD_DEPS=(
    git # source code control
    g++ libstdc++-14-dev # conan tool builds (ninja links -static-libstdc++)
    make cmake pkg-config # build system
    curl wget # for downloading libs
    gperf # conan libseccomp source build
    libreadline-dev # optional readline support (manual tests)
    libpython3-dev python3-dev # for query modules
    patchelf # POST_BUILD step rewrites memgraph's DT_NEEDED for Python abi3 portability
    libssl-dev # for mgconsole (cloned + built at package time)
    netcat-traditional # tests are using nc to wait for memgraph
    lsof # e2e test runners
    python3 virtualenv python3-virtualenv python3-pip python3-venv # for qa, macro_benchmark and stress tests
    python3-yaml # for the configuration generator
    custom-rust
    zip unzip openjdk-25-jdk-headless openjdk-25-jre-headless custom-maven # for driver tests
    dotnet-sdk-10.0 golang custom-golang nodejs npm # for driver tests
    autoconf # for jemalloc code generation
    libtool  # for protobuf code generation
    ninja-build
    libkrb5-dev # for building python gssapi (kerberos auth module)
)

MEMGRAPH_RUN_DEPS=(
    logrotate openssl python3 libseccomp2
    libkrb5-3 # runtime for python gssapi (kerberos auth module)
)

NEW_DEPS=(
    wget curl tar gzip
)

setup_repos() {
    # dotnet-sdk-10.0 comes from the Microsoft package repo
    if ! dpkg -s packages-microsoft-prod &>/dev/null; then
        wget -nv https://packages.microsoft.com/config/debian/13/packages-microsoft-prod.deb -O packages-microsoft-prod.deb
        dpkg -i packages-microsoft-prod.deb
        apt update -y
    fi
}

main "$@"
