#!/bin/bash
set -Eeuo pipefail
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source "$DIR/lib.sh"

TOOLCHAIN_BUILD_DEPS=(
    coreutils gcc g++ build-essential make # generic build tools
    wget # used for archive download
    gnupg # used for archive signature verification
    tar gzip bzip2 xz-utils unzip # used for archive unpacking
    zlib1g-dev # zlib library used for all builds
    libexpat1-dev libbabeltrace-dev liblzma-dev python3-dev texinfo # for gdb
    libcurl4-openssl-dev # for cmake
    libreadline-dev # for cmake and llvm
    libffi-dev libxml2-dev # for llvm
    curl # snappy
    file
    git # for thrift
    libgmp-dev # for gdb
    gperf # for proxygen
    libssl-dev
    libedit-dev libpcre2-dev libpcre3-dev automake bison # for swig
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
    g++ # conan tool builds (ninja links -static-libstdc++; pulls libstdc++-dev)
    make cmake pkg-config # build system
    curl wget tar gzip # for downloading and unpacking libs
    gperf # conan libseccomp source build
    libreadline-dev # optional readline support (manual tests)
    libpython3-dev python3-dev # for query modules
    patchelf # POST_BUILD step rewrites memgraph's DT_NEEDED for Python abi3 portability
    libssl-dev # for mgconsole (cloned + built at package time)
    python3 python3-pip python3-virtualenv python3-venv # for conan (runs in a venv) and build tooling
    python3-yaml # for the configuration generator
    custom-rust
    autoconf # for jemalloc code generation
    libtool  # for protobuf code generation
    ninja-build
    libkrb5-dev # for building python gssapi (kerberos auth module)
)

# Extra packages on top of MEMGRAPH_BUILD_DEPS needed to run the test suites.
MEMGRAPH_TEST_DEPS=(
    netcat # tests are using nc to wait for memgraph
    lsof # e2e test runners
    iptables # for stress tests that simulate network failures
    zip unzip openjdk-25-jre-headless openjdk-25-jdk-headless custom-maven # for driver tests (JDK 17 required)
    dotnet-sdk-10.0 golang custom-golang custom-node # for driver tests
    libxmlsec1-dev xmlsec1 # pip xmlsec (SAML SSO) builds from source; no wheels since 1.3.15
    sudo adduser # stress tests set up passwordless sudo for mg (iptables)
)

MEMGRAPH_RUN_DEPS=(
    logrotate openssl python3 libseccomp2
    libkrb5-3 # runtime for python gssapi (kerberos auth module)
)


setup_repos() {
    # dotnet-sdk-10.0 comes from the dotnet backports PPA
    if ! ls /etc/apt/sources.list.d/ | grep -F 'dotnet-ubuntu-backports'; then
        apt-get -y install software-properties-common
        add-apt-repository -y ppa:dotnet/backports
        apt-get update
    fi
    # deadsnakes PPA for non-default python versions
    if ! ls /etc/apt/sources.list.d/ | grep -F 'deadsnakes'; then
        apt-get -y install software-properties-common
        add-apt-repository -y ppa:deadsnakes/ppa
        apt-get update
    fi
}

main "$@"
