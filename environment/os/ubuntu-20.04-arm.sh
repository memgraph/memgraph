#!/bin/bash
set -Eeuo pipefail
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source "$DIR/lib.sh"

TOOLCHAIN_BUILD_DEPS=(
    coreutils gcc g++ build-essential make # generic build tools
    file # libtool library checks (swig)
    wget # archive download
    gnupg # archive signature verification
    tar gzip bzip2 xz-utils unzip # archive unpacking (unzip: pahole)
    git # LLVM / mgconsole / heaptrack clones
    rsync # kernel headers_install
    gawk bison python3 # glibc (bison also: binutils gprofng, swig)
    m4 # gcc's in-tree gmp
    perl # openssl Configure
    pkg-config # sysroot .pc resolution (curl, python)
    autoconf automake libtool # swig autogen.sh
    zlib1g-dev libbz2-dev liblzma-dev libzstd-dev # heaptrack static compression deps
    libdw-dev # heaptrack (pulls libelf-dev)
    libboost-filesystem-dev libboost-program-options-dev libboost-iostreams-dev libboost-system-dev # for heaptrack
)

TOOLCHAIN_RUN_DEPS=(
    make # generic build tools
    tar gzip bzip2 xz-utils # archive unpacking
    zlib1g # zlib library used for all builds
    python3 # llvm helper scripts; gdb ships its own python via the sysroot
)

MEMGRAPH_BUILD_DEPS=(
    git # source code control
    make cmake pkg-config # build system
    curl wget # for downloading libs
    uuid-dev default-jre-headless # required by antlr
    libreadline-dev # for memgraph console
    libpython3-dev python3-dev # for query modules
    libssl-dev
    libseccomp-dev
    netcat # tests are using nc to wait for memgraph
    python3 python3-virtualenv python3-pip python3-venv # for qa, macro_benchmark and stress tests
    python3-yaml # for the configuration generator
    libcurl4-openssl-dev # mg-requests
    sbcl # for custom Lisp C++ preprocessing
    mono-runtime mono-mcs zip unzip default-jdk-headless openjdk-17-jdk-headless custom-maven # for driver tests
    dotnet-sdk-6.0 golang custom-golang custom-node # for driver tests
    autoconf # for jemalloc code generation
    libtool  # for protobuf code generation
    libsasl2-dev
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

SPECIAL_PACKAGES=(dotnet-sdk-6.0)

install_special_package() {
    case "$1" in
        dotnet-sdk-6.0)
            if ! dpkg -s dotnet-sdk-6.0 &>/dev/null; then
                wget -nv https://packages.microsoft.com/config/ubuntu/20.04/packages-microsoft-prod.deb -O packages-microsoft-prod.deb
                dpkg -i packages-microsoft-prod.deb
                apt-get update
                apt-get install -y dotnet-sdk-6.0
            fi
            ;;
    esac
}

post_install() {
    if dpkg -s openjdk-17-jdk-headless &>/dev/null; then
        # The default Java version should be Java 11
        update-alternatives --set java /usr/lib/jvm/java-11-openjdk-arm64/bin/java
        update-alternatives --set javac /usr/lib/jvm/java-11-openjdk-arm64/bin/javac
    fi
}

main "$@"
