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
    python3 python3-virtualenv python3-pip # for qa, macro_benchmark and stress tests
    python3-yaml # for the configuration generator
    libcurl4-openssl-dev # mg-requests
    sbcl # for custom Lisp C++ preprocessing
    doxygen graphviz # source documentation generators
    mono-runtime mono-mcs zip unzip default-jdk-headless # for driver tests
    nodejs npm
)
list() {
    echo "$1"
}
check() {
    check_all_dpkg "$1"
}
install() {
    apt update
    apt install -y openssh-server wget git tmux tree htop
    apt install -y $1
    if [ ! -f /usr/local/go/bin/go ]; then
        wget -nv https://golang.org/dl/go1.15.2.linux-amd64.tar.gz -O go1.15.2.linux-amd64.tar.gz
        tar -C /usr/local -xzf go1.15.2.linux-amd64.tar.gz
    fi
    if [ -d "/home/gh/actions-runner" ]; then
        sed -i '${s/$/:\/usr\/local\/go\/bin/}' /home/gh/actions-runner/.path
    fi
    if ! which dotnet >/dev/null; then
        wget -nv https://packages.microsoft.com/config/ubuntu/20.04/packages-microsoft-prod.deb -O packages-microsoft-prod.deb
        dpkg -i packages-microsoft-prod.deb
        apt-get update
        apt-get install -y apt-transport-https dotnet-sdk-3.1
    fi
}
deps=$2"[*]"
"$1" "${!deps}"
