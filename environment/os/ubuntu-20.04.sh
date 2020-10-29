#!/bin/bash

set -Eeuo pipefail

function print_help () {
    echo "Usage: $0 [--include-toolchain-deps]"
}

include_toolchain_deps=0
if [[ $# -gt 1 ]]; then
    print_help
    exit 1
elif  [[ $# -eq 1 ]]; then
    case "$1" in
        --include-toolchain-deps)
        include_toolchain_deps=1
        ;;
        *)
        print_help
        exit 1
        ;;
    esac
fi

apt update

# Install all basic system utils.
apt install -y \
    openssh-server \
    wget \
    git \
    tmux \
    tree \
    htop

# Install all required to build toolchain.
if [ $include_toolchain_deps == 1 ]; then
    apt install -y \
        bison \
        automake \
        libpcre3-dev \
        libedit-dev \
        libxml2-dev \
        libffi-dev \
        libreadline-dev \
        libcurl4-openssl-dev \
        texinfo \
        python3-dev \
        liblzma-dev \
        libbabeltrace-dev \
        libipt-dev \
        libexpat1-dev \
        zlib1g-dev \
        unzip \
        make \
        build-essential \
        g++ \
        gcc
fi

# Install all required to build and test memgraph.
apt install -y \
    pkg-config \
    uuid-dev \
    default-jre-headless \
    default-jdk-headless \
    libssl-dev \
    libseccomp-dev \
    libreadline-dev \
    python3-virtualenv \
    python3-pip \
    sbcl \
    doxygen \
    php-cli \
    mono-runtime \
    mono-mcs \
    nodejs \
    graphviz

# TODO(gitbuda): Install go and dotnet.
