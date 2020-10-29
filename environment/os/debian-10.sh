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

cat >/etc/apt/sources.list <<EOF
deb http://deb.debian.org/debian/ buster main non-free contrib
deb-src http://deb.debian.org/debian/ buster main non-free contrib
deb http://deb.debian.org/debian/ buster-updates main contrib non-free
deb-src http://deb.debian.org/debian/ buster-updates main contrib non-free
deb http://security.debian.org/debian-security buster/updates main contrib non-free
deb-src http://security.debian.org/debian-security buster/updates main contrib non-free
EOF
apt update

# Install all basic system utils.
apt install -y \
    openssh-server \
    wget \
    git \
    tmux \
    tree \
    htop

# Install all required for the toolchain build.
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
        gnupg \
        make \
        build-essential \
        g++ \
        gcc
fi

# Install all required to build and test memgraph.

# TODO(gitbuda): Install init deps.

wget -nv https://golang.org/dl/go1.15.2.linux-amd64.tar.gz -O go1.15.2.linux-amd64.tar.gz
tar -C /usr/local -xzf go1.15.2.linux-amd64.tar.gz
sed -i '${s/$/:\/usr\/local\/go\/bin/}' /home/gh/actions-runner/.path

apt install -y npm

wget -nv https://packages.microsoft.com/config/debian/10/packages-microsoft-prod.deb -O packages-microsoft-prod.deb
dpkg -i packages-microsoft-prod.deb
apt-get update
apt-get install -y apt-transport-https dotnet-sdk-3.1
