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

# Install all required to build toolchain.
if [ $include_toolchain_deps == 1 ]; then
	yum install -y bison automake pcre-devel libedit-devel libxml2-devel \
        libffi-devel readline-devel libcurl-devel texinfo python3-devel \
        xz-devel libbabeltrace-devel expat-devel zlib-devel \
        unzip bzip2 wget gcc-c++ gcc
    if ! yum list installed libipt >/dev/null 2>/dev/null; then
        yum install -y http://repo.okay.com.mx/centos/8/x86_64/release/libipt-1.6.1-8.el8.x86_64.rpm
    fi
    if ! yum list installed libipt-devel >/dev/null 2>/dev/null; then
        yum install -y http://repo.okay.com.mx/centos/8/x86_64/release/libipt-devel-1.6.1-8.el8.x86_64.rpm
    fi
fi

exit 1

# Install all required for Memgraph build.
yum install -y epel-release
yum update -y
yum install -y wget git tmux htop make tar gzip bzip2 xz zlib expat \
    libbabeltrace xz-libs python3 readline libffi libxml2 libuuid-devel \
    java-1.8.0-openjdk readline-devel python3-devel openssl-devel \
    libseccomp-devel python-virtualenv nmap-ncat libcurl-devel sbcl rpm-build \
    rpmlint bison automake libedit-devel libxml2-devel libffi-devel \
    texinfo xz-devel libbabeltrace-devel expat-devel gcc-c++ sbcl gcc
pip3 install PyYAML
# There is no libipt for CentOS7, the one for CentOS8 should work fine.
rpm -q libipt
if [ $? -ne 0 ]; then
    yum install -y http://repo.okay.com.mx/centos/8/x86_64/release/libipt-1.6.1-8.el8.x86_64.rpm
else
    echo "libipt is already installed."
fi

# Install all required to run driver tests.
wget -nv https://golang.org/dl/go1.15.2.linux-amd64.tar.gz -O go1.15.2.linux-amd64.tar.gz
tar -C /usr/local -xzf go1.15.2.linux-amd64.tar.gz
# If GitHub Actions runner is installed, append go path to the .path file.
if [ -d "/home/gh/actions-runner" ]; then
    sed -i '${s/$/:\/usr\/local\/go\/bin/}' /home/gh/actions-runner/.path
fi

curl -sL https://rpm.nodesource.com/setup_12.x | bash -
yum update
yum remove -y npm nodejs
yum install -y nodejs

wget -nv https://packages.microsoft.com/config/centos/7/packages-microsoft-prod.rpm -O packages-microsoft-prod.rpm
rpm -Uvh https://packages.microsoft.com/config/centos/7/packages-microsoft-prod.rpm
yum update -y
yum install -y dotnet-sdk-3.1
