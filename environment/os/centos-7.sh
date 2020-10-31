#!/bin/bash

set -Eeuo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source "$DIR/../util.sh"

TOOLCHAIN_BUILD_DEPS=(
    coreutils gcc gcc-c++ make # generic build tools
    wget # used for archive download
    gnupg2 # used for archive signature verification
    tar gzip bzip2 xz unzip # used for archive unpacking
    zlib-devel # zlib library used for all builds
    expat-devel libipt-devel libbabeltrace-devel xz-devel python3-devel # gdb
    texinfo # gdb
    libcurl-devel # cmake
    readline-devel # cmake and llvm
    libffi-devel libxml2-devel perl-Digest-MD5 # llvm
    libedit-devel pcre-devel automake bison # swig
)
TOOLCHAIN_RUN_DEPS=(
    make # generic build tools
    tar gzip bzip2 xz # used for archive unpacking
    zlib # zlib library used for all builds
    expat libipt libbabeltrace xz-libs python3 # for gdb
    readline # for cmake and llvm
    libffi libxml2 # for llvm
)
MEMGRAPH_BUILD_DEPS=(
    git # source code control
    make pkgconfig # build system
    curl wget # for downloading libs
    libuuid-devel java-1.8.0-openjdk # required by antlr
    readline-devel # for memgraph console
    python3-devel # for query modules
    openssl-devel
    libseccomp-devel
    python3 python-virtualenv python3-pip nmap-ncat # for qa, macro_benchmark and stress tests
    # NOTE: python3-yaml does NOT exist on CentOS
    # Install it manually using `pip3 install PyYAML`
    PyYAML # Package name here does not correspond to the yum package!
    libcurl-devel # mg-requests
    sbcl # for custom Lisp C++ preprocessing
    rpm-build rpmlint # for RPM package building
    doxygen graphviz # source documentation generators
    mono-complete nodejs zip unzip java-1.8.0-openjdk-devel # for driver tests
)
list() {
    echo "$1"
}
check() {
    local missing=""
    for pkg in $1; do
        if [ "$pkg" == "PyYAML" ]; then
            echo "TODO(gitbuda): Implement PyYAML check."
            exit 1
        fi
        if ! yum list installed "$pkg" >/dev/null 2>/dev/null; then
            missing="$pkg $missing"
        fi
    done
    if [ "$missing" != "" ]; then
        echo "MISSING PACKAGES: $missing"
        exit 1
    fi
}
install() {
    yum install -y epel-release
    yum update -y
    yum install python3 python3-pip
    for pkg in $1; do
        if [ "$pkg" == libipt ]; then
            if ! yum list installed libipt >/dev/null 2>/dev/null; then
                yum install -y http://repo.okay.com.mx/centos/8/x86_64/release/libipt-1.6.1-8.el8.x86_64.rpm
            fi
        fi
        if [ "$pkg" == libipt-devel ]; then
            if ! yum list installed libipt-devel >/dev/null 2>/dev/null; then
                yum install -y http://repo.okay.com.mx/centos/8/x86_64/release/libipt-devel-1.6.1-8.el8.x86_64.rpm
            fi
        fi
        if [ "$pkg" == PyYAML ]; then
            pip3 install PyYAML
        fi
        yum install -y "$pkg"
    done

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
}
deps=$2"[*]"
"$1" "${!deps}"
