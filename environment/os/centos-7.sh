#!/bin/bash

set -Eeuo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source "$DIR/../util.sh"

check_operating_system "centos-7"
check_architecture "x86_64"

TOOLCHAIN_BUILD_DEPS=(
    coreutils gcc gcc-c++ make # generic build tools
    wget # used for archive download
    gnupg2 # used for archive signature verification
    tar gzip bzip2 xz unzip # used for archive unpacking
    zlib-devel # zlib library used for all builds
    expat-devel libipt libipt-devel libbabeltrace-devel xz-devel python3-devel # gdb
    texinfo # gdb
    libcurl-devel # cmake
    curl # snappy
    readline-devel # cmake and llvm
    libffi-devel libxml2-devel perl-Digest-MD5 # llvm
    libedit-devel pcre-devel automake bison # swig
    file
    openssl-devel
    gmp-devel
    gperf
    patch
)

TOOLCHAIN_RUN_DEPS=(
    make # generic build tools
    tar gzip bzip2 xz # used for archive unpacking
    zlib # zlib library used for all builds
    expat libipt libbabeltrace xz-libs python3 # for gdb
    readline # for cmake and llvm
    libffi libxml2 # for llvm
    openssl-devel
)

MEMGRAPH_BUILD_DEPS=(
    make cmake pkgconfig # build system
    curl wget # for downloading libs
    libuuid-devel java-11-openjdk # required by antlr
    readline-devel # for memgraph console
    python3-devel # for query modules
    openssl-devel
    libseccomp-devel
    python3 python-virtualenv python3-pip nmap-ncat # for qa, macro_benchmark and stress tests
    #
    # IMPORTANT: python3-yaml does NOT exist on CentOS
    # Install it using `pip3 install PyYAML`
    #
    PyYAML # Package name here does not correspond to the yum package!
    libcurl-devel # mg-requests
    sbcl # for custom Lisp C++ preprocessing
    rpm-build rpmlint # for RPM package building
    doxygen graphviz # source documentation generators
    which mono-complete dotnet-sdk-3.1 golang custom-golang1.18.9 # for driver tests
    nodejs zip unzip java-11-openjdk-devel jdk-17 custom-maven3.9.3 # for driver tests
    autoconf # for jemalloc code generation
    libtool  # for protobuf code generation
    cyrus-sasl-devel
)

MEMGRAPH_RUN_DEPS=(
    logrotate openssl python3 libseccomp
)

NEW_DEPS=(
    wget curl tar gzip
)

list() {
    echo "$1"
}

check() {
    local missing=""
    for pkg in $1; do
        if [ "$pkg" == custom-maven3.9.3 ]; then
            if [ ! -f "/opt/apache-maven-3.9.3/bin/mvn" ]; then
              missing="$pkg $missing"
            fi
            continue
        fi
        if [ "$pkg" == custom-golang1.18.9 ]; then
            if [ ! -f "/opt/go1.18.9/go/bin/go" ]; then
              missing="$pkg $missing"
            fi
            continue
        fi
        if [ "$pkg" == git ]; then
            if ! which "git" >/dev/null; then
                missing="git $missing"
            fi
            continue
        fi
        if [ "$pkg" == "PyYAML" ]; then
            if ! python3 -c "import yaml" >/dev/null 2>/dev/null; then
                missing="$pkg $missing"
            fi
            continue
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
    cd "$DIR"
    if [ "$EUID" -ne 0 ]; then
        echo "Please run as root."
        exit 1
    fi
    # If GitHub Actions runner is installed, append LANG to the environment.
    # Python related tests doesn't work the LANG export.
    if [ -d "/home/gh/actions-runner" ]; then
        echo "LANG=en_US.utf8" >> /home/gh/actions-runner/.env
    else
        echo "NOTE: export LANG=en_US.utf8"
    fi
    yum install -y epel-release
    yum remove -y ius-release
    yum install -y \
      https://repo.ius.io/ius-release-el7.rpm
    yum update -y
    yum install -y wget python3 python3-pip
    yum install -y git

    for pkg in $1; do
        if [ "$pkg" == custom-maven3.9.3 ]; then
            install_custom_maven "3.9.3"
            continue
        fi
        if [ "$pkg" == custom-golang1.18.9 ]; then
            install_custom_golang "1.18.9"
            continue
        fi
        if [ "$pkg" == jdk-17 ]; then
            if ! yum list installed jdk-17 >/dev/null 2>/dev/null; then
                wget https://download.oracle.com/java/17/latest/jdk-17_linux-x64_bin.rpm
                rpm -ivh jdk-17_linux-x64_bin.rpm
                update-alternatives --set java java-11-openjdk.x86_64
                update-alternatives --set javac java-11-openjdk.x86_64
            fi
            continue
        fi
        if [ "$pkg" == libipt ]; then
            if ! yum list installed libipt >/dev/null 2>/dev/null; then
                yum install -y http://repo.okay.com.mx/centos/8/x86_64/release/libipt-1.6.1-8.el8.x86_64.rpm
            fi
            continue
        fi
        if [ "$pkg" == libipt-devel ]; then
            if ! yum list installed libipt-devel >/dev/null 2>/dev/null; then
                yum install -y http://repo.okay.com.mx/centos/8/x86_64/release/libipt-devel-1.6.1-8.el8.x86_64.rpm
            fi
            continue
        fi
        if [ "$pkg" == dotnet-sdk-3.1 ]; then
            if ! yum list installed dotnet-sdk-3.1 >/dev/null 2>/dev/null; then
                wget -nv https://packages.microsoft.com/config/centos/7/packages-microsoft-prod.rpm -O packages-microsoft-prod.rpm
                rpm -Uvh https://packages.microsoft.com/config/centos/7/packages-microsoft-prod.rpm
                yum update -y
                yum install -y dotnet-sdk-3.1
            fi
            continue
        fi
        if [ "$pkg" == PyYAML ]; then
            if [ -z ${SUDO_USER+x} ]; then # Running as root (e.g. Docker).
                pip3 install --user PyYAML
            else # Running using sudo.
                sudo -H -u "$SUDO_USER" bash -c "pip3 install --user PyYAML"
            fi
            continue
        fi
        yum install -y "$pkg"
    done
}

deps=$2"[*]"
"$1" "${!deps}"
