#!/bin/bash

set -Eeuo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source "$DIR/../util.sh"

check_operating_system "amzn-2"
check_architecture "x86_64"

TOOLCHAIN_BUILD_DEPS=(
    gcc gcc-c++ make # generic build tools
    wget # used for archive download
    gnupg2 # used for archive signature verification
    tar gzip bzip2 xz unzip # used for archive unpacking
    zlib-devel # zlib library used for all builds
    expat-devel xz-devel python3-devel texinfo
    curl libcurl-devel # for cmake
    readline-devel # for cmake and llvm
    libffi-devel libxml2-devel # for llvm
    libedit-devel pcre-devel pcre2-devel automake bison # for swig
    file
    openssl-devel
    gmp-devel
    gperf
    diffutils
    patch
    libipt libipt-devel # intel
    perl # for openssl
)

TOOLCHAIN_RUN_DEPS=(
    make # generic build tools
    tar gzip bzip2 xz # used for archive unpacking
    zlib # zlib library used for all builds
    expat xz-libs python3 # for gdb
    readline # for cmake and llvm
    libffi libxml2 # for llvm
    openssl-devel
)

MEMGRAPH_BUILD_DEPS=(
    git # source code control
    make cmake # build system
    wget # for downloading libs
    libuuid-devel java-11-openjdk # required by antlr
    readline-devel # for memgraph console
    python3-devel # for query modules
    openssl-devel
    libseccomp-devel
    python3 python3-pip nmap-ncat # for tests
    #
    # IMPORTANT: python3-yaml does NOT exist on CentOS
    # Install it using `pip3 install PyYAML`
    #
    PyYAML # Package name here does not correspond to the yum package!
    libcurl-devel # mg-requests
    rpm-build rpmlint # for RPM package building
    doxygen graphviz # source documentation generators
    which nodejs golang custom-golang1.18.9 zip unzip java-11-openjdk-devel jdk-17 custom-maven3.9.3 # for driver tests
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
    # On Fedora yum/dnf and python10 use newer glibc which is not compatible
    # with ours, so we need to momentarely disable env
    local OLD_LD_LIBRARY_PATH=${LD_LIBRARY_PATH:-""}
    LD_LIBRARY_PATH=""
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
    LD_LIBRARY_PATH=${OLD_LD_LIBRARY_PATH}
}

install() {
    cd "$DIR"
    if [ "$EUID" -ne 0 ]; then
        echo "Please run as root."
        exit 1
    fi
    # If GitHub Actions runner is installed, append LANG to the environment.
    # Python related tests don't work without the LANG export.
    if [ -d "/home/gh/actions-runner" ]; then
        echo "LANG=en_US.utf8" >> /home/gh/actions-runner/.env
    else
        echo "NOTE: export LANG=en_US.utf8"
    fi

    yum update -y
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
              wget --no-check-certificate -c --header "Cookie: oraclelicense=accept-securebackup-cookie" https://download.oracle.com/java/17/latest/jdk-17_linux-x64_bin.rpm
              rpm -Uvh jdk-17_linux-x64_bin.rpm
              # NOTE: Set Java 11 as default.
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
        if [ "$pkg" == nodejs ]; then
            if ! yum list installed nodejs >/dev/null 2>/dev/null; then
              yum install https://rpm.nodesource.com/pub_16.x/nodistro/repo/nodesource-release-nodistro-1.noarch.rpm -y
              yum install nodejs -y --setopt=nodesource-nodejs.module_hotfixes=1
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
        if [ "$pkg" == java-11-openjdk ]; then
            amazon-linux-extras install -y java-openjdk11
            continue
        fi
        if [ "$pkg" == java-11-openjdk-devel ]; then
            amazon-linux-extras install -y java-openjdk11
            yum install -y java-11-openjdk-devel
            continue
        fi
        yum install -y "$pkg"
    done
}

deps=$2"[*]"
"$1" "${!deps}"
