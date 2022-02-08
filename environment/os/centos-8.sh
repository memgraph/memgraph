#!/bin/bash

set -Eeuo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source "$DIR/../util.sh"

TOOLCHAIN_BUILD_DEPS=(
    coreutils-common gcc gcc-c++ make # generic build tools
    wget # used for archive download
    gnupg2 # used for archive signature verification
    tar gzip bzip2 xz unzip # used for archive unpacking
    zlib-devel # zlib library used for all builds
    expat-devel libipt libipt-devel libbabeltrace-devel xz-devel python36-devel texinfo # for gdb
    libcurl-devel # for cmake
    curl # snappy
    readline-devel # for cmake and llvm
    libffi-devel libxml2-devel # for llvm
    libedit-devel pcre-devel automake bison # for swig
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
    expat libipt libbabeltrace xz-libs python36 # for gdb
    readline # for cmake and llvm
    libffi libxml2 # for llvm
    openssl-devel
)

MEMGRAPH_BUILD_DEPS=(
    git # source code control
    make pkgconf-pkg-config # build system
    curl wget # for downloading libs
    libuuid-devel java-11-openjdk # required by antlr
    readline-devel # for memgraph console
    python36-devel # for query modules
    openssl-devel
    libseccomp-devel
    python36 python3-virtualenv python3-pip nmap-ncat # for qa, macro_benchmark and stress tests
    #
    # IMPORTANT: python3-yaml does NOT exist on CentOS
    # Install it manually using `pip3 install PyYAML`
    #
    PyYAML # Package name here does not correspond to the yum package!
    libcurl-devel # mg-requests
    rpm-build rpmlint # for RPM package building
    doxygen graphviz # source documentation generators
    which mono-complete dotnet-sdk-3.1 nodejs golang zip unzip java-11-openjdk-devel # for driver tests
    sbcl # for custom Lisp C++ preprocessing
    autoconf # for jemalloc code generation
    libtool  # for protobuf code generation
)

list() {
    echo "$1"
}

check() {
    local missing=""
    for pkg in $1; do
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
    dnf install -y epel-release
    dnf install -y 'dnf-command(config-manager)'
    dnf config-manager --set-enabled powertools # Required to install texinfo.
    dnf update -y
    dnf install -y wget git python36 python3-pip
    for pkg in $1; do
        if [ "$pkg" == libipt ]; then
            if ! dnf list installed libipt >/dev/null 2>/dev/null; then
                dnf install -y http://repo.okay.com.mx/centos/8/x86_64/release/libipt-1.6.1-8.el8.x86_64.rpm
            fi
            continue
        fi
        if [ "$pkg" == libipt-devel ]; then
            if ! yum list installed libipt-devel >/dev/null 2>/dev/null; then
                dnf install -y http://repo.okay.com.mx/centos/8/x86_64/release/libipt-devel-1.6.1-8.el8.x86_64.rpm
            fi
            continue
        fi
        # Install GDB dependencies not present in the standard repos.
        # https://bugs.centos.org/view.php?id=17068
        # https://centos.pkgs.org
	    # Since 2020, there is Babeltrace2 (https://babeltrace.org). Not used
        # within GDB yet (an assumption).
        # http://mirror.centos.org/centos/8/PowerTools/x86_64/os/Packages/libbabeltrace-devel-1.5.4-3.el8.x86_64.rpm not working
        if [ "$pkg" == libbabeltrace-devel ]; then
            if ! dnf list installed libbabeltrace-devel >/dev/null 2>/dev/null; then
                dnf install -y https://rpmfind.net/linux/centos/8-stream/PowerTools/x86_64/os/Packages/libbabeltrace-devel-1.5.4-3.el8.x86_64.rpm
            fi
            continue
        fi
        if [ "$pkg" == sbcl ]; then
            if ! dnf list installed cl-asdf >/dev/null 2>/dev/null; then
                dnf install -y 	https://pkgs.dyn.su/el8/base/x86_64/cl-asdf-20101028-18.el8.noarch.rpm
            fi
            if ! dnf list installed common-lisp-controller >/dev/null 2>/dev/null; then
                dnf install -y https://pkgs.dyn.su/el8/base/x86_64/common-lisp-controller-7.4-20.el8.noarch.rpm
            fi
            if ! dnf list installed sbcl >/dev/null 2>/dev/null; then
                dnf install -y https://pkgs.dyn.su/el8/base/x86_64/sbcl-2.0.1-4.el8.x86_64.rpm
            fi
            continue
        fi
        if [ "$pkg" == dotnet-sdk-3.1 ]; then
            if ! dnf list installed dotnet-sdk-3.1 >/dev/null 2>/dev/null; then
                wget -nv https://packages.microsoft.com/config/centos/8/packages-microsoft-prod.rpm -O packages-microsoft-prod.rpm
                rpm -Uvh https://packages.microsoft.com/config/centos/8/packages-microsoft-prod.rpm
                dnf update -y
                dnf install -y dotnet-sdk-3.1
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
        dnf install -y "$pkg"
    done
}

deps=$2"[*]"
"$1" "${!deps}"
