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
    expat-devel libipt-devel libbabeltrace-devel xz-devel python36-devel texinfo # for gdb
    libcurl-devel # for cmake
    readline-devel # for cmake and llvm
    libffi-devel libxml2-devel # for llvm
    libedit-devel pcre-devel automake bison # for swig
)
TOOLCHAIN_RUN_DEPS=(
    make # generic build tools
    tar gzip bzip2 xz # used for archive unpacking
    zlib # zlib library used for all builds
    expat libipt libbabeltrace xz-libs python36 # for gdb
    readline # for cmake and llvm
    libffi libxml2 # for llvm
)
MEMGRAPH_BUILD_DEPS=(
    git # source code control
    make pkgconf-pkg-config # build system
    curl wget # for downloading libs
    libuuid-devel java-1.8.0-openjdk # required by antlr
    readline-devel # for memgraph console
    python36-devel # for query modules
    openssl-devel
    libseccomp-devel
    python36 python3-virtualenv python3-pip nmap-ncat # for qa, macro_benchmark and stress tests
    # NOTE: python3-yaml does NOT exist on CentOS
    # Install it manually using `pip3 install PyYAML`
    PyYAML # Package name here does not correspond to the yum package!
    libcurl-devel # mg-requests
    rpm-build rpmlint # for RPM package building
    doxygen graphviz # source documentation generators
    mono-complete nodejs zip unzip java-1.8.0-openjdk-devel # for driver tests
    sbcl # for custom Lisp C++ preprocessing
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
    dnf install -y epel-release
    dnf config-manager --set-enabled PowerTools # Required to install texinfo.
    dnf update -y
    dnf install -y openssh-server wget git tmux tree htop
    for pkg in $1; do
        if [ "$pkg" == libipt ]; then
            if ! dnf list installed libipt >/dev/null 2>/dev/null; then
                dnf install -y http://repo.okay.com.mx/centos/8/x86_64/release/libipt-1.6.1-8.el8.x86_64.rpm
            fi
        fi
        if [ "$pkg" == libipt-devel ]; then
            if ! yum list installed libipt-devel >/dev/null 2>/dev/null; then
                dnf install -y http://repo.okay.com.mx/centos/8/x86_64/release/libipt-devel-1.6.1-8.el8.x86_64.rpm
            fi
        fi
        # Install GDB dependencies not present in the standard repos.
        # https://bugs.centos.org/view.php?id=17068
        # https://centos.pkgs.org
	    # Since 2020, there is Babeltrace2 (https://babeltrace.org). Not used
        # within GDB yet (an assumption).
        if [ "$pkg" == libbabeltrace-devel ]; then
            if ! dnf list installed libbabeltrace-devel >/dev/null 2>/dev/null; then
                dnf install -y http://repo.okay.com.mx/centos/8/x86_64/release/libbabeltrace-devel-1.5.4-2.el8.x86_64.rpm
            fi
        fi
        if [ "$pkg" == PyYAML ]; then
            pip3 install PyYAML
        fi
        dnf install -y "$pkg"
    done

    if ! dnf list installed cl-asdf >/dev/null 2>/dev/null; then
        dnf install -y 	https://pkgs.dyn.su/el8/base/x86_64/cl-asdf-20101028-18.el8.noarch.rpm
    fi
    if ! dnf list installed common-lisp-controller >/dev/null 2>/dev/null; then
        dnf install -y https://pkgs.dyn.su/el8/base/x86_64/common-lisp-controller-7.4-20.el8.noarch.rpm
    fi
    if ! dnf list installed sbcl >/dev/null 2>/dev/null; then
        dnf install -y https://pkgs.dyn.su/el8/base/x86_64/sbcl-2.0.1-4.el8.x86_64.rpm
    fi
    if [ ! -f /usr/local/go/bin/go ]; then
        wget -nv https://golang.org/dl/go1.15.2.linux-amd64.tar.gz -O go1.15.2.linux-amd64.tar.gz
        tar -C /usr/local -xzf go1.15.2.linux-amd64.tar.gz
    fi
    if [ -d "/home/gh/actions-runner" ]; then
        sed -i '${s/$/:\/usr\/local\/go\/bin/}' /home/gh/actions-runner/.path
    fi
    if ! which node >/dev/null; then
        curl -sL https://rpm.nodesource.com/setup_12.x | bash -
        yum update
        yum remove -y npm nodejs
        yum install -y nodejs
    fi
    if ! which dotnet >/dev/null; then
        wget -nv https://packages.microsoft.com/config/centos/8/packages-microsoft-prod.rpm -O packages-microsoft-prod.rpm
        rpm -Uvh https://packages.microsoft.com/config/centos/8/packages-microsoft-prod.rpm
        yum update -y
        yum install -y dotnet-sdk-3.1
    fi
}
deps=$2"[*]"
"$1" "${!deps}"
