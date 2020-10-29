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

dnf install -y epel-release
dnf update -y

# Install all basic system utils.
dnf install -y \
    openssh-server \
    wget \
    git \
    tmux \
    tree \
    htop

# Install all required to build toolchain.
if [ $include_toolchain_deps == 1 ]; then
    dnf config-manager --set-enabled PowerTools # Required to install texinfo.
    dnf upgrade
    dnf install -y \
        bison \
        automake \
        pcre-devel \
        libedit-devel \
        libxml2-devel \
        libffi-devel \
        readline-devel \
        libcurl-devel \
        texinfo \
        python36 \
        python36-devel \
        xz-devel \
        expat-devel \
        zlib-devel \
        make \
        gcc-c++ \
        gcc
    # Install GDB dependencies not present in the standard repos.
    # https://bugs.centos.org/view.php?id=17068
    # https://centos.pkgs.org
	# Since 2020, there is Babeltrace2 (https://babeltrace.org). Not used
    # within GDB yet (an assumption).
    if ! dnf list installed libipt-devel >/dev/null 2>/dev/null; then
        dnf install -y http://repo.okay.com.mx/centos/8/x86_64/release/libipt-devel-1.6.1-8.el8.x86_64.rpm
    fi
    if ! dnf list installed libbabeltrace-devel >/dev/null 2>/dev/null; then
        dnf install -y http://repo.okay.com.mx/centos/8/x86_64/release/libbabeltrace-devel-1.5.4-2.el8.x86_64.rpm
    fi
fi

# Install all required to build and test memgraph.
pip3 install PyYAML
dnf install -y \
    pkgconfig \
    java-1.8.0-openjdk \
    java-1.8.0-openjdk-devel \
    python36 \
    python36-devel \
    python3-virtualenv \
    python3-pip \
    openssl-devel \
    libseccomp-devel \
    readline-devel \
    nmap-ncat \
    rpm-build \
    rpmlint \
    doxygen \
    graphviz \
    php-cli \
    mono-complete
if ! dnf list installed cl-asdf >/dev/null 2>/dev/null; then
    dnf install -y 	https://pkgs.dyn.su/el8/base/x86_64/cl-asdf-20101028-18.el8.noarch.rpm
fi
if ! dnf list installed common-lisp-controller >/dev/null 2>/dev/null; then
    dnf install -y https://pkgs.dyn.su/el8/base/x86_64/common-lisp-controller-7.4-20.el8.noarch.rpm
fi
if ! dnf list installed sbcl >/dev/null 2>/dev/null; then
    dnf install -y https://pkgs.dyn.su/el8/base/x86_64/sbcl-2.0.1-4.el8.x86_64.rpm
fi
if ! dnf list installed libipt >/dev/null 2>/dev/null; then
    dnf install -y http://repo.okay.com.mx/centos/8/x86_64/release/libipt-1.6.1-8.el8.x86_64.rpm
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
