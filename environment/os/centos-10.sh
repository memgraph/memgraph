#!/bin/bash
set -Eeuo pipefail
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source "$DIR/../util.sh"

check_operating_system "centos-10"
check_architecture "x86_64"

TOOLCHAIN_BUILD_DEPS=(
    wget # used for archive download
    coreutils-common gcc gcc-c++ make # generic build tools
    # NOTE: Pure libcurl conflicts with libcurl-minimal
    libcurl-devel # cmake build requires it
    gnupg2 # used for archive signature verification
    tar gzip bzip2 xz unzip # used for archive unpacking
    zlib-ng-compat-devel # zlib library used for all builds
    expat-devel xz-devel python3-devel texinfo libbabeltrace-devel # for gdb
    readline-devel # for cmake and llvm
    libffi-devel libxml2-devel # for llvm
    libedit-devel pcre2-devel automake bison # for swig
    file gmp-devel gperf diffutils
    libipt libipt-devel # intel
    patch
    custom-rust # for mgcxx
    libtool # for protobuf
    openssl-devel pkgconf-pkg-config # for pulsar
    cyrus-sasl-devel # for librdkafka
)

TOOLCHAIN_RUN_DEPS=(
    make # generic build tools
    tar gzip bzip2 xz # used for archive unpacking
    zlib-ng-compat # zlib library used for all builds
    expat xz-libs python3 # for gdb
    readline # for cmake and llvm
    libffi libxml2 # for llvm
    openssl-devel
    perl # for openssl
)

MEMGRAPH_BUILD_DEPS=(
    git # source code control
    make cmake pkgconf-pkg-config # build system
    wget # for downloading libs
    libuuid-devel java-11-openjdk-headless java-11-openjdk java-11-openjdk-devel # required by antlr
    readline-devel # for memgraph console
    python3-devel # for query modules
    openssl-devel
    libseccomp-devel
    python3 python3-pip python3-virtualenv nmap-ncat # for qa, macro_benchmark and stress tests
    #
    # IMPORTANT: python3-yaml does NOT exist on CentOS
    # Install it manually using `pip3 install PyYAML`
    #
    PyYAML # Package name here does not correspond to the yum package!
    libcurl-devel # mg-requests
    rpm-build rpmlint # for RPM package building
    doxygen graphviz # source documentation generators
    which nodejs golang custom-golang1.18.9 # for driver tests
    zip unzip java-17-openjdk-headless java-17-openjdk java-17-openjdk-devel custom-maven3.9.3 # for driver tests
    sbcl # for custom Lisp C++ preprocessing
    autoconf # for jemalloc code generation
    libtool  # for protobuf code generation
    cyrus-sasl-devel
    ninja-build
)

MEMGRAPH_TEST_DEPS="${MEMGRAPH_BUILD_DEPS[*]}"

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
        if [ "$pkg" == custom-rust ]; then
            if [ ! -x "$HOME/.cargo/bin/rustup" ]; then
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
        if [ "$pkg" == "python3-virtualenv" ]; then
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

    # enable EPEL repo for rpmlint
    sudo dnf install -y epel-release

    dnf install -y wget git python3 python3-pip
    # CRB repo is required for, e.g. texinfo, ninja-build
    dnf config-manager --set-enabled crb

    for pkg in $1; do
        if [ "$pkg" == custom-maven3.9.3 ]; then
            install_custom_maven "3.9.3"
            continue
        fi
        if [ "$pkg" == custom-golang1.18.9 ]; then
            install_custom_golang "1.18.9"
            continue
        fi
        if [ "$pkg" == custom-rust ]; then
            install_rust "1.80"
            continue
        fi
        if [ "$pkg" == custom-node ]; then
            install_node "20"
            continue
        fi

        # Since there is no support for libipt-devel for CentOS 9 we install
        # Fedoras version of same libs, they are the same version but released
        # for different OS
        # TODO Update when libipt-devel releases for CentOS 9
        if [ "$pkg" == libipt ]; then
            if ! dnf list installed libipt >/dev/null 2>/dev/null; then
                dnf install -y https://dl.fedoraproject.org/pub/fedora/linux/releases/41/Everything/x86_64/os/Packages/l/libipt-2.1.1-2.fc41.x86_64.rpm
            fi
            continue
        fi
        if [ "$pkg" == libipt-devel ]; then
            if ! dnf list installed libipt-devel >/dev/null 2>/dev/null; then
                dnf install -y https://dl.fedoraproject.org/pub/fedora/linux/releases/41/Everything/x86_64/os/Packages/l/libipt-devel-2.1.1-2.fc41.x86_64.rpm
            fi
            continue
        fi
        if [ "$pkg" == libbabeltrace-devel ]; then
            if ! dnf list installed libbabeltrace-devel >/dev/null 2>/dev/null; then
                dnf install -y https://mirror.stream.centos.org/10-stream/CRB/x86_64/os/Packages/libbabeltrace-devel-1.5.11-9.el10.x86_64.rpm
            fi
            continue
        fi
        if [ "$pkg" == sbcl ]; then
            if ! dnf list installed cl-asdf >/dev/null 2>/dev/null; then
                dnf install -y http://www.nosuchhost.net/~cheese/fedora/packages/epel-9/x86_64/cl-asdf-20101028-27.el9.noarch.rpm
            fi
            if ! dnf list installed common-lisp-controller >/dev/null 2>/dev/null; then
                dnf install -y http://www.nosuchhost.net/~cheese/fedora/packages/epel-9/x86_64/common-lisp-controller-7.4-29.el9.noarch.rpm
            fi
            if ! dnf list installed sbcl >/dev/null 2>/dev/null; then
                dnf install -y http://www.nosuchhost.net/~cheese/fedora/packages/epel-9/x86_64/sbcl-2.3.11-3.el9~bootstrap.x86_64.rpm
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
        if [ "$pkg" == python3-virtualenv ]; then
            if [ -z ${SUDO_USER+x} ]; then # Running as root (e.g. Docker).
                pip3 install virtualenv
                pip3 install virtualenvwrapper
            else # Running using sudo.
                sudo -H -u "$SUDO_USER" bash -c "pip3 install virtualenv"
                sudo -H -u "$SUDO_USER" bash -c "pip3 install virtualenvwrapper"
            fi
            continue
        fi

        if [ "$pkg" == java-11-openjdk-headless ]; then
            if ! dnf list installed java-11-openjdk-headless >/dev/null 2>/dev/null; then
                dnf install -y https://mirror.stream.centos.org/9-stream/AppStream/x86_64/os/Packages/java-11-openjdk-headless-11.0.20.1.1-2.el9.x86_64.rpm
            fi
            continue
        fi
        if [ "$pkg" == java-11-openjdk ]; then
            if ! dnf list installed java-11-openjdk >/dev/null 2>/dev/null; then
                dnf install -y https://mirror.stream.centos.org/9-stream/AppStream/x86_64/os/Packages/java-11-openjdk-11.0.20.1.1-2.el9.x86_64.rpm
            fi
            continue
        fi
        if [ "$pkg" == java-11-openjdk-devel ]; then
            if ! dnf list installed java-11-openjdk-devel >/dev/null 2>/dev/null; then
                dnf install -y https://mirror.stream.centos.org/9-stream/AppStream/x86_64/os/Packages/java-11-openjdk-devel-11.0.20.1.1-2.el9.x86_64.rpm
            fi
            continue
        fi
        if [ "$pkg" == java-17-openjdk-headless ]; then
            if ! dnf list installed java-17-openjdk-headless >/dev/null 2>/dev/null; then
                dnf install -y https://mirror.stream.centos.org/9-stream/AppStream/x86_64/os/Packages/java-17-openjdk-headless-17.0.13.0.11-4.el9.x86_64.rpm
            fi
            continue
        fi
        if [ "$pkg" == java-17-openjdk ]; then
            if ! dnf list installed java-17-openjdk >/dev/null 2>/dev/null; then
                dnf install -y https://mirror.stream.centos.org/9-stream/AppStream/x86_64/os/Packages/java-17-openjdk-17.0.13.0.11-4.el9.x86_64.rpm
            fi
            continue
        fi
        if [ "$pkg" == java-17-openjdk-devel ]; then
            if ! dnf list installed java-17-openjdk-devel >/dev/null 2>/dev/null; then
                dnf install -y https://mirror.stream.centos.org/9-stream/AppStream/x86_64/os/Packages/java-17-openjdk-devel-17.0.13.0.11-4.el9.x86_64.rpm
            fi
            continue
        fi

        dnf install -y "$pkg"
    done
}

deps=$2"[*]"
"$1" "${!deps}"
