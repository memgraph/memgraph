#!/bin/bash
set -Eeuo pipefail
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source "$DIR/../util.sh"

check_operating_system "centos-9"
check_architecture "x86_64"

TOOLCHAIN_BUILD_DEPS=(
    wget # used for archive download
    coreutils-common gcc gcc-c++ make # generic build tools
    # NOTE: Pure libcurl conflicts with libcurl-minimal
    libcurl-devel # cmake build requires it
    gnupg2 # used for archive signature verification
    tar gzip bzip2 xz unzip # used for archive unpacking
    zlib-devel # zlib library used for all builds
    expat-devel xz-devel python3-devel texinfo libbabeltrace-devel # for gdb
    readline-devel # for cmake and llvm
    libffi-devel libxml2-devel # for llvm
    libedit-devel pcre-devel pcre2-devel automake bison # for swig
    file
    openssl-devel
    gmp-devel
    gperf
    diffutils
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
    zlib # zlib library used for all builds
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
    libuuid-devel java-11-openjdk # required by antlr
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
    zip unzip java-11-openjdk-devel java-17-openjdk java-17-openjdk-devel custom-maven3.9.3 # for driver tests
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
    local -n packages=$1
    local missing=""
    local missing_custom=""

    # Separate standard and custom packages
    local standard_packages=()
    local custom_packages=()

    for pkg in "${packages[@]}"; do
        case "$pkg" in
            custom-*|PyYAML|python3-virtualenv)
                custom_packages+=("$pkg")
                ;;
            *)
                standard_packages+=("$pkg")
                ;;
        esac
    done

    # Check standard packages with Python script
    if [ ${#standard_packages[@]} -gt 0 ]; then
        missing=$(python3 "$DIR/check-packages.py" "check" "centos-9" "${standard_packages[@]}")
    fi

    # Check custom packages with bash logic
    for pkg in "${custom_packages[@]}"; do
        case "$pkg" in
            custom-maven3.9.3)
                if [ ! -f "/opt/apache-maven-3.9.3/bin/mvn" ]; then
                    missing_custom="$pkg $missing_custom"
                fi
                ;;
            custom-golang1.18.9)
                if [ ! -f "/opt/go1.18.9/go/bin/go" ]; then
                    missing_custom="$pkg $missing_custom"
                fi
                ;;
            custom-rust)
                if [ ! -x "$HOME/.cargo/bin/rustup" ]; then
                    missing_custom="$pkg $missing_custom"
                fi
                ;;
            PyYAML)
                if ! python3 -c "import yaml" >/dev/null 2>/dev/null; then
                    missing_custom="$pkg $missing_custom"
                fi
                ;;
            python3-virtualenv)
                # Skip this as it's handled during installation
                ;;
        esac
    done

    # Combine missing packages
    if [ -n "$missing" ] && [ -n "$missing_custom" ]; then
        missing="$missing $missing_custom"
    elif [ -n "$missing_custom" ]; then
        missing="$missing_custom"
    fi

    if [ -n "$missing" ]; then
        echo "MISSING PACKAGES: $missing"
        exit 1
    fi
}

install() {
    if [ "$EUID" -ne 0 ]; then
        echo "Please run as root."
        exit 1
    fi

    local -n packages=$1

    # If GitHub Actions runner is installed, append LANG to the environment.
    # Python related tests doesn't work the LANG export.
    if [ -d "/home/gh/actions-runner" ]; then
        echo "LANG=en_US.utf8" >> /home/gh/actions-runner/.env
    else
        echo "NOTE: export LANG=en_US.utf8"
    fi

    # --nobest is used because of libipt because we install custom versions
    # because libipt-devel is not available on CentOS 9 Stream
    dnf update -y --nobest
    dnf install -y wget git python3 python3-pip
    dnf config-manager --set-enabled crb

    # Enable EPEL for additional packages
    dnf install -y epel-release epel-next-release

    # Separate standard and custom packages
    local standard_packages=()
    local custom_packages=()

    for pkg in "${packages[@]}"; do
        case "$pkg" in
            custom-*|PyYAML|python3-virtualenv|libipt|libipt-devel)
                custom_packages+=("$pkg")
                ;;
            *)
                standard_packages+=("$pkg")
                ;;
        esac
    done

    # Install standard packages with Python script
    if [ ${#standard_packages[@]} -gt 0 ]; then
        if ! python3 "$DIR/check-packages.py" "install" "centos-9" "${standard_packages[@]}"; then
            echo "Failed to install standard packages"
            exit 1
        fi
    fi

    # Install custom packages with bash logic
    for pkg in "${custom_packages[@]}"; do
        case "$pkg" in
            custom-maven3.9.3)
                install_custom_maven "3.9.3"
                ;;
            custom-golang1.18.9)
                install_custom_golang "1.18.9"
                ;;
            custom-rust)
                install_rust "1.80"
                ;;
            custom-node)
                install_node "20"
                ;;
            libipt)
                if ! dnf list installed libipt >/dev/null 2>/dev/null; then
                    dnf install -y http://repo.okay.com.mx/centos/8/x86_64/release/libipt-1.6.1-8.el8.x86_64.rpm
                fi
                ;;
            libipt-devel)
                if ! dnf list installed libipt-devel >/dev/null 2>/dev/null; then
                    dnf install -y http://repo.okay.com.mx/centos/8/x86_64/release/libipt-devel-1.6.1-8.el8.x86_64.rpm
                fi
                ;;
            # Since there is no support for libipt-devel for CentOS 9 we install
            # Fedoras version of same libs, they are the same version but released
            # for different OS
            # TODO Update when libipt-devel releases for CentOS 9
            # SBCL packages are now available via EPEL repository
            PyYAML)
                if [ -z ${SUDO_USER+x} ]; then # Running as root (e.g. Docker).
                    pip3 install --user PyYAML
                else # Running using sudo.
                    sudo -H -u "$SUDO_USER" bash -c "pip3 install --user PyYAML"
                fi
                ;;
            python3-virtualenv)
                if [ -z ${SUDO_USER+x} ]; then # Running as root (e.g. Docker).
                    pip3 install virtualenv
                    pip3 install virtualenvwrapper
                else # Running using sudo.
                    sudo -H -u "$SUDO_USER" bash -c "pip3 install virtualenv"
                    sudo -H -u "$SUDO_USER" bash -c "pip3 install virtualenvwrapper"
                fi
                ;;
        esac
    done
}

deps=$2"[*]"
"$1" "$2"
