#!/bin/bash
set -Eeuo pipefail
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source "$DIR/../util.sh"

# Parse command line arguments for --skip-check flag
SKIP_CHECK=$(parse_skip_check_flag "$@")

# Only run checks if --skip-check flag is not provided
if [[ "$SKIP_CHECK" == false ]]; then
    check_operating_system "centos-9"
    check_architecture "x86_64"
else
    echo "Skipping checks for centos-9"
fi

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
    python3-pip # for conan
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
    which nodejs golang custom-golang # for driver tests
    zip unzip java-11-openjdk-devel java-17-openjdk java-17-openjdk-devel custom-maven # for driver tests
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
    local -n packages="$1"
    printf '%s\n' "${packages[@]}"
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
            custom-*|PyYAML|python3-virtualenv|sbcl|libipt|libipt-devel)
                custom_packages+=("$pkg")
                ;;
            *)
                standard_packages+=("$pkg")
                ;;
        esac
    done

    # check if python3 is installed
    if ! command -v python3 &>/dev/null; then
        echo "python3 is not installed"
        exit 1
    fi

    # Check standard packages with Python script
    if [ ${#standard_packages[@]} -gt 0 ]; then
        missing=$(python3 "$DIR/check-packages.py" "check" "centos-9" "${standard_packages[@]}")
    fi

    # Check custom packages with bash logic
    for pkg in "${custom_packages[@]}"; do
        missing_pkg=$(check_custom_package "$pkg" || true)
        if [ $? -eq 0 ]; then
            if [ -n "$missing_pkg" ]; then
                missing_custom="$missing_pkg $missing_custom"
            fi
        else
            case "$pkg" in
                PyYAML)
                    if ! python3 -c "import yaml" >/dev/null 2>/dev/null; then
                        missing_custom="$pkg $missing_custom"
                    fi
                    ;;
                python3-virtualenv)
                    # Skip this as it's handled during installation
                    ;;
            esac
        fi
    done

    # Combine missing packages
    [ -n "$missing_custom" ] && missing="${missing:+$missing }$missing_custom"

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
    dnf install -y wget git python3 python3-pip
    dnf config-manager --set-enabled crb

    # Enable EPEL for additional packages
    dnf install -y epel-release epel-next-release

    # enable rpm fusion
    dnf install --nogpgcheck -y https://mirrors.rpmfusion.org/free/el/rpmfusion-free-release-9.noarch.rpm

    # Separate standard and custom packages
    local standard_packages=()
    local custom_packages=()

    for pkg in "${packages[@]}"; do
        case "$pkg" in
            custom-*|PyYAML|python3-virtualenv|libipt|libipt-devel|sbcl)
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
    install_custom_packages "${custom_packages[@]}"

    # Handle non-custom packages that need special installation
    for pkg in "${custom_packages[@]}"; do
        case "$pkg" in
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
            sbcl)
                if ! dnf list installed cl-asdf >/dev/null 2>/dev/null; then
                    dnf install -y http://www.nosuchhost.net/~cheese/fedora/packages/epel-9/x86_64/cl-asdf-20101028-27.el9.noarch.rpm
                fi
                if ! dnf list installed common-lisp-controller >/dev/null 2>/dev/null; then
                    dnf install -y http://www.nosuchhost.net/~cheese/fedora/packages/epel-9/x86_64/common-lisp-controller-7.4-29.el9.noarch.rpm
                fi
                if ! dnf list installed sbcl >/dev/null 2>/dev/null; then
                    dnf install -y http://www.nosuchhost.net/~cheese/fedora/packages/epel-9/x86_64/sbcl-2.3.11-3.el9~bootstrap.x86_64.rpm
                fi
                ;;
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
            *)
                # Skip packages that don't need special handling
                ;;
        esac
    done
}

"$1" "$2"
