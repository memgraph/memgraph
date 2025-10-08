#!/bin/bash
set -Eeuo pipefail
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source "$DIR/../util.sh"

# Parse command line arguments for --skip-check flag
SKIP_CHECK=$(parse_skip_check_flag "$@")

# Only run checks if --skip-check flag is not provided
if [[ "$SKIP_CHECK" == false ]]; then
    check_operating_system "fedora-41"
    check_architecture "x86_64"
else
    echo "Skipping checks for fedora-41"
fi

TOOLCHAIN_BUILD_DEPS=(
    coreutils-common gcc gcc-c++ make # generic build tools
    wget2-wget # used for archive download
    gnupg2 # used for archive signature verification
    tar gzip bzip2 xz unzip # used for archive unpacking
    # NOTE: https://discussion.fedoraproject.org/t/f40-change-proposal-transitioning-to-zlib-ng-as-a-compatible-replacement-for-zlib-system-wide/95807
    zlib-ng-compat-devel # zlib library used for all builds
    expat-devel xz-devel python3-devel texinfo libbabeltrace-devel # for gdb
    curl libcurl-devel # for cmake
    readline-devel # for cmake and llvm
    libffi-devel libxml2-devel # for llvm
    libedit-devel pcre-devel pcre2-devel automake bison # for swig
    file
    openssl openssl-devel openssl-devel-engine # for pulsar
    gmp-devel
    gperf
    diffutils
    libipt libipt-devel # intel
    patch
    perl # for openssl
    git
    custom-rust # for mgcxx
    libtool # for protobuf
    pkgconf-pkg-config # for pulsar
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
)

MEMGRAPH_BUILD_DEPS=(
    git # source code control
    make pkgconf-pkg-config # build system
    wget2-wget # for downloading libs
    libuuid-devel java-11-openjdk # required by antlr
    readline-devel # for memgraph console
    python3-devel # for query modules
    openssl-devel openssl
    libseccomp-devel
    python3 python3-pip python3-virtualenv python3-virtualenvwrapper python3-pyyaml nmap-ncat # for tests
    libcurl-devel # mg-requests
    rpm-build rpmlint # for RPM package building
    doxygen graphviz # source documentation generators
    which nodejs golang custom-golang zip unzip java-11-openjdk-devel custom-maven custom-node # for driver tests
    sbcl # for custom Lisp C++ preprocessing
    autoconf # for jemalloc code generation
    libtool  # for protobuf code generation
    cyrus-sasl-devel
    ninja-build
    openblas-devel
)

MEMGRAPH_TEST_DEPS="${MEMGRAPH_BUILD_DEPS[*]}"

MEMGRAPH_RUN_DEPS=(
    logrotate openssl python3 libseccomp
)

NEW_DEPS=(
    wget2-wget curl tar gzip
)

list() {
    local -n packages="$1"
    printf '%s\n' "${packages[@]}"
}

check() {
    if [ -v LD_LIBRARY_PATH ]; then
      # On Fedora 38 yum/dnf and python11 use newer glibc which is not compatible
      # with ours, so we need to momentarely disable env
      local OLD_LD_LIBRARY_PATH=${LD_LIBRARY_PATH}
      LD_LIBRARY_PATH=""
    fi

    local -n packages=$1
    local missing=""
    local missing_custom=""

    # Separate standard and custom packages
    local standard_packages=()
    local custom_packages=()

    for pkg in "${packages[@]}"; do
        case "$pkg" in
            custom-*)
                custom_packages+=("$pkg")
                ;;
            *)
                standard_packages+=("$pkg")
                ;;
        esac
    done

    # Check standard packages with Python script
    if [ ${#standard_packages[@]} -gt 0 ]; then
        missing=$(python3 "$DIR/check-packages.py" "check" "fedora-41" "${standard_packages[@]}")
    fi

    # Check custom packages with bash logic
    for pkg in "${custom_packages[@]}"; do
        missing_pkg=$(check_custom_package "$pkg" || true)
        if [ $? -eq 0 ]; then
            if [ -n "$missing_pkg" ]; then
                missing_custom="$missing_pkg $missing_custom"
            fi
        fi
    done

    # Combine missing packages
    [ -n "$missing_custom" ] && missing="${missing:+$missing }$missing_custom"

    if [ -n "$missing" ]; then
        echo "MISSING PACKAGES: $missing"
        exit 1
    fi

    if [ -v OLD_LD_LIBRARY_PATH ]; then
      echo "Restoring LD_LIBRARY_PATH..."
      LD_LIBRARY_PATH=${OLD_LD_LIBRARY_PATH}
    fi
}

install() {
    if [ "$EUID" -ne 0 ]; then
        echo "Please run as root."
        exit 1
    fi

    local -n packages=$1

    # If GitHub Actions runner is installed, append LANG to the environment.
    # Python related tests don't work without the LANG export.
    if [ -d "/home/gh/actions-runner" ]; then
        echo "LANG=en_US.utf8" >> /home/gh/actions-runner/.env
    else
        echo "NOTE: export LANG=en_US.utf8"
    fi

    # Separate standard and custom packages
    local standard_packages=()
    local custom_packages=()

    for pkg in "${packages[@]}"; do
        case "$pkg" in
            custom-*)
                custom_packages+=("$pkg")
                ;;
            *)
                standard_packages+=("$pkg")
                ;;
        esac
    done

    # Install standard packages with Python script
    if [ ${#standard_packages[@]} -gt 0 ]; then
        if ! python3 "$DIR/check-packages.py" "install" "fedora-41" "${standard_packages[@]}"; then
            echo "Failed to install standard packages"
            exit 1
        fi
    fi

    # Install custom packages with bash logic
    install_custom_packages "${custom_packages[@]}"
}

"$1" "$2"
