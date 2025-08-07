#!/bin/bash
set -Eeuo pipefail

# Set noninteractive frontend to avoid prompts during package installation
export DEBIAN_FRONTEND=noninteractive
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source "$DIR/../util.sh"

# IMPORTANT: Deprecated since memgraph v2.12.0.

check_operating_system "debian-11"
check_architecture "arm64" "aarch64"

TOOLCHAIN_BUILD_DEPS=(
    coreutils gcc g++ build-essential make # generic build tools
    wget # used for archive download
    gnupg # used for archive signature verification
    tar gzip bzip2 xz-utils unzip # used for archive unpacking
    zlib1g-dev # zlib library used for all builds
    libexpat1-dev liblzma-dev python3-dev texinfo # for gdb
    libcurl4-openssl-dev # for cmake
    libreadline-dev # for cmake and llvm
    libffi-dev libxml2-dev # for llvm
    libedit-dev libpcre2-dev libpcre3-dev automake bison # for swig
    curl # snappy
    file # for libunwind
    libssl-dev # for libevent
    libgmp-dev
    gperf # for proxygen
    git # for fbthrift
    custom-rust
    libtool # for protobuf
    pkg-config # for pulsar
    libsasl2-dev # for librdkafka
)

TOOLCHAIN_RUN_DEPS=(
    make # generic build tools
    tar gzip bzip2 xz-utils # used for archive unpacking
    zlib1g # zlib library used for all builds
    libexpat1 liblzma5 python3 # for gdb
    libcurl4 # for cmake
    file # for CPack
    libreadline8 # for cmake and llvm
    libffi7 libxml2 # for llvm
    libssl-dev # for libevent
)

MEMGRAPH_BUILD_DEPS=(
    git # source code control
    make pkg-config # build system
    curl wget # for downloading libs
    uuid-dev default-jre-headless # required by antlr
    libreadline-dev # for memgraph console
    libpython3-dev python3-dev # for query modules
    libssl-dev
    libseccomp-dev
    netcat # tests are using nc to wait for memgraph
    python3 virtualenv python3-virtualenv python3-pip # for qa, macro_benchmark and stress tests
    python3-yaml # for the configuration generator
    libcurl4-openssl-dev # mg-requests
    sbcl # for custom Lisp C++ preprocessing
    doxygen graphviz # source documentation generators
    mono-runtime mono-mcs zip unzip default-jdk-headless openjdk-17-jdk custom-maven # for driver tests
    golang custom-golang nodejs npm
    autoconf # for jemalloc code generation
    libtool  # for protobuf code generation
    libsasl2-dev
    ninja-build
)

MEMGRAPH_RUN_DEPS=(
    logrotate openssl python3 libseccomp2
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
        missing=$(python3 "$DIR/check-packages.py" "check" "debian-11" "${standard_packages[@]}")
    fi

    # Check custom packages with bash logic
    for pkg in "${custom_packages[@]}"; do
        missing_pkg=$(check_custom_package "$pkg")
        if [ $? -eq 0 ]; then
            # Custom package was handled, check if it's missing
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
}

install() {
    local -n packages=$1

    # Set up sources.list for Debian 11
    cat >/etc/apt/sources.list <<EOF
deb http://deb.debian.org/debian bullseye main
deb-src http://deb.debian.org/debian bullseye main

deb http://deb.debian.org/debian-security/ bullseye-security main
deb-src http://deb.debian.org/debian-security/ bullseye-security main

deb http://deb.debian.org/debian bullseye-updates main
deb-src http://deb.debian.org/debian bullseye-updates main
EOF

    # Update package lists first
    apt update -y

    # If GitHub Actions runner is installed, append LANG to the environment.
    # Python related tests doesn't work the LANG export.
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
        if ! python3 "$DIR/check-packages.py" "install" "debian-11" "${standard_packages[@]}"; then
            echo "Failed to install standard packages"
            exit 1
        fi
    fi

    # Install custom packages with bash logic
    install_custom_packages "${custom_packages[@]}"

    # Handle special cases that need post-installation setup
    if dpkg -s openjdk-17-jdk &>/dev/null; then
        # The default Java version should be Java 11 for ARM
        update-alternatives --set java /usr/lib/jvm/java-11-openjdk-arm64/bin/java
        update-alternatives --set javac /usr/lib/jvm/java-11-openjdk-arm64/bin/javac
    fi
}

deps=$2"[*]"
"$1" "$2"
