#!/bin/bash
set -Eeuo pipefail

# Set noninteractive frontend to avoid prompts during package installation
export DEBIAN_FRONTEND=noninteractive
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source "$DIR/../util.sh"

check_operating_system "ubuntu-24.04"
check_architecture "x86_64"

TOOLCHAIN_BUILD_DEPS=(
    coreutils gcc g++ build-essential make # generic build tools
    wget # used for archive download
    gnupg # used for archive signature verification
    tar gzip bzip2 xz-utils unzip # used for archive unpacking
    zlib1g-dev # zlib library used for all builds
    libexpat1-dev libipt-dev libbabeltrace-dev liblzma-dev python3-dev texinfo # for gdb
    libcurl4-openssl-dev # for cmake
    libreadline-dev # for cmake and llvm
    libffi-dev libxml2-dev # for llvm
    curl # snappy
    file
    git # for thrift
    libgmp-dev # for gdb
    gperf # for proxygen
    libssl-dev
    libedit-dev libpcre2-dev libpcre3-dev automake bison # for swig
    custom-rust
    libtool # for protobuf
    libssl-dev pkg-config # for pulsar
    libsasl2-dev # for librdkafka
    gdb lcov libbz2-dev libgdbm-dev libgdbm-compat-dev libncurses-dev # for building python
    libreadline-dev libsqlite3-dev lzma lzma-dev tk-dev uuid-dev # for building python
)

TOOLCHAIN_RUN_DEPS=(
    make # generic build tools
    tar gzip bzip2 xz-utils # used for archive unpacking
    zlib1g # zlib library used for all builds
    libexpat1 libipt2 libbabeltrace1 liblzma5 python3 # for gdb
    libcurl4t64 # for cmake
    libreadline8t64 # for cmake and llvm
    libffi8 libxml2 # for llvm
    libssl-dev # for libevent
)

MEMGRAPH_BUILD_DEPS=(
    git # source code control
    make cmake pkg-config # build system
    curl wget # for downloading libs
    uuid-dev default-jre-headless # required by antlr
    libreadline-dev # for memgraph console
    libpython3-dev python3-dev # for query modules
    libssl-dev
    libseccomp-dev
    netcat-traditional # tests are using nc to wait for memgraph
    python3 python3-virtualenv python3-pip # for qa, macro_benchmark and stress tests
    python3-yaml # for the configuration generator
    libcurl4-openssl-dev # mg-requests
    sbcl # for custom Lisp C++ preprocessing
    doxygen graphviz # source documentation generators
    mono-runtime mono-mcs zip unzip default-jdk-headless openjdk-17-jdk-headless custom-maven # for driver tests
    dotnet-sdk-8.0 golang custom-golang nodejs npm # for driver tests
    autoconf # for jemalloc code generation
    libtool  # for protobuf code generation
    libsasl2-dev
    ninja-build
    libopenblas-dev # required for building scipy
)

MEMGRAPH_TEST_DEPS="${MEMGRAPH_BUILD_DEPS[*]}"

MEMGRAPH_RUN_DEPS=(
    logrotate openssl python3 libseccomp2 libatomic1 adduser
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
            custom-*|dotnet-sdk-8.0)
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
        missing=$(python3 "$DIR/check-packages.py" "check" "ubuntu-24.04" "${standard_packages[@]}")
    fi

    # Check custom packages with bash logic
    for pkg in "${custom_packages[@]}"; do
        missing_pkg=$(check_custom_package "$pkg" || true)
        exit_code=$?
        if [ $exit_code -eq 0 ]; then
            # Custom package was handled, check if it's missing
            if [ -n "$missing_pkg" ]; then
                missing_custom="$missing_pkg $missing_custom"
            fi
        else
            # Not a custom package, check with case statement
            case "$pkg" in
                dotnet-sdk-8.0)
                    if ! dpkg -s dotnet-sdk-8.0 &>/dev/null; then
                        missing_custom="$pkg $missing_custom"
                    fi
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
    local -n packages=$1

    # Update package lists first
    apt update -y

    # check if python3 is installed
    if ! command -v python3 &>/dev/null; then
        apt install -y python3 python3-pip python3-venv
    fi

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
            custom-*|dotnet-sdk-8.0)
                custom_packages+=("$pkg")
                ;;
            *)
                standard_packages+=("$pkg")
                ;;
        esac
    done

    # Install standard packages with Python script
    if [ ${#standard_packages[@]} -gt 0 ]; then
        if ! python3 "$DIR/check-packages.py" "install" "ubuntu-24.04" "${standard_packages[@]}"; then
            echo "Failed to install standard packages"
            exit 1
        fi
    fi

    # Install custom packages with bash logic
    install_custom_packages "${custom_packages[@]}"

    # Handle non-custom packages that need special installation
    for pkg in "${custom_packages[@]}"; do
        case "$pkg" in
            dotnet-sdk-8.0)
                if ! dpkg -s dotnet-sdk-8.0 &>/dev/null; then
                    wget -nv https://packages.microsoft.com/config/ubuntu/24.04/packages-microsoft-prod.deb -O packages-microsoft-prod.deb
                    dpkg -i packages-microsoft-prod.deb
                    apt-get update
                    apt-get install -y apt-transport-https dotnet-sdk-8.0
                fi
                ;;
            *)
                # Skip packages that don't need special handling
                ;;
        esac
    done

    # Handle special cases that need post-installation setup
    if dpkg -s openjdk-17-jdk-headless &>/dev/null; then
        update-alternatives --set java /usr/lib/jvm/java-17-openjdk-amd64/bin/java
        update-alternatives --set javac /usr/lib/jvm/java-17-openjdk-amd64/bin/javac
    fi
}

"$1" "$2"
