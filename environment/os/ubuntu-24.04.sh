#!/bin/bash
set -Eeuo pipefail
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
    mono-runtime mono-mcs zip unzip default-jdk-headless openjdk-17-jdk-headless custom-maven3.9.3 # for driver tests
    dotnet-sdk-8.0 golang custom-golang1.18.9 nodejs npm # for driver tests
    autoconf # for jemalloc code generation
    libtool  # for protobuf code generation
    libsasl2-dev
    ninja-build
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
    local missing=""
    local missing_custom=""
    declare -n packages=$1

    # check custom packages first
    for pkg in "${packages[@]}"; do
        if [ "$pkg" == custom-maven3.9.3 ]; then
            if [ ! -f "/opt/apache-maven-3.9.3/bin/mvn" ]; then
                missing_custom="$pkg $missing"
            fi
            continue
        fi
        if [ "$pkg" == custom-golang1.18.9 ]; then
            if [ ! -f "/opt/go1.18.9/go/bin/go" ]; then
                missing_custom="$pkg $missing"
            fi
            continue
        fi
        if [ "$pkg" == custom-rust ]; then
            if [ ! -x "$HOME/.cargo/bin/rustup" ]; then
                missing_custom="$pkg $missing"
            fi
            continue
        fi
    done

    # pop custom items off the package list
    filtered=()
    for pkg in "${packages[@]}"; do
        case "$pkg" in
            custom-maven3.9.3|custom-golang1.18.9|custom-rust|custom-node)
                continue
                ;;
            *) 
                filtered+=( "$pkg" )
                ;;
        esac
    done
    packages=("${filtered[@]}")

    # call python script to check the rest
    missing=$(python3 "$DIR/check-packages.py" "ubuntu-24.04" "${packages[@]}")

    # combine with custom packages
    if [ -n "$missing_custom" ]; then
        missing="$missing $missing_custom"
    fi

    if [ -n "$missing" ]; then
        echo "MISSING PACKAGES: $missing"
        exit 1
    fi
}

install() {
    cd "$DIR"
    apt update -y
    apt install -y wget
    # If GitHub Actions runner is installed, append LANG to the environment.
    # Python related tests doesn't work the LANG export.
    if [ -d "/home/gh/actions-runner" ]; then
        echo "LANG=en_US.utf8" >> /home/gh/actions-runner/.env
    else
        echo "NOTE: export LANG=en_US.utf8"
    fi
    # Create an array for packages to be installed via apt later.
    local apt_packages=()
    # Create an array from all arguments passed (the package list)
    local -n packages="$1"

    # Iterate through each package in the provided list
    for pkg in "${packages[@]}"; do
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
            dotnet-sdk-8.0)
                if ! dpkg -s dotnet-sdk-8.0 &>/dev/null; then
                    wget -nv https://packages.microsoft.com/config/ubuntu/24.04/packages-microsoft-prod.deb -O packages-microsoft-prod.deb
                    dpkg -i packages-microsoft-prod.deb
                    apt-get update
                    apt-get install -y apt-transport-https dotnet-sdk-8.0
                fi
                ;;
            openjdk-17-jdk-headless)
                # We delay installation so we can update alternatives after
                if ! dpkg -s "$pkg" &>/dev/null; then
                    apt_packages+=("$pkg")
                fi
                ;;
            *)
                # For a generic package, add it to the list
                apt_packages+=("$pkg")
                ;;
        esac
    done
    
    # Now use your python script to check which generic packages are missing.
    # It should output a space-separated list.
    missing=$(python3 "$DIR/check-packages.py" "ubuntu-24.04" "${apt_packages[@]}")

    # If there are missing packages, install them all in one apt install call.
    if [ -n "$missing" ]; then
        echo "Installing missing packages: $missing"
        apt install -y $missing
    fi

    # For openjdk-17-jdk-headless, update alternatives if it is installed.
    if dpkg -s openjdk-17-jdk-headless &>/dev/null; then
        update-alternatives --set java /usr/lib/jvm/java-17-openjdk-amd64/bin/java
        update-alternatives --set javac /usr/lib/jvm/java-17-openjdk-amd64/bin/javac
    fi
}

deps=$2"[*]"
"$1" "$2"
