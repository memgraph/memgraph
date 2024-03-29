#!/bin/bash

SUPPORTED_OS=(
    all
    amzn-2
    centos-7 centos-9
    debian-10 debian-11 debian-11-arm debian-12 debian-12-arm
    fedora-36 fedora-38 fedora-39
    rocky-9.3
    ubuntu-18.04 ubuntu-20.04 ubuntu-22.04 ubuntu-22.04-arm
)

# Define toolchain download URLs for supported OS and architectures
declare -A TOOLCHAIN_URLS=(
    [centos-7]="https://s3-eu-west-1.amazonaws.com/deps.memgraph.io/toolchain-v5/toolchain-v5-binaries-centos-7-x86_64.tar.gz"
    [centos-8]="https://s3-eu-west-1.amazonaws.com/deps.memgraph.io/toolchain-v5/toolchain-v5-binaries-centos-8-x86_64.tar.gz"
    [centos-9]="https://s3-eu-west-1.amazonaws.com/deps.memgraph.io/toolchain-v5/toolchain-v5-binaries-centos-9-x86_64.tar.gz"
    [fedora-39]="https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/toolchain-v5/toolchain-v5-binaries-fedora-39-amd64.tar.gz"
    [debian-10]="https://s3-eu-west-1.amazonaws.com/deps.memgraph.io/toolchain-v4/toolchain-v4-binaries-debian-10-amd64.tar.gz" # Note v4 for Debian 10
    [debian-11]="https://s3-eu-west-1.amazonaws.com/deps.memgraph.io/toolchain-v5/toolchain-v5-binaries-debian-11-amd64.tar.gz"
    [debian-11-arm]="https://s3-eu-west-1.amazonaws.com/deps.memgraph.io/toolchain-v5/toolchain-v5-binaries-debian-11-arm64.tar.gz"
    [ubuntu-18.04]="https://s3-eu-west-1.amazonaws.com/deps.memgraph.io/toolchain-v4/toolchain-v4-binaries-ubuntu-18.04-amd64.tar.gz" # Note v4 for Ubuntu 18.04
    [ubuntu-20.04]="https://s3-eu-west-1.amazonaws.com/deps.memgraph.io/toolchain-v5/toolchain-v5-binaries-ubuntu-20.04-amd64.tar.gz"
    [ubuntu-22.04]="https://s3-eu-west-1.amazonaws.com/deps.memgraph.io/toolchain-v5/toolchain-v5-binaries-ubuntu-22.04-amd64.tar.gz"
    [ubuntu-22.04-arm]="https://s3-eu-west-1.amazonaws.com/deps.memgraph.io/toolchain-v5/toolchain-v5-binaries-ubuntu-22.04-arm64.tar.gz"
)

# Check for help argument before processing others
if [[ "$#" -eq 1 && ("$1" == "-h" || "$1" == "--help") ]]; then
    cat <<EOF
Usage:
    ./install_deps.sh [command] [dependency_set]

Commands:
    install       Install the specified dependencies.
    check         Check the specified dependencies.

Dependency sets:
    TOOLCHAIN_RUN_DEPS       Dependencies required for the toolchain runtime.
    MEMGRAPH_BUILD_DEPS      Dependencies required for building Memgraph.

Examples:
    sudo ./install_deps.sh prepare TOOLCHAIN_RUN_DEPS
    sudo ./install_deps.sh check TOOLCHAIN_RUN_DEPS
    sudo ./install_deps.sh install TOOLCHAIN_RUN_DEPS
    sudo ./install_deps.sh check MEMGRAPH_BUILD_DEPS
    sudo ./install_deps.sh install MEMGRAPH_BUILD_DEPS


Link to create new github issue: https://github.com/memgraph/memgraph/issues/new?title=install-deps.sh%20...&assignee=gitbuda&body=%0A%0A%0A---%0AI%27m+a+human.+Please+be+nice.
EOF
    exit 0
fi

# Function to check and install dependencies for a given distribution script
run_script() {
    local distro_script=$1
    echo "Running script for $@"
    ./environment/os/"$@"
}

# New function for 'prepare' command to download and extract the toolchain
prepare_toolchain() {
    local os_arch="$1"
    local toolchain_url="${TOOLCHAIN_URLS[$os_arch]}"

    if [ -z "$toolchain_url" ]; then
        echo "No toolchain URL found for $os_arch. Please ensure your OS and architecture are supported."
        exit 1
    fi

    echo "Setting up toolchain for $os_arch..."
    curl -L "$toolchain_url" --output /tmp/toolchain.tar.gz || {
        echo "Failed to download toolchain. Please check your internet connection or the URL and try again."
        exit 1
    }

    echo "Extracting toolchain to /opt..."
    tar xzvf /tmp/toolchain.tar.gz -C /opt && rm /tmp/toolchain.tar.gz || {
        echo "Failed to extract toolchain. Please check the archive and your permissions."
        exit 1
    }

    echo "Toolchain setup complete."
}

# Detect OS, version, and architecture
if [ -f /etc/os-release ]; then
    . /etc/os-release
    OS=$ID
    VER=$VERSION_ID
elif type lsb_release >/dev/null 2>&1; then
    OS=$(lsb_release -si)
    VER=$(lsb_release -sr)
elif [ -f /etc/lsb-release ]; then
    . /etc/lsb-release
    OS=$DISTRIB_ID
    VER=$DISTRIB_RELEASE
elif [ -f /etc/debian_version ]; then
    OS=Debian
    VER=$(cat /etc/debian_version)
else
    echo "OS not identified"
    exit 1
fi

ARCH=$(uname -m)
case $ARCH in
    arm*|aarch64)
        ARCH="arm"
        ;;
    *)
        ARCH=""
        ;;
esac

# Normalize OS name to lowercase
OS=$(echo "$OS" | tr '[:upper:]' '[:lower:]')

if [[ -z "$ARCH" ]]; then
    OS_ARCH="${OS}-${VER}"
else
    OS_ARCH="${OS}-${VER}-${ARCH}"
fi

OS_ARCH_SCRIPT="${OS_ARCH}.sh"

# Check if OS_ARCH is in the SUPPORTED_OS array
is_supported=false
for supported_os in "${SUPPORTED_OS[@]}"; do
    if [[ "$OS_ARCH" == "$supported_os" ]]; then
        is_supported=true
        break
    fi
done

if [[ "$1" == "prepare" && "$2" == "TOOLCHAIN_RUN_DEPS" ]]; then
    if [[ "$is_supported" == true ]]; then
        prepare_toolchain "$OS_ARCH"
    else
        echo "Unsupported OS: $OS_ARCH. The 'setup' command cannot proceed."
        exit 1
    fi
else
    # If supported, run the script with all original arguments
    if [[ "$is_supported" == true ]]; then
        run_script "$OS_ARCH_SCRIPT" "$@"
    else
        echo "Unsupported OS: $OS_ARCH"
        exit 1
    fi
fi
