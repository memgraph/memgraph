#!/bin/bash
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# Source the util.sh file to get the parse_operating_system function
source "$SCRIPT_DIR/../util.sh"

SUPPORTED_OS=(
    all
    centos-9 centos-10
    debian-12 debian-12-arm debian-13 debian-13-arm
    fedora-42 fedora-42-arm
    rocky-10
    ubuntu-22.04 ubuntu-24.04 ubuntu-24.04-arm
)

# Define toolchain download URLs for supported OS and architectures
declare -A TOOLCHAIN_URLS=(
    [centos-9]="https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/toolchain-v7/toolchain-v7-binaries-centos-9-x86_64.tar.gz"
    [centos-10]="https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/toolchain-v7/toolchain-v7-binaries-centos-10-x86_64.tar.gz"
    [debian-12]="https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/toolchain-v7/toolchain-v7-binaries-debian-12-amd64.tar.gz"
    [debian-12-arm]="https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/toolchain-v7/toolchain-v7-binaries-debian-12-arm64.tar.gz"
    [debian-13]="https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/toolchain-v7/toolchain-v7-binaries-debian-13-amd64.tar.gz"
    [debian-13-arm]="https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/toolchain-v7/toolchain-v7-binaries-debian-13-arm64.tar.gz"
    [fedora-42]="https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/toolchain-v7/toolchain-v7-binaries-fedora-42-amd64.tar.gz"
    [fedora-42-arm]="https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/toolchain-v7/toolchain-v7-binaries-fedora-42-arm64.tar.gz"
    [rocky-10]="https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/toolchain-v7/toolchain-v7-binaries-rocky-10-amd64.tar.gz"
    [ubuntu-22.04]="https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/toolchain-v7/toolchain-v7-binaries-ubuntu-22.04-amd64.tar.gz"
    [ubuntu-24.04]="https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/toolchain-v7/toolchain-v7-binaries-ubuntu-24.04-amd64.tar.gz"
    [ubuntu-24.04-arm]="https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/toolchain-v7/toolchain-v7-binaries-ubuntu-24.04-arm64.tar.gz"
)

# Parse command line arguments to extract --set-os flag
SET_OS=""
ARGS=()

while [[ $# -gt 0 ]]; do
    case $1 in
        --set-os)
            SET_OS="$2"
            shift 2
            ;;
        -h|--help)
            cat <<EOF
Usage:
    ./install_deps.sh [command] [dependency_set] [--set-os <os>]

Options:
    --set-os <os>  Override OS detection and use specified OS.
                   Supported values: ${SUPPORTED_OS[*]}

Commands:
    prepare       Downloads and extracts specified dependencies.
                  Supported only for TOOLCHAIN_RUN_DEPS.
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
    sudo ./install_deps.sh install TOOLCHAIN_RUN_DEPS --set-os ubuntu-24.04
    sudo ./install_deps.sh --set-os ubuntu-24.04 install TOOLCHAIN_RUN_DEPS


Link to create new github issue: https://github.com/memgraph/memgraph/issues/new?title=install-deps.sh%20...&assignee=gitbuda&body=%0A%0A%0A---%0AI%27m+a+human.+Please+be+nice.
EOF
            exit 0
            ;;
        *)
            ARGS+=("$1")
            shift
            ;;
    esac
done

# Set the arguments back to $@ for the rest of the script
set -- "${ARGS[@]}"

# Function to check and install dependencies for a given distribution script
run_script() {
    local distro_script=$1
    echo "Running script for $@"
    $SCRIPT_DIR/"$@"
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
    tar xzvf /tmp/toolchain.tar.gz -C /opt && rm -f /tmp/toolchain.tar.gz || {
        echo "Failed to extract toolchain. Please check the archive and your permissions."
        exit 1
    }

    echo "Toolchain setup complete."
}

# Detect OS, version, and architecture
if [[ -n "$SET_OS" ]]; then
    # Parse OS and VER from SET_OS string (e.g., "ubuntu-24.04" -> OS="ubuntu", VER="24.04")
    # Handle formats like: ubuntu-24.04, ubuntu-24.04-arm, centos-9, debian-11-arm
    if [[ "$SET_OS" =~ ^([a-z]+)-([0-9.]+)$ ]]; then
        OS="${BASH_REMATCH[1]}"
        VER="${BASH_REMATCH[2]}"
    else
        echo "Error: Invalid OS format: $SET_OS. Expected format: os-version"
        exit 1
    fi
elif [ -f /etc/os-release ]; then
    . /etc/os-release
    parse_operating_system
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

if [[ "$1" == "prepare" ]]; then
    if [[ "$2" == "TOOLCHAIN_RUN_DEPS" ]]; then
        if [[ "$is_supported" == true ]]; then
            prepare_toolchain "$OS_ARCH"
        else
            echo "Unsupported OS: $OS_ARCH. The 'prepare' command cannot proceed."
            echo "Supported OS values: ${SUPPORTED_OS[*]}"
            exit 1
        fi
    else
        echo "Error: The 'prepare' command only supports 'TOOLCHAIN_RUN_DEPS' as the second argument."
        exit 1
    fi
else
    # If supported, run the script with all original arguments
    if [[ "$is_supported" == true ]]; then
        if [ -z "$SET_OS" ]; then
            run_script "$OS_ARCH_SCRIPT" "$@"
        else
            run_script "$OS_ARCH_SCRIPT" "$@" --skip-check
        fi
    else
        echo "Unsupported OS: $OS_ARCH"
        echo "Supported OS values: ${SUPPORTED_OS[*]}"
        exit 1
    fi
fi
