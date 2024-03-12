#!/bin/bash

# Initialize a variable to track if the --build-deps or --toolchain-deps flag is passed
build_deps_flag=false
toolchain_deps_flag=false

# Display help if no arguments are passed
if [ "$#" -eq 0 ]; then
    cat <<EOF
Please specify one or both of these config flags:
    --build-deps        for installing Memgraph build dependencies
    --toolchain-deps    for installing toolchain runtime dependencies

If you run into issues with this script, please run corresponding commands manually and open a github issue:
    sudo ./environment/os/debian-10.sh install TOOLCHAIN_RUN_DEPS
    sudo ./environment/os/debian-10.sh install MEMGRAPH_BUILD_DEPS
    
Link to create new github issue: https://github.com/memgraph/memgraph/issues/new?title=install-deps.sh%20...&assignee=gitbuda&body=%0A%0A%0A---%0AI%27m+a+human.+Please+be+nice.
EOF
    exit 0
fi

# Parse command-line arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --build-deps) build_deps_flag=true ;;
        --toolchain-deps) toolchain_deps_flag=true ;;
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
    shift
done

# Function to check and install dependencies for a given distribution script
run_script() {
    local distro_script=$1
    echo "Running script for $distro_script"
    if [ "$toolchain_deps_flag" = true ]; then
        sudo ./environment/os/$distro_script check TOOLCHAIN_RUN_DEPS
        sudo ./environment/os/$distro_script install TOOLCHAIN_RUN_DEPS
    fi
    if [ "$build_deps_flag" = true ]; then
        sudo ./environment/os/$distro_script install MEMGRAPH_BUILD_DEPS
    fi
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

# Map OS, version, and architecture to script name
case "$OS" in
    debian)
        case "$VER" in
            10) run_script "debian-10.sh" ;;
            11) 
                if [ "$ARCH" = "arm" ]; then
                    run_script "debian-11-arm.sh"
                else
                    run_script "debian-11.sh"
                fi
                ;;
            *) echo "Unsupported Debian version: $VER"; exit 1 ;;
        esac
        ;;
    ubuntu)
        case "$VER" in
            18.04) run_script "ubuntu-18.04.sh" ;;
            20.04) run_script "ubuntu-20.04.sh" ;;
            22.04) 
                if [ "$ARCH" = "arm" ]; then
                    run_script "ubuntu-22.04-arm.sh"
                else
                    run_script "ubuntu-22.04.sh"
                fi
                ;;
            *) echo "Unsupported Ubuntu version: $VER"; exit 1 ;;
        esac
        ;;
    centos)
        case "$VER" in
            7) run_script "centos-7.sh" ;;
            9) run_script "centos-9.sh" ;;
            *) echo "Unsupported CentOS version: $VER"; exit 1 ;;
        esac
        ;;
    fedora)
        case "$VER" in
            36) run_script "fedora-36.sh" ;;
            38) run_script "fedora-38.sh" ;;
            *) echo "Unsupported Fedora version: $VER"; exit 1 ;;
        esac
        ;;
    "amzn")
        case "$VER" in
            2) run_script "amzn-2.sh" ;;
            *) echo "Unsupported Amazon Linux version: $VER"; exit 1 ;;
        esac
        ;;
    *)
        echo "Unsupported OS: $OS"
        exit 1
        ;;
esac
