#!/bin/bash

function operating_system() {
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        local detected_os=$(grep -E '^(VERSION_)?ID=' /etc/os-release | \
        sort | cut -d '=' -f 2- | sed 's/"//g' | paste -s -d '-')
        
        # Map popular Linux distributions to supported OSes
        case "$detected_os" in
            # Ubuntu mappings
            "ubuntu-22.04"|"ubuntu-22.10"|"ubuntu-23.04"|"ubuntu-23.10")
                echo "ubuntu-22.04"
                ;;
            "ubuntu-24.04"|"ubuntu-24.10"|"ubuntu-25.04")
                echo "ubuntu-24.04"
                ;;
            
            # Linux Mint mappings (based on Ubuntu)
            "linuxmint-20"|"linuxmint-20.1"|"linuxmint-20.2"|"linuxmint-20.3")
                echo "ubuntu-22.04"
                ;;
            "linuxmint-21"|"linuxmint-21.1"|"linuxmint-21.2"|"linuxmint-21.3")
                echo "ubuntu-22.04"
                ;;
            "linuxmint-22"|"linuxmint-22.1")
                echo "ubuntu-24.04"
                ;;
            
            # Debian mappings (same version only)
            "debian-11")
                echo "debian-11"
                ;;
            "debian-12")
                echo "debian-12"
                ;;
            
            # CentOS mappings (same version only)
            "centos-9")
                echo "centos-9"
                ;;
            "centos-10")
                echo "centos-10"
                ;;
            
            # Rocky Linux mappings (same version only)
            "rocky-9"|"rocky-9.0"|"rocky-9.1"|"rocky-9.2"|"rocky-9.3"|"rocky-9.4"|"rocky-9.5"|"rocky-9.6")
                echo "rocky-9"
                ;;
            "rocky-10"|"rocky-10.0")
                echo "rocky-10"
                ;;
            
            # RHEL mappings (compatible with CentOS)
            "rhel-9")
                echo "centos-9"
                ;;
            "rhel-10")
                echo "centos-10"
                ;;
            
            # AlmaLinux mappings (compatible with CentOS)
            "almalinux-9")
                echo "centos-9"
                ;;
            "almalinux-10")
                echo "centos-10"
                ;;
            
            # Amazon Linux mappings
            "amzn-2")
                echo "centos-9"
                ;;
            
            # Fedora mappings (same version only)
            "fedora-41")
                echo "fedora-41"
                ;;
            "fedora-42")
                echo "fedora-42"
                ;;
            
            # Default: return the detected OS as-is
            *)
                echo "$detected_os"
                ;;
        esac
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        echo "$(sw_vers -productName)-$(sw_vers -productVersion | cut -d '.' -f 1)"
    else
        echo "operating_system called on an unknown OS"
        exit 1
    fi
}

function check_operating_system() {
    # NOTE: We are actually checking for prefix because:
    #   * Rocky Linux on dnf update automatically updates minor version, at one
    #   point during install/checking version could be 9.3, while later if
    #   could be 9.5.
    if [[ "$(operating_system)" == "$1"* ]]; then
        echo "The right operating system."
    else
        echo "Not the right operating system!"
        exit 1
    fi
}

function architecture() {
    uname -m
}

check_architecture() {
    local ARCH=$(architecture)
    for arch in "$@"; do
        if [ "${ARCH}" = "$arch" ]; then
            echo "The right architecture!"
            return 0
        fi
    done
    echo "Not the right architecture!"
    echo "Expected: $@"
    echo "Actual: ${ARCH}"
    exit 1
}

function check_all_yum() {
    local missing=""
    for pkg in $1; do
        if ! yum list installed "$pkg" >/dev/null 2>/dev/null; then
            missing="$pkg $missing"
        fi
    done
    if [ "$missing" != "" ]; then
        echo "MISSING PACKAGES: $missing"
        exit 1
    fi
}

function check_all_dpkg() {
    local missing=""
    for pkg in $1; do
        if ! dpkg -s "$pkg" >/dev/null 2>/dev/null; then
            missing="$pkg $missing"
        fi
    done
    if [ "$missing" != "" ]; then
        echo "MISSING PACKAGES: $missing"
        exit 1
    fi
}

function check_all_dnf() {
    local missing=""
    for pkg in $1; do
        if ! dnf list installed "$pkg" >/dev/null 2>/dev/null; then
            missing="$pkg $missing"
        fi
    done
    if [ "$missing" != "" ]; then
        echo "MISSING PACKAGES: $missing"
        exit 1
    fi
}

function install_all_apt() {
    for pkg in $1; do
        apt install -y "$pkg"
    done
}

function install_custom_golang() {
    # NOTE: The official https://go.dev/doc/manage-install doesn't seem to be working.
    GOVERSION="$1"
    GOINSTALLDIR="/opt/go$GOVERSION"
    GOROOT="$GOINSTALLDIR/go" # GOPATH=$HOME/go
    if [ ! -f "$GOROOT/bin/go" ]; then
      curl -LO https://go.dev/dl/go$GOVERSION.linux-amd64.tar.gz
      mkdir -p "$GOINSTALLDIR"
      tar -C "$GOINSTALLDIR" -xzf go$GOVERSION.linux-amd64.tar.gz
    fi
    echo "go $GOVERSION installed under $GOROOT"
}

function install_custom_maven() {
  MVNVERSION="$1"
  MVNINSTALLDIR="/opt/apache-maven-$MVNVERSION"
  MVNURL="https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/maven/apache-maven-$MVNVERSION-bin.tar.gz"
  if [ ! -f "$MVNINSTALLDIR/bin/mvn" ]; then
    echo "Downloading maven from $MVNURL"
    curl -LO "$MVNURL"
    tar -C "/opt" -xzf "apache-maven-$MVNVERSION-bin.tar.gz"
  fi
  echo "maven $MVNVERSION installed under $MVNINSTALLDIR"
}

function install_dotnet_sdk ()
{
  DOTNETSDKVERSION="$1"
  DOTNETSDKINSTALLDIR="/opt/dotnet-sdk-$DOTNETSDKVERSION"
  if [ ! -d $DOTNETSDKINSTALLDIR ]; then
    mkdir -p $DOTNETSDKINSTALLDIR
  fi
  if [ ! -f "$DOTNETSDKINSTALLDIR/.dotnet/dotnet" ]; then
    wget https://dot.net/v1/dotnet-install.sh -O dotnet-install.sh
    chmod +x ./dotnet-install.sh
    ./dotnet-install.sh --channel 8.0 --install-dir $DOTNETSDKINSTALLDIR
    rm dotnet-install.sh
    ln -sf $DOTNETSDKINSTALLDIR/dotnet /usr/bin/dotnet
  fi
  echo "dotnet sdk $DOTNETSDKVERSION installed under $DOTNETSDKINSTALLDIR"
}

function install_rust () {
  RUST_VERSION="$1"
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y \
    && . "$HOME/.cargo/env" \
    && rustup default ${RUST_VERSION}
}

function install_node () {
  NODE_VERSION="$1"
  curl https://raw.githubusercontent.com/creationix/nvm/master/install.sh | bash \
      && . ~/.nvm/nvm.sh \
      && nvm install ${NODE_VERSION} \
      && nvm use ${NODE_VERSION}
}

function parse_operating_system() {
    local os_output=$(operating_system)
    local os_name=""
    local os_version=""
    
    # Split on the first dash to separate OS name from version
    if [[ "$os_output" == *"-"* ]]; then
        os_name=$(echo "$os_output" | cut -d'-' -f1)
        os_version=$(echo "$os_output" | cut -d'-' -f2-)
    else
        # If no dash found, treat the whole string as OS name
        os_name="$os_output"
        os_version=""
    fi
    
    # Export variables for use by calling script
    export OS="$os_name"
    export VER="$os_version"
    
    echo "OS: $OS"
    echo "VER: $VER"
}
