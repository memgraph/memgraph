#!/bin/bash

function operating_system() {
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        grep -E '^(VERSION_)?ID=' /etc/os-release | \
        sort | cut -d '=' -f 2- | sed 's/"//g' | paste -s -d '-'
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

function check_custom_package() {
    local pkg="$1"
    
    case "$pkg" in
        custom-maven*)
            if [ ! -f "/opt/apache-maven-3.9.3/bin/mvn" ]; then
                echo "$pkg"
            fi
            ;;
        custom-golang*)
            if [ ! -f "/opt/go1.18.9/go/bin/go" ]; then
                echo "$pkg"
            fi
            ;;
        custom-rust)
            if [ ! -x "$HOME/.cargo/bin/rustup" ]; then
                echo "$pkg"
            fi
            ;;
        *)
            return 1
            ;;
    esac
    return 0
}

function install_custom_packages() {
    local packages=("$@")
    
    for pkg in "${packages[@]}"; do
        case "$pkg" in
            custom-maven*)
                install_custom_maven "3.9.3"
                ;;
            custom-golang*)
                install_custom_golang "1.18.9"
                ;;
            custom-rust)
                install_rust "1.80"
                ;;
            custom-node)
                install_node "20"
                ;;
        esac
    done
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
