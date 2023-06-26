#!/bin/bash

function operating_system() {
    grep -E '^(VERSION_)?ID=' /etc/os-release | \
    sort | cut -d '=' -f 2- | sed 's/"//g' | paste -s -d '-'
}

function check_operating_system() {
    if [ "$(operating_system)" != "$1" ]; then
        echo "Not the right operating system!"
        exit 1
    else
        echo "The right operating system."
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
  if [ ! -f "$MVNINSTALLDIR/bin/mvn" ]; then
    curl -LO "https://dlcdn.apache.org/maven/maven-3/$MVNVERSION/binaries/apache-maven-$MVNVERSION-bin.tar.gz"
    tar -C "/opt" -xzf "apache-maven-$MVNVERSION-bin.tar.gz"
  fi
  echo "maven $MVNVERSION installed under $MVNINSTALLDIR"
}
