#!/bin/bash

operating_system() {
    grep -E '^(VERSION_)?ID=' /etc/os-release | \
    sort | cut -d '=' -f 2- | sed 's/"//g' | paste -s -d '-'
}

check_operating_system() {
    if [ "$(operating_system)" != "$1" ]; then
        echo "Not the right operating system!"
        exit 1
    else
        echo "The right operating system."
    fi
}

architecture() {
    uname -m
}

check_architecture() {
    if [ "$(architecture)" != "$1" ]; then
        echo "Not the right architecture!"
        exit 1
    else
        echo "The right architecture."
    fi
}

check_all_yum() {
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

check_all_dpkg() {
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

check_all_dnf() {
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
install_all_apt() {
    for pkg in $1; do
        apt install -y "$pkg"
    done
}
