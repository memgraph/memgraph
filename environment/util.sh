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
    for arch in "$@"; do
        if [ "$(architecture)" = "$arch" ]; then
            echo "The right architecture!"
            return 0
        fi
    done
    echo "Not the right architecture!"
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
