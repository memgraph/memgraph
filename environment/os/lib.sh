#!/bin/bash
# Shared machinery for the per-distro dependency scripts in this directory.
#
# A distro script is package data plus optional hooks:
#
#     DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
#     source "$DIR/lib.sh"
#     TOOLCHAIN_BUILD_DEPS=(...)
#     TOOLCHAIN_RUN_DEPS=(...)
#     MEMGRAPH_BUILD_DEPS=(...)
#     MEMGRAPH_RUN_DEPS=(...)
#     NEW_DEPS=(...)
#     main "$@"
#
# The filename is the OS name (fedora-44.sh, debian-13.sh, ...); one script
# serves every architecture — anything arch-specific belongs in a hook that
# inspects `uname -m`. MEMGRAPH_TEST_DEPS defaults to MEMGRAPH_BUILD_DEPS
# unless the script sets it explicitly.
#
# Optional hooks, defined between `source lib.sh` and `main "$@"`:
#   setup_repos()              enable extra repos before packages install
#   SPECIAL_PACKAGES=(...)     packages install() must not hand to the package
#                              manager; routed to install_special_package
#   install_special_package()  install one such package ($1)
#   post_install()             runs after all packages are installed
#
# Packages named custom-* are handled by util.sh's check_custom_package /
# install_custom_packages on every distro.

set -Eeuo pipefail

_LIB_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source "$_LIB_DIR/../util.sh"

if [[ -z "${BASH_SOURCE[1]:-}" ]]; then
    echo "lib.sh must be sourced by a distro script, not executed directly."
    exit 1
fi
OS="$(basename "${BASH_SOURCE[1]}" .sh)"

_package_manager() {
    case "$OS" in
        debian-*|ubuntu-*) echo apt ;;
        centos-*|fedora-*|rocky-*) echo dnf ;;
        *)
            echo "lib.sh: cannot derive package manager from OS '$OS'" >&2
            exit 1
            ;;
    esac
}

list() {
    local -n _packages="$1"
    printf '%s\n' "${_packages[@]}"
}

_is_special_package() {
    local pkg
    for pkg in ${SPECIAL_PACKAGES[@]+"${SPECIAL_PACKAGES[@]}"}; do
        [[ "$1" == "$pkg" ]] && return 0
    done
    return 1
}

check() {
    local -n _packages="$1"
    local missing="" missing_custom="" missing_pkg=""
    local standard_packages=() custom_packages=() pkg

    # custom-* packages are checked by install location; everything else --
    # special packages included -- by querying the package database.
    for pkg in "${_packages[@]}"; do
        if [[ "$pkg" == custom-* ]]; then
            custom_packages+=("$pkg")
        else
            standard_packages+=("$pkg")
        fi
    done

    if ! command -v python3 &>/dev/null; then
        echo "python3 is not installed"
        exit 1
    fi

    if [ ${#standard_packages[@]} -gt 0 ]; then
        missing=$(python3 "$_LIB_DIR/check-packages.py" "check" "$OS" "${standard_packages[@]}")
    fi

    for pkg in ${custom_packages[@]+"${custom_packages[@]}"}; do
        missing_pkg=$(check_custom_package "$pkg" || true)
        if [ -n "$missing_pkg" ]; then
            missing_custom="$missing_pkg $missing_custom"
        fi
    done

    [ -n "$missing_custom" ] && missing="${missing:+$missing }$missing_custom"

    if [ -n "$missing" ]; then
        echo "MISSING PACKAGES: $missing"
        exit 1
    fi
}

install() {
    if [ "$EUID" -ne 0 ]; then
        echo "Please run as root."
        exit 1
    fi

    local -n _packages="$1"

    # If GitHub Actions runner is installed, append LANG to the environment.
    # Python related tests don't work without the LANG export.
    if [ -d "/home/gh/actions-runner" ]; then
        echo "LANG=en_US.utf8" >> /home/gh/actions-runner/.env
    else
        echo "NOTE: export LANG=en_US.utf8"
    fi

    # Bootstrap the tools the machinery itself needs: python3 for
    # check-packages.py, wget/git for the custom-* installers.
    if [[ "$(_package_manager)" == "apt" ]]; then
        export DEBIAN_FRONTEND=noninteractive
        apt update -y
        apt install -y python3 wget git
    else
        dnf install -y wget git python3 python3-pip
    fi

    if declare -F setup_repos >/dev/null; then
        setup_repos
    fi

    local standard_packages=() custom_packages=() special_packages=() pkg
    for pkg in "${_packages[@]}"; do
        if [[ "$pkg" == custom-* ]]; then
            custom_packages+=("$pkg")
        elif _is_special_package "$pkg"; then
            special_packages+=("$pkg")
        else
            standard_packages+=("$pkg")
        fi
    done

    if [ ${#standard_packages[@]} -gt 0 ]; then
        if ! python3 "$_LIB_DIR/check-packages.py" "install" "$OS" "${standard_packages[@]}"; then
            echo "Failed to install standard packages"
            exit 1
        fi
    fi

    install_custom_packages ${custom_packages[@]+"${custom_packages[@]}"}

    for pkg in ${special_packages[@]+"${special_packages[@]}"}; do
        install_special_package "$pkg"
    done

    if declare -F post_install >/dev/null; then
        post_install
    fi
}

main() {
    local skip_check
    skip_check=$(parse_skip_check_flag "$@")
    if [[ "$skip_check" == "false" ]]; then
        check_operating_system "$OS"
        check_architecture "x86_64" "arm64" "aarch64"
    else
        echo "Skipping checks for $OS"
    fi

    if [[ -z "${MEMGRAPH_TEST_DEPS+x}" ]]; then
        MEMGRAPH_TEST_DEPS=("${MEMGRAPH_BUILD_DEPS[@]}")
    fi

    local cmd="${1:-}" group="${2:-}"
    case "$cmd" in
        list|check|install) ;;
        *)
            echo "Usage: $0 {list|check|install} <DEPS_GROUP> [--skip-check]"
            exit 1
            ;;
    esac
    if ! declare -p "$group" &>/dev/null; then
        echo "Unknown dependency group: '$group'"
        exit 1
    fi
    "$cmd" "$group"
}
