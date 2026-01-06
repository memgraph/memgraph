#!/bin/bash
set -eo pipefail

BUILD_TYPE="${1:-Release}"
RUST_VERSION="${2:-1.85}"

config_only=false
shift 2
while [[ $# -gt 0 ]]; do
  case "$1" in
    --config-only)
      config_only=true
      shift 1
    ;;
  esac
done

build_args=(
    "build"
    "-p"
    "$HOME/query_modules/"
    "--cpp-build-flags"
    "CMAKE_BUILD_TYPE=${BUILD_TYPE}"
)

if [[ "$config_only" = true ]]; then
    build_args+=("--config-only")
fi

source /opt/toolchain-v7/activate
curl https://sh.rustup.rs -sSf | sh -s -- -y
export PATH="$HOME/.cargo/bin:${PATH}"
rustup toolchain install $RUST_VERSION
rustup default $RUST_VERSION
mkdir -p $HOME/query_modules
python3 setup "${build_args[@]}"
