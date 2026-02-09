#!/bin/bash
set -eo pipefail

config_only=false
BUILD_TYPE="Release"
RUST_VERSION="1.85"
CUGRAPH=false

# Process flags first
while [[ $# -gt 0 ]]; do
  case "$1" in
    --rust-version)
      RUST_VERSION=$2
      shift 2
    ;;
    --build-type)
      BUILD_TYPE=$2
      shift 2
    ;;
    --config-only)
      config_only=true
      shift 1
    ;;
    --cugraph)
      CUGRAPH=true
      shift 1
    ;;
    *)
      echo "Unknown option: $1"
      exit 1
    ;;
  esac
done

# Extract positional arguments after flags have been processed
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
if [[ "$CUGRAPH" = true ]]; then
    build_args+=("--gpu" "--cpp-build-flags" "MAGE_CUGRAPH_ROOT=/opt/conda")
fi
python3 setup "${build_args[@]}"
