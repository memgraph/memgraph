#!/bin/bash
set -eo pipefail

config_only=false
BUILD_TYPE="Release"
RUST_VERSION="1.85"
CUGRAPH=false
SPLIT_DEBUG=false

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
    --split-debug)
      SPLIT_DEBUG=true
      shift 1
    ;;
    *)
      echo "Unknown option: $1"
      exit 1
    ;;
  esac
done

# Build the cpp-build-flags list in ONE go. mage/setup parses
# --cpp-build-flags with argparse nargs="+", so passing the option more
# than once silently keeps only the last occurrence (i.e. earlier flags
# would be dropped) — collect every cmake -D<flag>=<value> here first,
# then emit a single --cpp-build-flags entry below.
cpp_build_flags=("CMAKE_BUILD_TYPE=${BUILD_TYPE}")
if [[ "$SPLIT_DEBUG" = true ]]; then
    cpp_build_flags+=("MG_SPLIT_DEBUG=ON")
fi
if [[ "$CUGRAPH" = true ]]; then
    cpp_build_flags+=("MAGE_CUGRAPH_ROOT=/opt/conda")
fi

build_args=(
    "build"
    "-p"
    "$HOME/query_modules/"
    "--cpp-build-flags"
    "${cpp_build_flags[@]}"
)
if [[ "$config_only" = true ]]; then
    build_args+=("--config-only")
fi
if [[ "$CUGRAPH" = true ]]; then
    # --gpu is a flag on mage/setup itself, not part of --cpp-build-flags,
    # so it's safe to append after the cpp-build-flags list.
    build_args+=("--gpu")
fi

source /opt/toolchain-v7/activate
curl https://sh.rustup.rs -sSf | sh -s -- -y
export PATH="$HOME/.cargo/bin:${PATH}"
rustup toolchain install $RUST_VERSION
rustup default $RUST_VERSION
mkdir -p $HOME/query_modules
python3 setup "${build_args[@]}"
