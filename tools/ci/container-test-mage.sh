#!/bin/bash
set -euo pipefail

# runs Python, Rust and C++ tests for MAGE inside build container

# Color codes
RED_BOLD='\033[1;31m'
GREEN_BOLD='\033[1;32m'
RESET='\033[0m'

CONTAINER_NAME=mgbuild
CI=false
CACHE_PRESENT=false
CUDA=false
ARCH=amd
# Keep in sync with DEFAULT_RUST_VERSION in release/package/mgbuild.sh; the
# images' default rustc is too old for bindgen 0.71's generated bindings.
RUST_VERSION=1.89
while [[ $# -gt 0 ]]; do
  case $1 in
    --container-name)
      CONTAINER_NAME=$2
      shift 2
    ;;
    --ci)
      CI=true
      shift
      ;;
    --cache-present)
      CACHE_PRESENT=$2
      shift 2
      ;;
    --cuda)
      CUDA=$2
      shift 2
      ;;
    --arch)
      ARCH="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
    ;;
  esac
done

exit_handler() {
  local exit_code=${1:-$?}
  if [[ $exit_code -ne 0 ]]; then
    echo -e "${RED_BOLD}Failed to run tests${RESET}"
  fi
  exit $exit_code
}

trap exit_handler ERR EXIT

echo -e "${GREEN_BOLD}Running tests in container: $CONTAINER_NAME${RESET}"

echo -e "${GREEN_BOLD}Running Rust tests${RESET}"
docker exec -i -u mg $CONTAINER_NAME bash -c "source /opt/toolchain-v7/activate && source \$HOME/.cargo/env && rustup toolchain install $RUST_VERSION && export RUSTUP_TOOLCHAIN=$RUST_VERSION && cd \$HOME/memgraph/mage/rust/rsmgp-sys && cargo fmt -- --check && RUST_BACKTRACE=1 cargo test"


echo -e "${GREEN_BOLD}Running C++ tests${RESET}"
# MAGE unit tests are registered in the root build tree with the mage__
# prefix (the standalone mage/cpp/build no longer exists).
docker exec -i -u mg $CONTAINER_NAME bash -c "cd \$HOME/memgraph/build && ctest -R mage__ --output-on-failure -j\$(nproc)"


echo -e "${GREEN_BOLD}Running Python tests${RESET}"
if [[ "$CUDA" == true ]]; then
  requirements_file="requirements-gpu.txt"
else
  requirements_file="requirements.txt"
fi
docker cp mage/python/$requirements_file $CONTAINER_NAME:/tmp/$requirements_file
docker cp src/auth/reference_modules/requirements.txt $CONTAINER_NAME:/tmp/auth_module-requirements.txt
docker exec -i -u mg $CONTAINER_NAME bash -c "export PIP_DEFAULT_TIMEOUT=120 PIP_RETRIES=8 && cd \$HOME/memgraph/mage/ && \
  ./install_python_requirements.sh --ci --cache-present $CACHE_PRESENT --cuda $CUDA --arch $ARCH && \
  pip install -r \$HOME/memgraph/mage/python/tests/requirements.txt --break-system-packages"
docker exec -i -u mg $CONTAINER_NAME bash -c "cd \$HOME/memgraph/mage/python/ && python3 -m pytest ."
