#!/bin/bash
set -euo pipefail
# run me as `memgraph` user

CI=false
CACHE_PRESENT=false
CUDA=false
CUDA_VERSION=13.0
ARCH=amd64
WHEEL_CACHE_DIR="$(pwd)/wheels"
DEB_PACKAGE=false
while [[ $# -gt 0 ]]; do
  case $1 in
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
    --cuda-version)
      CUDA_VERSION=$2
      shift 2
      ;;
    --arch)
      ARCH="$2"
      shift 2
      ;;
    --wheel-cache-dir)
      WHEEL_CACHE_DIR=$2
      shift 2
      ;;
    --deb-package)
      DEB_PACKAGE=true
      shift 1
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

if [[ "$CUDA" == true && "$ARCH" != "amd64" ]]; then
  echo "CUDA is only supported on amd64 architecture."
  exit 1
fi

export PIP_BREAK_SYSTEM_PACKAGES=1
if [[ "$CUDA" == true && "$DEB_PACKAGE" == "false" ]]; then
  requirements_file="requirements-gpu.txt"
else
  requirements_file="requirements.txt"
fi

if [ "$CI" = true ]; then
  # take torch from the wheel house if the cache is present
  if [ "$CACHE_PRESENT" = "true" ]; then
    echo "Installing torch from wheel cache"
    python3 -m pip install --no-cache-dir --no-index --find-links=$WHEEL_CACHE_DIR torch
  fi

  # for building the docker image
  python3 -m pip install --no-cache-dir -r "/tmp/${requirements_file}"
  python3 -m pip install --no-cache-dir -r /tmp/auth_module-requirements.txt
elif [[ "$DEB_PACKAGE" == "true" ]]; then
  # for installing python deps during deb package installation
  python3 -m pip install --no-cache-dir -r "/usr/lib/memgraph/mage-requirements.txt"
  python3 -m pip install --no-cache-dir -r "/usr/lib/memgraph/auth_module/requirements.txt"
else
  # for installing locally, from within the memgraph repo, under the mage directory
  python3 -m pip install --no-cache-dir -r "$(pwd)/python/${requirements_file}"
  python3 -m pip install --no-cache-dir -r "$(pwd)/../src/auth/reference_modules/requirements.txt"
fi

# custom package links TODO(matt): use official binaries when available
BASE_URL="https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/wheels"
if [[ "$ARCH" == "arm64" ]]; then
  BASE_URL="${BASE_URL}/arm64"
  TORCH_CLUSTER="$BASE_URL/torch_cluster-1.6.3-cp312-cp312-linux_aarch64.whl"
  TORCH_GEOMETRIC="$BASE_URL/torch_geometric-2.8.0-py3-none-any.whl"
  TORCH_SCATTER="$BASE_URL/torch_scatter-2.1.2-cp312-cp312-linux_aarch64.whl"
  TORCH_SPARSE="$BASE_URL/torch_sparse-0.6.18-cp312-cp312-linux_aarch64.whl"
  TORCH_SPLINE_CONV="$BASE_URL/torch_spline_conv-1.2.2-cp312-cp312-linux_aarch64.whl"
  DGL="$BASE_URL/dgl-2.5-cp312-cp312-linux_aarch64.whl"
else
  if [[ "$CUDA" == true ]]; then
    BASE_URL="${BASE_URL}/cuda-${CUDA_VERSION}"
  else
    BASE_URL="${BASE_URL}/amd64"
  fi
  TORCH_CLUSTER="$BASE_URL/torch_cluster-1.6.3-cp312-cp312-linux_x86_64.whl"
  TORCH_GEOMETRIC="$BASE_URL/torch_geometric-2.8.0-py3-none-any.whl"
  TORCH_SCATTER="$BASE_URL/torch_scatter-2.1.2-cp312-cp312-linux_x86_64.whl"
  TORCH_SPARSE="$BASE_URL/torch_sparse-0.6.18-cp312-cp312-linux_x86_64.whl"
  TORCH_SPLINE_CONV="$BASE_URL/torch_spline_conv-1.2.2-cp312-cp312-linux_x86_64.whl"
  DGL="$BASE_URL/dgl-2.5-cp312-cp312-linux_x86_64.whl"
fi

if [ "$ARCH" = "arm64" ]; then
  if [ "$CACHE_PRESENT" = "true" ]; then
    echo "Using cached torch packages"
    python3 -m pip install --no-index --find-links=$WHEEL_CACHE_DIR torch-sparse torch-cluster torch-spline-conv torch-geometric torch-scatter dgl
  else
    # Attempt to install from S3 first, if that fails, fallback to pulling from PyG and building from source, if necessary
    {
      python3 -m pip install --no-cache-dir $TORCH_SPARSE $TORCH_CLUSTER $TORCH_SPLINE_CONV $TORCH_GEOMETRIC $TORCH_SCATTER
    } || {
      python3 -m pip install --no-cache-dir torch-sparse torch-cluster torch-spline-conv torch-geometric torch-scatter -f https://data.pyg.org/whl/torch-2.9.0+cpu.html
    }
    curl -o dgl-2.5-cp312-cp312-linux_aarch64.whl https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/wheels/arm64/dgl-2.5-cp312-cp312-linux_aarch64.whl
    python3 -m pip install --no-cache-dir $DGL
    rm dgl-2.5-cp312-cp312-linux_aarch64.whl
  fi
else
  if [ "$CACHE_PRESENT" = "true" ]; then
      echo "Using cached torch packages"
      python3 -m pip install --no-index --find-links=$WHEEL_CACHE_DIR torch-sparse torch-cluster torch-spline-conv torch-geometric torch-scatter dgl
  else
    # Attempt to install from S3 first, if that fails, fallback to pulling from PyG and building from source, if necessary
    {
      python3 -m pip install --no-cache-dir $TORCH_SPARSE $TORCH_CLUSTER $TORCH_SPLINE_CONV $TORCH_GEOMETRIC $TORCH_SCATTER
    } || {
      if [[ "$CUDA" == true ]]; then
        python3 -m pip install --no-cache-dir torch-sparse torch-cluster torch-spline-conv torch-geometric torch-scatter -f https://data.pyg.org/whl/torch-2.9.0+cu130.html
      else
        python3 -m pip install --no-cache-dir torch-sparse torch-cluster torch-spline-conv torch-geometric torch-scatter -f https://data.pyg.org/whl/torch-2.9.0+cpu.html
      fi
    }
    curl -o dgl-2.5-cp312-cp312-linux_x86_64.whl https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/wheels/amd64/dgl-2.5-cp312-cp312-linux_x86_64.whl
    python3 -m pip install --no-cache-dir $DGL
    rm dgl-2.5-cp312-cp312-linux_x86_64.whl
  fi
fi
rm -fr /home/memgraph/.cache/pip
