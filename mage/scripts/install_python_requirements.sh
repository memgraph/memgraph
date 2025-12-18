#!/bin/bash
set -euo pipefail
# run me as `memgraph` user

CI=false
CACHE_PRESENT=false
CUDA=false
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
    # for building the docker image
    python3 -m pip install --no-cache-dir -r "/tmp/${requirements_file}"
    python3 -m pip install --no-cache-dir -r /tmp/auth_module-requirements.txt
elif [[ "$DEB_PACKAGE" == "true" ]]; then
    # for installing python deps during deb package installation
    python3 -m pip install --no-cache-dir -r "/usr/lib/memgraph/mage-requirements.txt"
    python3 -m pip install --no-cache-dir -r "/usr/lib/memgraph/auth_module/requirements.txt"
else
    # for installing locally, from within the mage repo
    python3 -m pip install --no-cache-dir -r "$(pwd)/python/${requirements_file}"
    python3 -m pip install --no-cache-dir -r "$(pwd)/cpp/memgraph/src/auth/reference_modules/requirements.txt"
fi

if [ "$ARCH" = "arm64" ]; then
    if [ "$CACHE_PRESENT" = "true" ]; then
        echo "Using cached torch packages"
        python3 -m pip install --no-index --find-links=$WHEEL_CACHE_DIR torch-sparse torch-cluster torch-spline-conv torch-geometric torch-scatter
    else
        python3 -m pip install --no-cache-dir torch-sparse torch-cluster torch-spline-conv torch-geometric torch-scatter -f https://data.pyg.org/whl/torch-2.6.0+cpu.html
    fi
    curl -o dgl-2.5.0-cp312-cp312-linux_aarch64.whl https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/wheels/arm64/dgl-2.5.0-cp312-cp312-linux_aarch64.whl
    python3 -m pip install --no-cache-dir dgl-2.5.0-cp312-cp312-linux_aarch64.whl
    rm dgl-2.5.0-cp312-cp312-linux_aarch64.whl
else
    if [[ "$CUDA" == true ]]; then
      python3 -m pip install --no-cache-dir torch-sparse torch-cluster torch-spline-conv torch-geometric torch-scatter -f https://data.pyg.org/whl/torch-2.6.0+cu126.html
    elif [ "$CACHE_PRESENT" = "true" ]; then
        echo "Using cached torch packages"
        python3 -m pip install --no-index --find-links=$WHEEL_CACHE_DIR torch-sparse torch-cluster torch-spline-conv torch-geometric torch-scatter
    else
        python3 -m pip install --no-cache-dir torch-sparse torch-cluster torch-spline-conv torch-geometric torch-scatter -f https://data.pyg.org/whl/torch-2.6.0+cpu.html
    fi
    python3 -m pip install --no-cache-dir dgl==2.5.0 -f https://data.dgl.ai/wheels/torch-2.6/repo.html
fi
rm -fr /home/memgraph/.cache/pip
