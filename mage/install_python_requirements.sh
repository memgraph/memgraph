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
# Pull the custom torch/PyG/DGL wheels from the S3 "wheels-staging" prefix
# rather than the usual "wheels" prefix. Defaults to true so the RPM %post
# (which calls this script without extra flags) uses the staging wheels too.
# Set --staging false to go back to the promoted "wheels" prefix.
STAGING=true
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
    --staging)
      STAGING=$2
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

# MAGE's pinned GNN wheels (torch/PyG/DGL below) are all cp312, and memgraph
# embeds python 3.12 to match. Most distros already default `python3` to 3.12,
# but some don't (e.g. CentOS Stream 9 ships 3.9 as python3 and memgraph is
# built against an explicitly-installed python3.12). Prefer python3.12 when
# present so the deps land in the interpreter memgraph actually loads; otherwise
# fall back to python3. Override with PYTHON=<interpreter> if needed.
PYTHON="${PYTHON:-}"
if [[ -z "$PYTHON" ]]; then
  if command -v python3.12 >/dev/null 2>&1; then
    PYTHON=python3.12
  else
    PYTHON=python3
  fi
fi
echo "Installing MAGE python requirements with: $PYTHON ($($PYTHON --version 2>&1))"

export PIP_BREAK_SYSTEM_PACKAGES=1
export PIP_DEFAULT_TIMEOUT=120
export PIP_RETRIES=8
if [[ "$CUDA" == true && "$DEB_PACKAGE" == "false" ]]; then
  requirements_file="requirements-gpu.txt"
else
  requirements_file="requirements.txt"
fi

if [ "$CI" = true ]; then
  # take torch from the wheel house if the cache is present
  if [ "$CACHE_PRESENT" = "true" ]; then
    echo "Installing torch from wheel cache"
    $PYTHON -m pip install --no-cache-dir --no-index --find-links=$WHEEL_CACHE_DIR torch
  fi

  # for building the docker image
  $PYTHON -m pip install --no-cache-dir -r "/tmp/${requirements_file}"
  $PYTHON -m pip install --no-cache-dir -r /tmp/auth_module-requirements.txt
elif [[ "$DEB_PACKAGE" == "true" ]]; then
  # for installing python deps during deb package installation
  $PYTHON -m pip install --no-cache-dir -r "/usr/lib/memgraph/mage-requirements.txt"
  $PYTHON -m pip install --no-cache-dir -r "/usr/lib/memgraph/auth_module/requirements.txt"
else
  # for installing locally, from within the memgraph repo, under the mage directory
  $PYTHON -m pip install --no-cache-dir -r "$(pwd)/python/${requirements_file}"
  $PYTHON -m pip install --no-cache-dir -r "$(pwd)/../src/auth/reference_modules/requirements.txt"
fi

# custom package links TODO(matt): use official binaries when available
#
# IMPORTANT: the PyG/DGL versions baked into the URLs below must stay in sync
# with the *_VERSION variables in
# tools/ci/mage-build/offline-installer/download-wheels.sh. These wheels are
# custom-built and hosted in our S3 bucket (not on PyPI), so both scripts have
# to agree on which version to fetch — there's no upstream registry to derive
# a single answer from. Any version bump here must be mirrored there and vice
# versa.
S3_HOST="https://s3.eu-west-1.amazonaws.com/deps.memgraph.io"
# pyg_lib only exists in the staging wheelhouse; stays empty for the promoted
# "wheels" prefix so the install commands below simply omit it.
PYG_LIB=""
if [[ "$ARCH" == "arm64" ]]; then
  # wheels-staging is amd64-only — there are no arm64 staging wheels — so arm64
  # always uses the promoted "wheels" prefix and its linux_aarch64 filenames.
  if [[ "$STAGING" == true ]]; then
    echo "Note: wheels-staging has no arm64 build; using the 'wheels' prefix for arm64."
  fi
  BASE_URL="$S3_HOST/wheels/arm64"
  TORCH_CLUSTER="$BASE_URL/torch_cluster-1.6.3-cp312-cp312-linux_aarch64.whl"
  TORCH_GEOMETRIC="$BASE_URL/torch_geometric-2.8.0-py3-none-any.whl"
  TORCH_SCATTER="$BASE_URL/torch_scatter-2.1.2-cp312-cp312-linux_aarch64.whl"
  TORCH_SPARSE="$BASE_URL/torch_sparse-0.6.18-cp312-cp312-linux_aarch64.whl"
  TORCH_SPLINE_CONV="$BASE_URL/torch_spline_conv-1.2.2-cp312-cp312-linux_aarch64.whl"
  DGL="$BASE_URL/dgl-2.5-cp312-cp312-linux_aarch64.whl"
elif [[ "$STAGING" == true ]]; then
  # amd64 staging wheelhouse: portable manylinux_2_34 wheels that load on both
  # Ubuntu and CentOS Stream 9. Filenames/versions differ from the promoted
  # prefix (torch_geometric bumped to 2.9.0, pyg_lib added). cuda wheels live
  # under cuda-<ver>/amd64.
  if [[ "$CUDA" == true ]]; then
    BASE_URL="$S3_HOST/wheels-staging/cuda-${CUDA_VERSION}/amd64"
  else
    BASE_URL="$S3_HOST/wheels-staging/amd64"
  fi
  TORCH_CLUSTER="$BASE_URL/torch_cluster-1.6.3-cp312-cp312-manylinux_2_34_x86_64.whl"
  TORCH_GEOMETRIC="$BASE_URL/torch_geometric-2.9.0-py3-none-any.whl"
  TORCH_SCATTER="$BASE_URL/torch_scatter-2.1.2-cp312-cp312-manylinux_2_34_x86_64.whl"
  TORCH_SPARSE="$BASE_URL/torch_sparse-0.6.18-cp312-cp312-manylinux_2_34_x86_64.whl"
  TORCH_SPLINE_CONV="$BASE_URL/torch_spline_conv-1.2.2-cp312-cp312-manylinux_2_34_x86_64.whl"
  DGL="$BASE_URL/dgl-2.5-cp312-cp312-manylinux_2_34_x86_64.whl"
  PYG_LIB="$BASE_URL/pyg_lib-0.8.0-cp312-cp312-manylinux_2_34_x86_64.whl"
else
  # amd64 promoted "wheels" prefix (original behaviour, linux_x86_64 filenames).
  if [[ "$CUDA" == true ]]; then
    BASE_URL="$S3_HOST/wheels/cuda-${CUDA_VERSION}"
  else
    BASE_URL="$S3_HOST/wheels/amd64"
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
    $PYTHON -m pip install --no-index --find-links=$WHEEL_CACHE_DIR torch-sparse torch-cluster torch-spline-conv torch-geometric torch-scatter dgl
  else
    # Attempt to install from S3 first, if that fails, fallback to pulling from PyG and building from source, if necessary
    {
      $PYTHON -m pip install --no-cache-dir $TORCH_SPARSE $TORCH_CLUSTER $TORCH_SPLINE_CONV $TORCH_GEOMETRIC $TORCH_SCATTER
    } || {
      $PYTHON -m pip install --no-cache-dir torch-sparse torch-cluster torch-spline-conv torch-geometric torch-scatter -f https://data.pyg.org/whl/torch-2.9.0+cpu.html
    }
    $PYTHON -m pip install --no-cache-dir "$DGL"
  fi
else
  if [ "$CACHE_PRESENT" = "true" ]; then
      echo "Using cached torch packages"
      $PYTHON -m pip install --no-index --find-links=$WHEEL_CACHE_DIR torch-sparse torch-cluster torch-spline-conv torch-geometric torch-scatter dgl
  else
    # Attempt to install from S3 first, if that fails, fallback to pulling from PyG and building from source, if necessary
    {
      # $PYG_LIB is set only for the staging wheelhouse; empty (omitted) otherwise.
      $PYTHON -m pip install --no-cache-dir $TORCH_SPARSE $TORCH_CLUSTER $TORCH_SPLINE_CONV $TORCH_GEOMETRIC $TORCH_SCATTER $PYG_LIB
    } || {
      if [[ "$CUDA" == true ]]; then
        $PYTHON -m pip install --no-cache-dir torch-sparse torch-cluster torch-spline-conv torch-geometric torch-scatter -f https://data.pyg.org/whl/torch-2.9.0+cu130.html
      else
        $PYTHON -m pip install --no-cache-dir torch-sparse torch-cluster torch-spline-conv torch-geometric torch-scatter -f https://data.pyg.org/whl/torch-2.9.0+cpu.html
      fi
    }
    $PYTHON -m pip install --no-cache-dir "$DGL"
  fi
fi
rm -fr /home/memgraph/.cache/pip
