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
USE_UV=false
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
    --uv)
      USE_UV=true
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
# uv wants an unambiguous interpreter: given a bare name it treats --python as a
# request and can prefer a uv-managed install over the one on PATH.
PYTHON_BIN="$(command -v "$PYTHON")"

export PIP_BREAK_SYSTEM_PACKAGES=1
export PIP_DEFAULT_TIMEOUT=120
export PIP_RETRIES=8

UV_TARGET_ARGS=()
if [[ "$USE_UV" == "true" ]]; then
  if ! command -v uv >/dev/null 2>&1; then
    echo "--uv requested but uv is not on PATH" >&2
    exit 1
  fi
  # MAGE's deps have to be global: memgraph loads the modules from the
  # interpreter it embeds, so there is no virtualenv for uv to install into.
  export UV_SYSTEM_PYTHON=1
  export UV_BREAK_SYSTEM_PACKAGES=1
  # Never reuse cached packages (stale-cache issues in CI containers) and never
  # download a managed Python.
  export UV_NO_CACHE=1
  export UV_PYTHON_DOWNLOADS=never
  # pip asks every index and takes the best match; uv stops at the first index
  # that carries a package. requirements.txt adds download.pytorch.org as an
  # extra index and that index also mirrors a handful of PyPI packages
  # (filelock, networkx, setuptools, ...) at versions our pins forbid, so uv
  # needs pip's behaviour to resolve them.
  export UV_INDEX_STRATEGY=unsafe-best-match
  export UV_HTTP_TIMEOUT="$PIP_DEFAULT_TIMEOUT"

  # pip quietly falls back to a user install when the global site-packages isn't
  # writable, which is how MAGE's deps have always landed when this runs as an
  # unprivileged user (mg in CI, memgraph from the deb postinst). uv rejects
  # --user outright and fails on the permission error instead, so make the same
  # choice explicitly. Run as root, the global site stays the target.
  system_site="$("$PYTHON_BIN" -c 'import sysconfig; print(sysconfig.get_path("purelib"))')"
  probe="$system_site"
  while [[ ! -e "$probe" && "$probe" != "/" ]]; do
    probe="$(dirname "$probe")"
  done
  if [[ ! -w "$probe" ]]; then
    user_site="$("$PYTHON_BIN" -m site --user-site)"
    echo "$system_site is not writable; installing into the user site $user_site"
    mkdir -p "$user_site"
    UV_TARGET_ARGS=(--target "$user_site")
  fi
fi

# Installs into the interpreter memgraph embeds - its global site-packages, or
# its user site when UV_TARGET_ARGS says that isn't writable - with pip or uv.
pip_install () {
  if [[ "$USE_UV" != "true" ]]; then
    $PYTHON -m pip install "$@"
    return
  fi
  local args=()
  local arg
  for arg in "$@"; do
    # uv spells this --no-cache, and UV_NO_CACHE above already covers it.
    [[ "$arg" == "--no-cache-dir" ]] && continue
    args+=("$arg")
  done
  uv pip install --python "$PYTHON_BIN" ${UV_TARGET_ARGS[@]+"${UV_TARGET_ARGS[@]}"} "${args[@]}"
}

# A committed lockfile next to a requirements file pins the whole dependency
# tree (see compile_requirements_locks.sh), so uv installs from it and skips
# resolution entirely. pip keeps using the loose pins: image and package builds
# stay exactly as they were.
requirements_source () {
  local requirements="$1"
  local lock="${requirements%.txt}.lock"
  if [[ "$USE_UV" == "true" && -f "$lock" ]]; then
    echo "Installing from lockfile $lock" >&2
    echo "$lock"
  else
    echo "$requirements"
  fi
}

if [[ "$CUDA" == true && "$DEB_PACKAGE" == "false" ]]; then
  requirements_file="requirements-gpu.txt"
else
  requirements_file="requirements.txt"
fi

if [ "$CI" = true ]; then
  # take torch from the wheel house if the cache is present
  if [ "$CACHE_PRESENT" = "true" ]; then
    echo "Installing torch from wheel cache"
    pip_install --no-cache-dir --no-index --find-links=$WHEEL_CACHE_DIR torch
  fi

  # for building the docker image
  pip_install --no-cache-dir -r "$(requirements_source "/tmp/${requirements_file}")"
  pip_install --no-cache-dir -r "$(requirements_source "/tmp/auth_module-requirements.txt")"
elif [[ "$DEB_PACKAGE" == "true" ]]; then
  # for installing python deps during deb package installation
  pip_install --no-cache-dir -r "$(requirements_source "/usr/lib/memgraph/mage-requirements.txt")"
  pip_install --no-cache-dir -r "$(requirements_source "/usr/lib/memgraph/auth_module/requirements.txt")"
else
  # for installing locally, from within the memgraph repo, under the mage directory
  pip_install --no-cache-dir -r "$(requirements_source "$(pwd)/python/${requirements_file}")"
  pip_install --no-cache-dir -r "$(requirements_source "$(pwd)/../src/auth/reference_modules/requirements.txt")"
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
#
# Wheels are portable manylinux_2_34 builds (load on both Ubuntu and CentOS
# Stream 9) hosted under a dated prefix wheels/<date>/<arch>/; bump WHEELS_DATE
# when a new set is published. cuda wheels live under cuda-<ver>/amd64.
S3_HOST="https://s3.eu-west-1.amazonaws.com/deps.memgraph.io"
WHEELS_DATE="2026-06-25"
if [[ "$ARCH" == "arm64" ]]; then
  BASE_URL="$S3_HOST/wheels/${WHEELS_DATE}/arm64"
  PLAT="manylinux_2_34_aarch64"
elif [[ "$CUDA" == true ]]; then
  BASE_URL="$S3_HOST/wheels/${WHEELS_DATE}/cuda-${CUDA_VERSION}/amd64"
  PLAT="manylinux_2_34_x86_64"
else
  BASE_URL="$S3_HOST/wheels/${WHEELS_DATE}/amd64"
  PLAT="manylinux_2_34_x86_64"
fi
TORCH_CLUSTER="$BASE_URL/torch_cluster-1.6.3-cp312-cp312-${PLAT}.whl"
TORCH_GEOMETRIC="$BASE_URL/torch_geometric-2.9.0-py3-none-any.whl"
TORCH_SCATTER="$BASE_URL/torch_scatter-2.1.2-cp312-cp312-${PLAT}.whl"
TORCH_SPARSE="$BASE_URL/torch_sparse-0.6.18-cp312-cp312-${PLAT}.whl"
TORCH_SPLINE_CONV="$BASE_URL/torch_spline_conv-1.2.2-cp312-cp312-${PLAT}.whl"
DGL="$BASE_URL/dgl-2.5-cp312-cp312-${PLAT}.whl"
PYG_LIB="$BASE_URL/pyg_lib-0.8.0-cp312-cp312-${PLAT}.whl"

if [ "$ARCH" = "arm64" ]; then
  if [ "$CACHE_PRESENT" = "true" ]; then
    echo "Using cached torch packages"
    pip_install --no-index --find-links=$WHEEL_CACHE_DIR torch-sparse torch-cluster torch-spline-conv torch-geometric torch-scatter dgl
  else
    # Attempt to install from S3 first, if that fails, fallback to pulling from PyG and building from source, if necessary
    {
      pip_install --no-cache-dir $TORCH_SPARSE $TORCH_CLUSTER $TORCH_SPLINE_CONV $TORCH_GEOMETRIC $TORCH_SCATTER $PYG_LIB
    } || {
      pip_install --no-cache-dir torch-sparse torch-cluster torch-spline-conv torch-geometric torch-scatter -f https://data.pyg.org/whl/torch-2.9.0+cpu.html
    }
    pip_install --no-cache-dir "$DGL"
  fi
else
  if [ "$CACHE_PRESENT" = "true" ]; then
      echo "Using cached torch packages"
      pip_install --no-index --find-links=$WHEEL_CACHE_DIR torch-sparse torch-cluster torch-spline-conv torch-geometric torch-scatter dgl
  else
    # Attempt to install from S3 first, if that fails, fallback to pulling from PyG and building from source, if necessary
    {
      pip_install --no-cache-dir $TORCH_SPARSE $TORCH_CLUSTER $TORCH_SPLINE_CONV $TORCH_GEOMETRIC $TORCH_SCATTER $PYG_LIB
    } || {
      if [[ "$CUDA" == true ]]; then
        pip_install --no-cache-dir torch-sparse torch-cluster torch-spline-conv torch-geometric torch-scatter -f https://data.pyg.org/whl/torch-2.9.0+cu130.html
      else
        pip_install --no-cache-dir torch-sparse torch-cluster torch-spline-conv torch-geometric torch-scatter -f https://data.pyg.org/whl/torch-2.9.0+cpu.html
      fi
    }
    pip_install --no-cache-dir "$DGL"
  fi
fi
rm -fr /home/memgraph/.cache/pip
