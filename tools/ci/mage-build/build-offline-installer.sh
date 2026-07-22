#!/bin/bash
set -euo pipefail
# Build a self-contained .run installer for memgraph + MAGE on Ubuntu 24.04.
#
# The output is a single executable file containing:
#   - memgraph .deb + memgraph-mage .deb
#   - every apt dependency .deb needed on a vanilla Ubuntu 24.04 host
#   - every Python wheel needed by mage and the auth modules (CPU or CUDA
#     variant, depending on --cuda)
#   - install.sh (offline installer logic)
#
# Build steps:
#   1. Spin up a fresh ubuntu:24.04 container and run download-debs.sh to
#      harvest .debs into a host-side staging dir.
#   2. Spin up another fresh ubuntu:24.04 container and run download-wheels.sh
#      to populate the wheels/ staging dir from PyPI + our S3 bucket + any
#      pre-built wheels under mage/wheels (e.g. gssapi).
#   3. Drop the two input .deb files (memgraph + memgraph-mage) and the
#      install.sh script into the staging tree.
#   4. tar the staging tree and prepend a tiny self-extracting shell preamble.
#
# Variant flags (--build-type/--malloc/--cuda/--cugraph) feed into the output
# filename and, for --cuda/--cugraph, also select GPU-flavoured Python wheels
# so the cache matches what the memgraph-mage .deb's postinst expects.

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$SCRIPT_DIR/../../.."
OFFLINE_DIR="$SCRIPT_DIR/offline-installer"

MEMGRAPH_DEB=""
MAGE_DEB=""
ARCH="amd64"
BUILD_TYPE="Release"
CUDA="false"
CUDA_VERSION="13.0"
MALLOC="false"
CUGRAPH="false"
OUTPUT=""
WHEELS_PREBUILT_DIR="$PROJECT_ROOT/release/package/mage/wheels"

usage() {
  cat <<EOF
Usage: $0 --memgraph-deb PATH --mage-deb PATH [options]

Required:
  --memgraph-deb PATH       Path to memgraph_*.deb
  --mage-deb PATH           Path to memgraph-mage_*.deb

Options:
  --arch amd64|arm64        Target dpkg arch (default amd64)
  --build-type STR          Release|RelWithDebInfo (default Release)
  --cuda true|false         Bundle CUDA wheels (requires --arch amd64)
  --cuda-version X.Y        CUDA version for S3 wheel path (default 13.0)
  --malloc true|false       Variant flag — for output filename only
  --cugraph true|false      Variant flag — implies --cuda true
  --output PATH             Output .run file
                            (default: memgraph-mage-offline-<version>-<arch><suffix>.run)
  --wheels-dir PATH         Pre-built host wheels to bundle (default: \$PROJECT_ROOT/release/package/mage/wheels)
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --memgraph-deb) MEMGRAPH_DEB="$2"; shift 2 ;;
    --mage-deb)     MAGE_DEB="$2";     shift 2 ;;
    --arch)         ARCH="$2";         shift 2 ;;
    --build-type)   BUILD_TYPE="$2";   shift 2 ;;
    --cuda)         CUDA="$2";         shift 2 ;;
    --cuda-version) CUDA_VERSION="$2"; shift 2 ;;
    --malloc)       MALLOC="$2";       shift 2 ;;
    --cugraph)      CUGRAPH="$2";      shift 2 ;;
    --output)       OUTPUT="$2";       shift 2 ;;
    --wheels-dir)   WHEELS_PREBUILT_DIR="$2"; shift 2 ;;
    -h|--help)      usage; exit 0 ;;
    *)              echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

# cugraph implies cuda — mirrors release/package/mgbuild.sh::package_mage_deb.
if [[ "$CUGRAPH" == "true" ]]; then
  CUDA="true"
fi

if [[ -z "$MEMGRAPH_DEB" || -z "$MAGE_DEB" ]]; then
  echo "Error: --memgraph-deb and --mage-deb are required." >&2
  usage
  exit 1
fi
if [[ ! -f "$MEMGRAPH_DEB" ]]; then
  echo "Error: memgraph deb not found at $MEMGRAPH_DEB" >&2
  exit 1
fi
if [[ ! -f "$MAGE_DEB" ]]; then
  echo "Error: mage deb not found at $MAGE_DEB" >&2
  exit 1
fi
case "$ARCH" in
  amd64|arm64) ;;
  *) echo "Error: --arch must be amd64 or arm64 (got $ARCH)" >&2; exit 1 ;;
esac
case "$BUILD_TYPE" in
  Release|RelWithDebInfo) ;;
  *) echo "Error: --build-type must be Release or RelWithDebInfo (got $BUILD_TYPE)" >&2; exit 1 ;;
esac
if [[ "$CUDA" == "true" && "$ARCH" != "amd64" ]]; then
  echo "Error: --cuda true requires --arch amd64 (no CUDA wheels published for arm64)" >&2
  exit 1
fi

# Build the variant suffix. Must match matrix.image_ext in
# .github/workflows/promote_rc_release.yml so the promote step can find the
# .run file by predictable name.
SUFFIX=""
[[ "$BUILD_TYPE" == "RelWithDebInfo" ]] && SUFFIX="${SUFFIX}-relwithdebinfo"
[[ "$MALLOC" == "true" ]]                && SUFFIX="${SUFFIX}-malloc"
[[ "$CUDA" == "true" && "$CUGRAPH" != "true" ]] && SUFFIX="${SUFFIX}-cuda"
[[ "$CUGRAPH" == "true" ]]               && SUFFIX="${SUFFIX}-cugraph"

if [[ -z "$OUTPUT" ]]; then
  if ! command -v dpkg-deb >/dev/null 2>&1; then
    echo "Error: dpkg-deb not found on PATH; pass --output explicitly." >&2
    exit 1
  fi
  version=$(dpkg-deb -f "$MEMGRAPH_DEB" Version)
  if [[ -z "$version" ]]; then
    echo "Error: could not read Version from $MEMGRAPH_DEB; pass --output explicitly." >&2
    exit 1
  fi
  # The control file's Version is `<upstream>-<debian-revision>` (e.g.
  # 3.10.2-1). Strip the trailing `-<revision>` to match the CI naming
  # convention which uses just the upstream version.
  version="${version%-*}"
  OUTPUT="$PWD/memgraph-mage-offline-${version}-${ARCH}${SUFFIX}.run"
fi

echo "==> Build config: arch=$ARCH build_type=$BUILD_TYPE cuda=$CUDA malloc=$MALLOC cugraph=$CUGRAPH"
echo "==> Output: $OUTPUT"

STAGING=$(mktemp -d -t mg-offline-build-XXXXXX)
trap 'rm -rf "$STAGING"' EXIT

echo "==> Staging bundle in $STAGING"
mkdir -p "$STAGING/bundle/debs" "$STAGING/bundle/wheels" "$STAGING/bundle/requirements"

# Copy memgraph + mage debs into the bundle.
cp -v "$MEMGRAPH_DEB" "$STAGING/bundle/debs/"
cp -v "$MAGE_DEB" "$STAGING/bundle/debs/"

# The mage .deb's postinst will run install_python_requirements.sh, which
# reads /usr/lib/memgraph/mage-requirements.txt (packaged inside the deb based
# on the deb's own --cuda flag at deb-build time). Our install.sh's step 3
# pip-installs from the .run's bundled requirements file. To keep the version
# pins consistent, we mirror the deb's choice here: CUDA build → -gpu file.
if [[ "$CUDA" == "true" ]]; then
  MAGE_REQ_SRC="$PROJECT_ROOT/src/mage/python/requirements-gpu.txt"
else
  MAGE_REQ_SRC="$PROJECT_ROOT/src/mage/python/requirements.txt"
fi

cp -v "$MAGE_REQ_SRC" "$STAGING/bundle/requirements/mage-requirements.txt"
cp -v "$PROJECT_ROOT/src/auth/reference_modules/requirements.txt" \
  "$STAGING/bundle/requirements/auth-module-requirements.txt"

# Stage inputs for the wheels container.
mkdir -p "$STAGING/wheels-input/wheels-prebuilt"
cp -v "$MAGE_REQ_SRC" "$STAGING/wheels-input/requirements.txt"
cp -v "$PROJECT_ROOT/src/auth/reference_modules/requirements.txt" \
  "$STAGING/wheels-input/auth-module-requirements.txt"
if [[ -d "$WHEELS_PREBUILT_DIR" ]]; then
  shopt -s nullglob
  for whl in "$WHEELS_PREBUILT_DIR"/*.whl; do
    cp -v "$whl" "$STAGING/wheels-input/wheels-prebuilt/"
  done
  shopt -u nullglob
fi

# ---------------------------------------------------------------------------
# Pass 1: download apt deps in a fresh ubuntu:24.04 container.
# ---------------------------------------------------------------------------
echo "==> Downloading apt dependencies (fresh ubuntu:24.04 container)"
docker run --rm \
  --platform "linux/$ARCH" \
  -v "$OFFLINE_DIR/download-debs.sh:/usr/local/bin/download-debs.sh:ro" \
  -v "$STAGING/bundle/debs:/output/debs" \
  ubuntu:24.04 \
  bash /usr/local/bin/download-debs.sh

# ---------------------------------------------------------------------------
# Pass 2: download python wheels in a fresh ubuntu:24.04 container.
# ---------------------------------------------------------------------------
echo "==> Downloading Python wheels (fresh ubuntu:24.04 container)"
docker run --rm \
  --platform "linux/$ARCH" \
  -e ARCH="$ARCH" \
  -e CUDA="$CUDA" \
  -e CUDA_VERSION="$CUDA_VERSION" \
  -v "$OFFLINE_DIR/download-wheels.sh:/usr/local/bin/download-wheels.sh:ro" \
  -v "$STAGING/wheels-input:/input:ro" \
  -v "$STAGING/bundle/wheels:/output/wheels" \
  ubuntu:24.04 \
  bash /usr/local/bin/download-wheels.sh

# Drop the installer script into the bundle root.
cp -v "$OFFLINE_DIR/install.sh" "$STAGING/bundle/install.sh"
chmod +x "$STAGING/bundle/install.sh"

# ---------------------------------------------------------------------------
# Assemble the self-extracting .run.
# ---------------------------------------------------------------------------
echo "==> Packing bundle into $OUTPUT"

tar -C "$STAGING/bundle" -czf "$STAGING/payload.tar.gz" .

PREAMBLE="$STAGING/preamble.sh"
cat > "$PREAMBLE" <<'PREAMBLE_EOF'
#!/bin/bash
set -euo pipefail
# Self-extracting installer for memgraph + MAGE on Ubuntu 24.04.

if [[ "${EUID}" -ne 0 ]]; then
  echo "Error: this installer must be run as root (try: sudo $0)" >&2
  exit 1
fi

EXTRACT_DIR=$(mktemp -d -t mg-offline-XXXXXX)
trap 'rm -rf "$EXTRACT_DIR"' EXIT

echo "==> Extracting bundle to $EXTRACT_DIR"
ARCHIVE_LINE=$(awk '/^__PAYLOAD_BELOW__$/{print NR + 1; exit 0}' "$0")
tail -n +"$ARCHIVE_LINE" "$0" | tar xz -C "$EXTRACT_DIR"

BUNDLE_DIR="$EXTRACT_DIR" bash "$EXTRACT_DIR/install.sh" "$@"
exit $?
__PAYLOAD_BELOW__
PREAMBLE_EOF

cat "$PREAMBLE" "$STAGING/payload.tar.gz" > "$OUTPUT"
chmod +x "$OUTPUT"

size=$(du -h "$OUTPUT" | cut -f1)
echo
echo "Built offline installer: $OUTPUT ($size)"
echo "Run on target: sudo $OUTPUT"
