#!/bin/bash
set -euo pipefail
# Build a self-contained .run installer for memgraph + MAGE on Ubuntu 24.04.
#
# The output is a single executable file containing:
#   - memgraph .deb + memgraph-mage .deb
#   - every apt dependency .deb needed on a vanilla Ubuntu 24.04 host
#   - every Python wheel needed by mage and the auth modules
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
# Currently amd64 / CPU-only; --arch and --cuda are wired through for the
# future but not yet validated.

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$SCRIPT_DIR/../../.."
OFFLINE_DIR="$SCRIPT_DIR/offline-installer"

MEMGRAPH_DEB=""
MAGE_DEB=""
ARCH="amd64"
CUDA="false"
OUTPUT=""
WHEELS_PREBUILT_DIR="$PROJECT_ROOT/mage/wheels"

usage() {
  cat <<EOF
Usage: $0 --memgraph-deb PATH --mage-deb PATH [options]

Required:
  --memgraph-deb PATH     Path to memgraph_*.deb
  --mage-deb PATH         Path to memgraph-mage_*.deb

Options:
  --arch amd64            Target architecture (only amd64 supported today)
  --cuda true|false       Bundle GPU wheels (not supported yet; must be false)
  --output PATH           Output .run file (default: memgraph-mage-offline-<version>-<arch>.run)
  --wheels-dir PATH       Directory of pre-built host wheels to include
                          (default: $PROJECT_ROOT/mage/wheels)
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --memgraph-deb) MEMGRAPH_DEB="$2"; shift 2 ;;
    --mage-deb)     MAGE_DEB="$2";     shift 2 ;;
    --arch)         ARCH="$2";         shift 2 ;;
    --cuda)         CUDA="$2";         shift 2 ;;
    --output)       OUTPUT="$2";       shift 2 ;;
    --wheels-dir)   WHEELS_PREBUILT_DIR="$2"; shift 2 ;;
    -h|--help)      usage; exit 0 ;;
    *)              echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

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
if [[ "$ARCH" != "amd64" ]]; then
  echo "Error: only amd64 is supported in this version of the offline installer." >&2
  exit 1
fi
if [[ "$CUDA" != "false" ]]; then
  echo "Error: CUDA bundles are not supported yet (got --cuda $CUDA)." >&2
  exit 1
fi

# Derive a default output filename from the memgraph deb version.
if [[ -z "$OUTPUT" ]]; then
  base=$(basename "$MEMGRAPH_DEB")
  # memgraph_<version>_<arch>.deb -> <version>
  version=$(echo "$base" | sed -E 's/^memgraph_(.+)_[a-z0-9]+\.deb$/\1/')
  OUTPUT="$PWD/memgraph-mage-offline-${version}-${ARCH}.run"
fi

STAGING=$(mktemp -d -t mg-offline-build-XXXXXX)
trap 'rm -rf "$STAGING"' EXIT

echo "==> Staging bundle in $STAGING"
mkdir -p "$STAGING/bundle/debs" "$STAGING/bundle/wheels"

# Copy memgraph + mage debs into the bundle.
cp -v "$MEMGRAPH_DEB" "$STAGING/bundle/debs/"
cp -v "$MAGE_DEB" "$STAGING/bundle/debs/"

# Stage the input for the wheels container: requirements files + any prebuilt
# wheels the caller wants us to include.
mkdir -p "$STAGING/wheels-input/wheels-prebuilt"
cp -v "$PROJECT_ROOT/mage/python/requirements.txt" "$STAGING/wheels-input/requirements.txt"
cp -v "$PROJECT_ROOT/src/auth/reference_modules/requirements.txt" "$STAGING/wheels-input/auth-module-requirements.txt"
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

# Tar the bundle.
tar -C "$STAGING/bundle" -czf "$STAGING/payload.tar.gz" .

# Shell preamble. tail finds the line immediately after __PAYLOAD_BELOW__ and
# pipes the rest of the file into tar. We exec install.sh from the extract dir
# and exit before bash would parse the appended binary.
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
