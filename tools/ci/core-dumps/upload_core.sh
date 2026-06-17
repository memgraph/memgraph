#!/usr/bin/env bash
# Upload a Memgraph core dump and the matching build artifacts to S3, so the
# core can be loaded in gdb offline.
#
# The destination prefix is handed in by collect.sh, so the core, the build
# artifacts and the stack traces for one crash all share a folder:
#   s3://<bucket>/<s3-prefix>/
#     binaries.tar.gz   - memgraph + *.debug + *.so, retaining build/ structure
#     <core>.gz         - the gzip-compressed core dump(s)
#
# Whether the core uploads depends on --mode and its size:
#   false  - never upload
#   auto   - upload only cores <= --core-size-limit (default 2 GiB)
#   true   - upload cores below a 1 TiB hard safety ceiling
# The build-artifacts tarball is uploaded only when at least one core uploads
# (binaries are useless without a core to load them against).
#
# This stays a separate concern from the packaging debug-symbol upload
# (tools/ci/upload-debug-symbols.sh, the build-id-keyed debuginfod layout).
#
# Both the core and the binaries live inside the build container and the aws
# CLI does not, so everything is streamed out of the container straight into
# `aws s3 cp -` (the multi-GB core is never staged on the host disk).
#
# Best-effort: never fail the CI job.
set -uo pipefail

BUILD_CONTAINER=""
CORES_DIR="/tmp/mg-cores"
BUILD_DIR="/home/mg/memgraph/build"
S3_PREFIX=""
S3_BUCKET="deps.memgraph.io"
S3_REGION="eu-west-1"
MODE="auto"
CORE_SIZE_LIMIT="2"   # GiB
URL_OUT=""
EXEC_USER="mg"

# Sizes are expressed in GiB. Hard safety ceiling for --mode true so a runaway /
# corrupt core can't trigger an absurd upload.
HARD_LIMIT_GIB=1024   # 1 TiB
GIB=$((1024 * 1024 * 1024))

print_usage() {
  cat <<EOF
Usage: upload_core.sh --build-container NAME --s3-prefix PREFIX [OPTIONS]

Options:
  --build-container NM  Container holding the core + build artifacts (required)
  --s3-prefix PREFIX    S3 key prefix to upload under (required)
  --cores-dir DIR       Core dump dir inside the container (default: $CORES_DIR)
  --build-dir DIR       Build dir inside the container (default: $BUILD_DIR)
  --bucket NAME         S3 bucket (default: $S3_BUCKET)
  --region NAME         S3 region for the HTTP URL (default: $S3_REGION)
  --mode MODE           true | false | auto (default: $MODE)
  --core-size-limit N   auto threshold in GiB (default: $CORE_SIZE_LIMIT)
  --exec-user USER      Container user to run stat/gzip/tar as (default: $EXEC_USER)
  --url-out FILE        Write 'binaries_url=' / 'core_url=' lines for the caller
  -h, --help            Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --build-container) BUILD_CONTAINER="$2"; shift 2 ;;
    --cores-dir)       CORES_DIR="$2"; shift 2 ;;
    --build-dir)       BUILD_DIR="$2"; shift 2 ;;
    --s3-prefix)       S3_PREFIX="$2"; shift 2 ;;
    --bucket)          S3_BUCKET="$2"; shift 2 ;;
    --region)          S3_REGION="$2"; shift 2 ;;
    --mode)            MODE="$2"; shift 2 ;;
    --core-size-limit) CORE_SIZE_LIMIT="$2"; shift 2 ;;
    --exec-user)       EXEC_USER="$2"; shift 2 ;;
    --url-out)         URL_OUT="$2"; shift 2 ;;
    -h|--help)         print_usage; exit 0 ;;
    *) echo "Error: unknown option '$1'" >&2; print_usage >&2; exit 1 ;;
  esac
done

# Always start the url-out file empty so the caller never reads stale URLs.
[[ -n "$URL_OUT" ]] && : > "$URL_OUT"

if [[ -z "$BUILD_CONTAINER" || -z "$S3_PREFIX" ]]; then
  echo "Error: --build-container and --s3-prefix are required" >&2
  exit 1
fi

# Resolve the effective size limit (in GiB) from the mode, then to bytes.
case "$MODE" in
  false) echo "Core upload mode=false — skipping core and build-artifact upload."; exit 0 ;;
  true)  limit_gib=$HARD_LIMIT_GIB ;;
  auto)
    if [[ ! "$CORE_SIZE_LIMIT" =~ ^[0-9]+$ ]]; then
      echo "Error: --core-size-limit must be an integer number of GiB (got '$CORE_SIZE_LIMIT')" >&2
      exit 1
    fi
    limit_gib=$CORE_SIZE_LIMIT
    ;;
  *) echo "Error: --mode must be true, false or auto (got '$MODE')" >&2; exit 1 ;;
esac
limit_bytes=$(( limit_gib * GIB ))

if ! command -v aws >/dev/null 2>&1; then
  echo "Error: aws CLI not found — cannot upload core." >&2
  exit 1
fi

if ! docker inspect "$BUILD_CONTAINER" >/dev/null 2>&1; then
  echo "Container '$BUILD_CONTAINER' not found — skipping core upload."
  exit 0
fi

base_uri="s3://${S3_BUCKET}/${S3_PREFIX}"
base_url="https://s3.${S3_REGION}.amazonaws.com/${S3_BUCKET}/${S3_PREFIX}"

# Gather cores with their sizes and decide which are within the limit.
mapfile -t core_lines < <(docker exec -u "$EXEC_USER" "$BUILD_CONTAINER" bash -c \
  "for f in ${CORES_DIR}/core.*; do [ -e \"\$f\" ] && stat -c '%s	%n' \"\$f\"; done" 2>/dev/null)

eligible=()
for line in "${core_lines[@]}"; do
  [[ -z "$line" ]] && continue
  size="${line%%	*}"
  path="${line#*	}"
  if (( size <= limit_bytes )); then
    eligible+=("${size}	${path}")
  else
    echo "Skipping core ${path} ($((size / 1048576)) MiB) — exceeds limit (${limit_gib} GiB, mode=${MODE})."
  fi
done

if [[ ${#eligible[@]} -eq 0 ]]; then
  echo "No core within the size limit — skipping core and build-artifact upload."
  exit 0
fi

# 1) The ELF set (binary + .debug sidecars + .so), tarred inside the container
#    with build/-relative paths and streamed to S3 as one object. Only now that
#    at least one core qualifies.
build_parent="$(dirname "$BUILD_DIR")"
build_base="$(basename "$BUILD_DIR")"
echo "Uploading build artifacts (memgraph + *.debug + *.so) -> ${base_uri}/binaries.tar.gz"
if docker exec -u "$EXEC_USER" "$BUILD_CONTAINER" bash -c \
     "cd '$build_parent' && find '$build_base' -type f \\( -name memgraph -o -name '*.debug' -o -name '*.so' \\) -print0 | tar --null -czf - -T -" \
     | aws s3 cp - "${base_uri}/binaries.tar.gz"; then
  echo "  build artifacts uploaded: ${base_url}/binaries.tar.gz"
else
  echo "Warning: build artifact upload failed (continuing)." >&2
fi

# 2) The eligible core(s): gzip-compressed inside the container and streamed
#    out. --expected-size uses the raw size as an upper bound so aws sizes
#    multipart parts correctly for large streams; overestimating is harmless.
first_core_url=""
for entry in "${eligible[@]}"; do
  raw_size="${entry%%	*}"
  core="${entry#*	}"
  cbase="$(basename "$core")"
  echo "Uploading core ${core} ($((raw_size / 1048576)) MiB raw) -> ${base_uri}/${cbase}.gz"
  if docker exec -u "$EXEC_USER" "$BUILD_CONTAINER" sh -c "gzip -c '$core'" \
       | aws s3 cp --expected-size "$raw_size" - "${base_uri}/${cbase}.gz"; then
    echo "  core uploaded: ${base_url}/${cbase}.gz"
    [[ -z "$first_core_url" ]] && first_core_url="${base_url}/${cbase}.gz"
  else
    echo "Warning: core upload failed for ${core} (continuing)." >&2
  fi
done

if [[ -n "$URL_OUT" ]]; then
  {
    echo "binaries_url=${base_url}/binaries.tar.gz"
    echo "core_url=${first_core_url}"
  } > "$URL_OUT"
fi

echo "Core bundle available under: ${base_url}/"
if [[ -n "${GITHUB_ACTIONS:-}" ]]; then
  echo "::warning title=Memgraph core dump uploaded::Core + binaries bundle: ${base_url}/"
fi
