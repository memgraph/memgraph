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
# This is deliberately the same folder as the stack traces but stays a separate
# concern from the packaging debug-symbol upload (tools/ci/upload-debug-symbols.sh,
# which writes a build-id-keyed debuginfod layout under .../debug-symbols/).
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
S3_REGION="${AWS_REGION:-eu-west-1}"

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
    -h|--help)         print_usage; exit 0 ;;
    *) echo "Error: unknown option '$1'" >&2; print_usage >&2; exit 1 ;;
  esac
done

if [[ -z "$BUILD_CONTAINER" || -z "$S3_PREFIX" ]]; then
  echo "Error: --build-container and --s3-prefix are required" >&2
  exit 1
fi

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

# 1) The 445 MiB ELF set (binary + .debug sidecars + .so), tarred inside the
#    container with build/-relative paths and streamed to S3 as one object.
build_parent="$(dirname "$BUILD_DIR")"
build_base="$(basename "$BUILD_DIR")"
echo "Uploading build artifacts (memgraph + *.debug + *.so) -> ${base_uri}/binaries.tar.gz"
if docker exec -u mg "$BUILD_CONTAINER" bash -c \
     "cd '$build_parent' && find '$build_base' -type f \\( -name memgraph -o -name '*.debug' -o -name '*.so' \\) -print0 | tar --null -czf - -T -" \
     | aws s3 cp - "${base_uri}/binaries.tar.gz"; then
  echo "  build artifacts uploaded: ${base_url}/binaries.tar.gz"
else
  echo "Warning: build artifact upload failed (continuing)." >&2
fi

# 2) The core dump(s): gzip-compressed inside the container and streamed out.
#    --expected-size uses the raw size as an upper bound so aws sizes multipart
#    parts correctly for large (>50 GB) streams; overestimating is harmless.
while IFS= read -r core; do
  [[ -z "$core" ]] && continue
  cbase="$(basename "$core")"
  raw_size="$(docker exec "$BUILD_CONTAINER" stat -c %s "$core" 2>/dev/null || echo 0)"
  echo "Uploading core ${core} ($((raw_size / 1048576)) MiB raw) -> ${base_uri}/${cbase}.gz"
  if docker exec "$BUILD_CONTAINER" sh -c "gzip -c '$core'" \
       | aws s3 cp --expected-size "$raw_size" - "${base_uri}/${cbase}.gz"; then
    echo "  core uploaded: ${base_url}/${cbase}.gz"
  else
    echo "Warning: core upload failed for ${core} (continuing)." >&2
  fi
done < <(docker exec "$BUILD_CONTAINER" bash -c "ls -1 ${CORES_DIR}/core.* 2>/dev/null")

echo "Core bundle available under: ${base_url}/"
if [[ -n "${GITHUB_ACTIONS:-}" ]]; then
  echo "::warning title=Memgraph core dump uploaded::Core + binaries bundle: ${base_url}/"
fi
