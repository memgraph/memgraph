#!/usr/bin/env bash
# Upload ELF debug-symbol sidecars (*.debug) to a build-id-keyed S3 bucket.
#
# Usage: upload-debug-symbols.sh <source-dir> [--bucket <bucket>] [--prefix <prefix>]
#                                              [--endpoint-url <url>] [--region <region>]
#                                              [--dry-run]
#
# Each .debug file is uploaded to
#   s3://<bucket>/<prefix>/<aa>/<rest>.debug
# where <aa> is the first two hex chars of the ELF .note.gnu.build-id and
# <rest> is the remainder — the canonical layout understood by debuginfod,
# gdb, and lldb. A future debuginfod server can sit directly on top of this
# bucket without any re-indexing.
#
# --endpoint-url overrides the S3 endpoint for the underlying `aws s3 cp`
# (use this for MinIO and other S3-compatible servers; AWS CLI v1 ignores
# the AWS_ENDPOINT_URL env var, so passing it explicitly is the safe path).
# --region sets --region on the cp; some SDK versions error without one.

set -euo pipefail

SRC_DIR=""
BUCKET="deps.memgraph.io"
PREFIX="debug-symbols"
ENDPOINT_URL=""
REGION=""
DRY_RUN=false
# When set, after uploading each .debug we write the build-id list out to a
# local temp file and `aws s3 cp` it to this URL. Promotion (rc -> release)
# reads these manifests to know which build-ids to copy from the staging
# bucket (deps.memgraph.io) into the production debuginfod bucket
# (memgraph-debugsym), so daily / PR runs (which don't pass this flag) leave
# no trace in the release symbol server.
MANIFEST_S3_URL=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bucket) BUCKET=$2; shift 2 ;;
    --prefix) PREFIX=$2; shift 2 ;;
    --endpoint-url) ENDPOINT_URL=$2; shift 2 ;;
    --region) REGION=$2; shift 2 ;;
    --manifest-s3-url) MANIFEST_S3_URL=$2; shift 2 ;;
    --dry-run) DRY_RUN=true; shift ;;
    -h|--help)
      sed -n '2,/^$/p' "$0" | sed 's/^# //; s/^#//'
      exit 0
      ;;
    -*) echo "Unknown flag: $1" >&2; exit 1 ;;
    *)
      if [[ -z "$SRC_DIR" ]]; then
        SRC_DIR=$1
      else
        echo "Unexpected positional arg: $1" >&2
        exit 1
      fi
      shift
      ;;
  esac
done

# Build the optional pieces of the aws s3 cp command line once.
aws_extra_args=()
if [[ -n "$ENDPOINT_URL" ]]; then
  aws_extra_args+=(--endpoint-url "$ENDPOINT_URL")
fi
if [[ -n "$REGION" ]]; then
  aws_extra_args+=(--region "$REGION")
fi

if [[ -z "$SRC_DIR" || ! -d "$SRC_DIR" ]]; then
  echo "Error: source directory required" >&2
  echo "Usage: $0 <source-dir> [--bucket <bucket>] [--prefix <prefix>] [--dry-run]" >&2
  exit 1
fi

if [[ "$DRY_RUN" != "true" ]] && ! command -v aws >/dev/null; then
  echo "Error: aws CLI not found" >&2
  exit 1
fi
if ! command -v readelf >/dev/null; then
  echo "Error: readelf not found" >&2
  exit 1
fi

manifest_tmp=""
if [[ -n "$MANIFEST_S3_URL" ]]; then
  manifest_tmp=$(mktemp)
  trap 'rm -f "$manifest_tmp"' EXIT
fi

uploaded=0
skipped=0
while IFS= read -r -d '' f; do
  build_id=$(readelf -n "$f" 2>/dev/null | awk '/Build ID:/ {print $3}' | head -n 1)
  if [[ -z "$build_id" || ${#build_id} -lt 3 ]]; then
    echo "Warning: no build-id for $f, skipping" >&2
    skipped=$((skipped + 1))
    continue
  fi
  key="${PREFIX}/${build_id:0:2}/${build_id:2}.debug"
  target="s3://${BUCKET}/${key}"
  if [[ "$DRY_RUN" == "true" ]]; then
    echo "[dry-run] $f -> $target"
  else
    echo "Uploading $(basename "$f") (build-id ${build_id:0:12}...) -> $target"
    aws s3 cp "${aws_extra_args[@]}" --only-show-errors "$f" "$target"
  fi
  if [[ -n "$manifest_tmp" ]]; then
    echo "$build_id" >> "$manifest_tmp"
  fi
  uploaded=$((uploaded + 1))
done < <(find "$SRC_DIR" -name '*.debug' -type f -print0)

echo "Done. Uploaded: $uploaded, skipped: $skipped."
if [[ "$uploaded" -eq 0 ]]; then
  echo "Error: no .debug files found under $SRC_DIR" >&2
  exit 1
fi

if [[ -n "$MANIFEST_S3_URL" ]]; then
  # One build-id per line, sorted+unique so retries don't bloat the file.
  sort -u "$manifest_tmp" -o "$manifest_tmp"
  if [[ "$DRY_RUN" == "true" ]]; then
    echo "[dry-run] manifest -> $MANIFEST_S3_URL"
    cat "$manifest_tmp"
  else
    echo "Uploading manifest ($(wc -l < "$manifest_tmp") build-ids) -> $MANIFEST_S3_URL"
    aws s3 cp "${aws_extra_args[@]}" --only-show-errors "$manifest_tmp" "$MANIFEST_S3_URL"
  fi
fi
