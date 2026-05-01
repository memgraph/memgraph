#!/usr/bin/env bash
# Upload split-DWARF .dwp debug bundles to a build-id-keyed S3 bucket.
#
# Usage: upload-debug-symbols.sh <source-dir> [--bucket <bucket>] [--prefix <prefix>]
#                                              [--endpoint-url <url>] [--region <region>]
#                                              [--dry-run]
#
# Each .dwp is uploaded to
#   s3://<bucket>/<prefix>/<aa>/<rest>.dwp
# where <aa> is the first two hex chars of the matching binary's
# .note.gnu.build-id and <rest> is the remainder. This is the canonical
# debuginfod layout (with .debug -> .dwp): a debuginfod server / proxy can
# sit directly on top of this bucket and clients (gdb 16+ via DEBUGINFOD_URLS)
# resolve split-DWARF debug info by binary build-id.
#
# The build-id is taken from the matching binary file: foo.dwp pairs with
# binary `foo` next to it. If `foo` isn't present the .dwp is skipped.
#
# --endpoint-url overrides the S3 endpoint for the underlying `aws s3 cp`
# (use this for MinIO and other S3-compatible servers).
# --region sets --region on the cp.

set -euo pipefail

SRC_DIR=""
BUCKET="deps.memgraph.io"
PREFIX="debug-symbols"
ENDPOINT_URL=""
REGION=""
DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bucket) BUCKET=$2; shift 2 ;;
    --prefix) PREFIX=$2; shift 2 ;;
    --endpoint-url) ENDPOINT_URL=$2; shift 2 ;;
    --region) REGION=$2; shift 2 ;;
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

uploaded=0
skipped=0
while IFS= read -r -d '' dwp; do
  binary="${dwp%.dwp}"
  if [[ ! -f "$binary" ]]; then
    echo "Warning: no binary at $binary for $dwp, skipping" >&2
    skipped=$((skipped + 1))
    continue
  fi
  build_id=$(readelf -n "$binary" 2>/dev/null | awk '/Build ID:/ {print $3}' | head -n 1)
  if [[ -z "$build_id" || ${#build_id} -lt 3 ]]; then
    echo "Warning: no build-id in $binary, skipping" >&2
    skipped=$((skipped + 1))
    continue
  fi
  key="${PREFIX}/${build_id:0:2}/${build_id:2}.dwp"
  target="s3://${BUCKET}/${key}"
  if [[ "$DRY_RUN" == "true" ]]; then
    echo "[dry-run] $dwp -> $target"
  else
    echo "Uploading $(basename "$dwp") (build-id ${build_id:0:12}...) -> $target"
    aws s3 cp "${aws_extra_args[@]}" --only-show-errors "$dwp" "$target"
  fi
  uploaded=$((uploaded + 1))
done < <(find "$SRC_DIR" -name '*.dwp' -type f -print0)

echo "Done. Uploaded: $uploaded, skipped: $skipped."
if [[ "$uploaded" -eq 0 ]]; then
  echo "Error: no uploadable .dwp files found under $SRC_DIR" >&2
  exit 1
fi
