#!/usr/bin/env bash
# Upload Memgraph CI core-dump stack traces to S3 and (best-effort) ping the
# remote monitoring server with the resulting s3:// URI(s).
#
# The destination prefix is handed in by collect.sh so the stack traces, the
# core dump and the build artifacts for one crash all land in the same folder:
#   s3://<bucket>/<s3-prefix>/   (e.g. ci-stack-traces/<run>/<job>/<hash>/)
#
# AWS credentials are taken from the environment (AWS_ACCESS_KEY_ID /
# AWS_SECRET_ACCESS_KEY / AWS_REGION), same as the Jepsen core-dump upload.
#
# Best-effort: no `set -e` — a failed upload must not abort before the
# annotations / monitoring pings run; each fallible call is guarded instead.
set -uo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"

TRACES_DIR=""
S3_PREFIX=""
S3_BUCKET="memgraph-cores"
S3_REGION="eu-west-1"
CORE_URL=""
BINARIES_URL=""

print_usage() {
  cat <<EOF
Usage: upload_stack_trace.sh --traces-dir DIR --s3-prefix PREFIX [OPTIONS]

Options:
  --traces-dir DIR    Directory of stack-trace files to upload (required)
  --s3-prefix PREFIX  S3 key prefix to upload under (required)
  --bucket NAME       S3 bucket (default: $S3_BUCKET)
  --region NAME       S3 region for the upload (default: $S3_REGION)
  --core-url URL      Core dump URL to include in the monitoring log line
  --binaries-url URL  Build-artifacts URL to include in the monitoring log line
  -h, --help          Show this help

Environment:
  AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_REGION  AWS credentials
  MONITORING_HOST (+ MONITORING_USERNAME/PASSWORD, CLUSTER_ID/ENV, SERVICE_NAME)
                  When MONITORING_HOST is set, a log line with the URLs is sent.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --traces-dir)   TRACES_DIR="$2"; shift 2 ;;
    --s3-prefix)    S3_PREFIX="$2"; shift 2 ;;
    --bucket)       S3_BUCKET="$2"; shift 2 ;;
    --region)       S3_REGION="$2"; shift 2 ;;
    --core-url)     CORE_URL="$2"; shift 2 ;;
    --binaries-url) BINARIES_URL="$2"; shift 2 ;;
    -h|--help)      print_usage; exit 0 ;;
    *) echo "Error: unknown option '$1'" >&2; print_usage >&2; exit 1 ;;
  esac
done

if [[ -z "$TRACES_DIR" || -z "$S3_PREFIX" ]]; then
  echo "Error: --traces-dir and --s3-prefix are required" >&2
  print_usage >&2
  exit 1
fi

if [[ ! -d "$TRACES_DIR" ]] || [[ -z "$(ls -A "$TRACES_DIR" 2>/dev/null)" ]]; then
  echo "No stack traces in '$TRACES_DIR' — nothing to upload."
  exit 0
fi

if ! command -v aws >/dev/null 2>&1; then
  echo "Error: aws CLI not found — cannot upload stack traces." >&2
  exit 1
fi

s3_uri="s3://${S3_BUCKET}/${S3_PREFIX}/"

echo "Uploading $(find "$TRACES_DIR" -type f | wc -l) stack-trace file(s) to $s3_uri"
if aws s3 cp --region "$S3_REGION" --sse aws:kms --recursive "$TRACES_DIR" "$s3_uri"; then
  echo "Stack traces available under: $s3_uri"
else
  # Upload failed (e.g. creds not configured). Still emit annotations/pings
  # below so the crash is at least visible, just without a working link.
  echo "Warning: stack-trace upload failed (continuing to annotations/ping)." >&2
fi

# Build the list of per-file URLs. The analyze step sanitizes trace filenames
# to a URL-safe set, so the uploaded object names need no extra encoding and we
# can point the log line / annotation directly at each individual file.
mapfile -t trace_files < <(cd "$TRACES_DIR" && find . -type f -printf '%P\n' | sort)

# Surface each file URL as a GitHub annotation when running in Actions.
if [[ -n "${GITHUB_ACTIONS:-}" ]]; then
  for f in "${trace_files[@]}"; do
    echo "::warning title=Memgraph core dump captured::Stack trace: ${s3_uri}${f}"
  done
fi

# Best-effort monitoring ping (no-op when MONITORING_HOST is unset). One log
# line per trace file, each carrying the direct URL to that file, the crash
# signal parsed from the trace filename (analyze names them ..._sig<N>.txt),
# and — when the core was uploaded — the core dump and build-artifacts URLs.
if [[ -n "${MONITORING_HOST:-}" ]]; then
  for f in "${trace_files[@]}"; do
    ping_args=(--url "${s3_uri}${f}")
    if [[ "$f" =~ _sig([0-9]+)\.txt$ ]]; then
      ping_args+=(--signal "${BASH_REMATCH[1]}")
    fi
    [[ -n "$CORE_URL" ]] && ping_args+=(--core-url "$CORE_URL")
    [[ -n "$BINARIES_URL" ]] && ping_args+=(--binaries-url "$BINARIES_URL")
    "$SCRIPT_DIR/ping_monitoring.sh" "${ping_args[@]}" || \
      echo "Warning: monitoring ping failed for ${f} (continuing)." >&2
  done
else
  echo "monitoring host not set — skipping ping."
fi
