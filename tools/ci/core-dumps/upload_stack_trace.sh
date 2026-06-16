#!/usr/bin/env bash
# Upload Memgraph CI core-dump stack traces to S3 and (best-effort) ping the
# remote monitoring server with the resulting HTTP URL.
#
# Destination layout:
#   s3://deps.memgraph.io/ci-stack-traces/<run-id>/<job-id>/<random-hash>/
# which maps to the HTTP URL:
#   https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/ci-stack-traces/<run-id>/<job-id>/<random-hash>/
#
# AWS credentials are taken from the environment (AWS_ACCESS_KEY_ID /
# AWS_SECRET_ACCESS_KEY / AWS_REGION), same as the Jepsen core-dump upload.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"

TRACES_DIR=""
RUN_ID=""
JOB_ID=""
S3_BUCKET="deps.memgraph.io"
S3_REGION="${AWS_REGION:-eu-west-1}"

print_usage() {
  cat <<EOF
Usage: upload_stack_trace.sh --traces-dir DIR --run-id ID --job-id ID [OPTIONS]

Options:
  --traces-dir DIR  Directory of stack-trace files to upload (required)
  --run-id ID       Workflow run id (required)
  --job-id ID       Job identifier (required)
  --bucket NAME     S3 bucket (default: $S3_BUCKET)
  --region NAME     S3 region for the HTTP URL (default: $S3_REGION)
  -h, --help        Show this help

Environment:
  AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_REGION  AWS credentials
  MONITORING_HOST (+ MONITORING_USERNAME/PASSWORD, CLUSTER_ID/ENV, SERVICE_NAME)
                  When MONITORING_HOST is set, a log line with the URL is sent.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --traces-dir) TRACES_DIR="$2"; shift 2 ;;
    --run-id)     RUN_ID="$2"; shift 2 ;;
    --job-id)     JOB_ID="$2"; shift 2 ;;
    --bucket)     S3_BUCKET="$2"; shift 2 ;;
    --region)     S3_REGION="$2"; shift 2 ;;
    -h|--help)    print_usage; exit 0 ;;
    *) echo "Error: unknown option '$1'" >&2; print_usage >&2; exit 1 ;;
  esac
done

if [[ -z "$TRACES_DIR" || -z "$RUN_ID" || -z "$JOB_ID" ]]; then
  echo "Error: --traces-dir, --run-id and --job-id are required" >&2
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

# Random leaf segment so concurrent jobs / matrix legs / re-runs never collide.
if command -v openssl >/dev/null 2>&1; then
  hash="$(openssl rand -hex 8)"
else
  hash="$(head -c 8 /dev/urandom | od -An -tx1 | tr -d ' \n')"
fi

prefix="ci-stack-traces/${RUN_ID}/${JOB_ID}/${hash}"
s3_uri="s3://${S3_BUCKET}/${prefix}/"
http_url="https://s3.${S3_REGION}.amazonaws.com/${S3_BUCKET}/${prefix}/"

echo "Uploading $(find "$TRACES_DIR" -type f | wc -l) stack-trace file(s) to $s3_uri"
aws s3 cp --recursive "$TRACES_DIR" "$s3_uri"

echo "Stack traces available under: $http_url"

# Build the list of per-file URLs. The analyze step sanitizes trace filenames
# to a URL-safe set, so the uploaded object names need no extra encoding and we
# can point the log line / annotation directly at each individual file.
mapfile -t trace_files < <(cd "$TRACES_DIR" && find . -type f -printf '%P\n' | sort)

# Surface each file URL as a GitHub annotation when running in Actions.
if [[ -n "${GITHUB_ACTIONS:-}" ]]; then
  for f in "${trace_files[@]}"; do
    echo "::warning title=Memgraph core dump captured::Stack trace: ${http_url}${f}"
  done
fi

# Best-effort monitoring ping (no-op when MONITORING_HOST is unset). One log
# line per trace file, each carrying the direct URL to that file plus the crash
# signal parsed from the trace filename (analyze names them ..._sig<N>.txt).
if [[ -n "${MONITORING_HOST:-}" ]]; then
  for f in "${trace_files[@]}"; do
    ping_args=(--url "${http_url}${f}")
    if [[ "$f" =~ _sig([0-9]+) ]]; then
      ping_args+=(--signal "${BASH_REMATCH[1]}")
    fi
    "$SCRIPT_DIR/ping_monitoring.sh" "${ping_args[@]}" || \
      echo "Warning: monitoring ping failed for ${f} (continuing)." >&2
  done
else
  echo "monitoring host not set — skipping ping."
fi
