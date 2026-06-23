#!/usr/bin/env bash
# Boot a clean Memgraph from the feat/projection build and run the projection
# demo through mgconsole.
#
#   ./run_projection_demo.sh                     # Acts 1-6, then the guardrails
#   ./run_projection_demo.sh --with-procedures   # also Act 7 (installs read.* module)
#
# Assumes ./build/memgraph exists (cmake --build --preset conan-debug --target memgraph).

set -uo pipefail

REPO="/home/gareth/checkout/memgraph"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEMO="$HERE/projection_demo.cypher"
DATADIR="$(mktemp -d)"
MODULES="$DATADIR/modules"
mkdir -p "$MODULES"

WITH_PROCS=0
[[ "${1:-}" == "--with-procedures" ]] && WITH_PROCS=1
if [[ "$WITH_PROCS" == 1 ]]; then
  cp "$REPO/tests/e2e/write_procedures/procedures/read.py" "$MODULES/read.py"
  echo ">> Installed read.* procedures into $MODULES"
fi

# Strip // comment-only lines and blanks: mgconsole's splitter mishandles comments
# wedged between statements. The .cypher file stays the human-readable artifact.
strip() { grep -vE '^[[:space:]]*(//|$)'; }

# Split the file on the @@ sentinels into the three sections.
MAIN="$(awk '/@@MAIN/{f=1;next} /@@OPTIONAL_PROC/{f=0} f' "$DEMO")"
PROC="$(awk '/@@OPTIONAL_PROC/{f=1;next} /@@EXPECTED_FAILURE/{f=0} f' "$DEMO")"
FAIL="$(awk '/@@EXPECTED_FAILURE/{f=1;next} f' "$DEMO")"

"$REPO/build/memgraph" \
  --data-directory="$DATADIR/data" \
  --query-modules-directory="$MODULES" \
  --storage-properties-on-edges \
  --log-level=ERROR --also-log-to-stderr \
  --storage-snapshot-on-exit=false --storage-wal-enabled=false \
  --data-recovery-on-startup=false \
  --telemetry-enabled=false >/dev/null 2>&1 &
MG_PID=$!
trap 'kill "$MG_PID" 2>/dev/null || true; rm -rf "$DATADIR"' EXIT

for _ in $(seq 1 50); do
  (echo > /dev/tcp/127.0.0.1/7687) >/dev/null 2>&1 && break
  sleep 0.2
done

echo ">> Acts 1-6"
printf '%s\n' "$MAIN" | strip | mgconsole --output-format=tabular

if [[ "$WITH_PROCS" == 1 ]]; then
  echo; echo ">> Act 7 (ambient view into a procedure)"
  printf '%s\n' "$PROC" | strip | mgconsole --output-format=tabular
fi

# mgconsole aborts a batch on the first error, so run each guardrail on its own
# and report that the rejection fired as designed.
echo; echo ">> Act 8 guardrails (each statement below must be REJECTED)"
printf '%s' "$FAIL" | strip | awk 'BEGIN{RS=";"} /[^[:space:]]/{print $0 ";"}' | while IFS= read -r -d ';' stmt; do
  [[ -z "${stmt//[[:space:]]/}" ]] && continue
  if printf '%s;\n' "$stmt" | mgconsole >/dev/null 2>&1; then
    echo "   UNEXPECTED PASS: ${stmt//$'\n'/ }"
  else
    echo "   rejected as expected: $(printf '%s' "$stmt" | tr '\n' ' ' | sed 's/  */ /g' | cut -c1-72)..."
  fi
done

echo; echo ">> Demo complete."
