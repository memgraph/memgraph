#!/usr/bin/env bash
#
# Compare two ClangBuildAnalyzer analysis outputs produced by
# tools/profile_build_time.sh. Used to validate reproducibility: run the
# profiling script twice with different LABELs, then run this to check the
# results agree.
#
#   tools/profile_build_time_compare.sh LABEL_A LABEL_B
#
set -euo pipefail

LABEL_A="${1:?usage: $0 LABEL_A LABEL_B}"
LABEL_B="${2:?usage: $0 LABEL_A LABEL_B}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULTS_DIR="${REPO_ROOT}/build_time_results"

A="${RESULTS_DIR}/analysis_${LABEL_A}.txt"
B="${RESULTS_DIR}/analysis_${LABEL_B}.txt"
[[ -s "${A}" ]] || { echo "missing or empty: ${A}" >&2; exit 1; }
[[ -s "${B}" ]] || { echo "missing or empty: ${B}" >&2; exit 1; }

# ClangBuildAnalyzer prints sections beginning with a line like
#   **** Files that took longest to parse (compiler frontend):
# followed by ranked entries and a blank line. Extract up to N lines after
# each such header for diffing.
extract_sections() {
  local file="$1" n="${2:-20}"
  awk -v n="${n}" '
    /^\*\*\*\*/ { in_sect=1; count=0; print; next }
    in_sect && NF==0 { in_sect=0; print ""; next }
    in_sect && count < n { print; count++; next }
    in_sect { next }  # drop tail beyond N
    { next }          # drop non-section lines (totals, prelude)
  ' "${file}"
}

TMP_A="$(mktemp)"; TMP_B="$(mktemp)"
trap 'rm -f "${TMP_A}" "${TMP_B}"' EXIT

extract_sections "${A}" 20 >"${TMP_A}"
extract_sections "${B}" 20 >"${TMP_B}"

echo "=== diff: top-20 per section (${LABEL_A} vs ${LABEL_B}) ==="
if diff -u --label "${LABEL_A}" "${TMP_A}" --label "${LABEL_B}" "${TMP_B}"; then
  echo "sections are byte-identical"
fi
