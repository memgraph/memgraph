#!/usr/bin/env bash
# Build + perf the standalone cursor-dispatch model study (cursor_models.cpp).
# Compiles with the SAME toolchain/flags as the kernel (clang 20.1, -O2 -g -DNDEBUG, C++23),
# then runs perf stat (instructions/cycles/branch-misses) on every design across scenarios and
# prints instr/row + overhead-vs-legacy tables. See INVESTIGATION.md (EXP-5).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CXX=/opt/toolchain-v7/bin/clang++
BIN=/tmp/cursor_models
SCR="${SCR:-/tmp}"

echo "== compile (clang $($CXX --version | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1), -O2 -g -DNDEBUG -std=c++23) =="
"$CXX" -std=c++23 -O2 -g -DNDEBUG -fno-omit-frame-pointer "$HERE/cursor_models.cpp" -o "$BIN"

DESIGNS=(legacy fused mixed allcoro allcoroY helper)

# scenario: NAME N F W reps  (reps chosen so total emitted rows ~ 200M)
run_scenario() {
  local name=$1 N=$2 F=$3 W=$4 reps=$5
  echo
  echo "############ SCENARIO: $name  (N=$N F=$F W=$W reps=$reps) ############"
  printf "%-9s %14s %12s %10s %9s %8s\n" design instr/row "ns/row" "br-miss%" "IPC" "vs-legacy"
  local base_ipr=""
  for d in "${DESIGNS[@]}"; do
    perf stat -e instructions,cycles,branches,branch-misses "$BIN" "$d" "$N" "$F" "$W" "$reps" \
      >"$SCR/cm_out_$d.txt" 2>"$SCR/cm_perf_$d.txt"
    local rows ins cyc brm nspr
    rows=$(grep -oE 'rows=[0-9]+' "$SCR/cm_out_$d.txt" | cut -d= -f2)
    nspr=$(grep -oE 'ns/row=[0-9.]+' "$SCR/cm_out_$d.txt" | cut -d= -f2)
    ins=$(grep -E '\binstructions\b' "$SCR/cm_perf_$d.txt" | grep -oE '^[ ]*[0-9.]+' | tr -d ' .' )
    cyc=$(grep -E '\bcycles\b' "$SCR/cm_perf_$d.txt" | grep -oE '^[ ]*[0-9.]+' | tr -d ' .' )
    brm=$(grep -E 'branch-misses' "$SCR/cm_perf_$d.txt" | grep -oE '[0-9.]+%' | head -1)
    python3 - "$d" "$ins" "$rows" "$cyc" "$nspr" "$brm" "${base_ipr:-0}" <<'PY'
import sys
d,ins,rows,cyc,nspr,brm,base=sys.argv[1:8]
ins=float(ins); rows=float(rows); cyc=float(cyc); nspr=float(nspr); base=float(base)
ipr=ins/rows; ipc=ins/cyc if cyc else 0
vs = "" if base==0 else f"{100*(ipr-base)/base:+.1f}%"
print(f"{d:<9} {ipr:14.1f} {nspr:12.3f} {brm:>10} {ipc:9.3f} {vs:>8}")
PY
    if [[ "$d" == "legacy" ]]; then base_ipr=$(python3 -c "print($ins/$rows)"); fi
  done
}

run_scenario "chain_pulldense"  1000000 1   0  200   # F=1, ~0 work: worst case (count(*)-over-expand)
run_scenario "chain_realistic"  1000000 1  20  200   # F=1, real per-row work
run_scenario "fanout_pulldense"  100000 100 0  20    # F=100: input pull amortized

echo
echo "############ DEPTH SWEEP: D pass-through coroutine cursors over an immediate leaf (W=0) ############"
echo "(crossings/row == D; slope of instr/row over D == per-coroutine-boundary cost)"
printf "%-7s %14s %12s %10s\n" depth instr/row "ns/row" "IPC"
for D in 0 1 2 4 8 16; do
  perf stat -e instructions,cycles "$BIN" depth 1000000 "$D" 0 200 >"$SCR/cm_out_d.txt" 2>"$SCR/cm_perf_d.txt"
  rows=$(grep -oE 'rows=[0-9]+' "$SCR/cm_out_d.txt" | cut -d= -f2)
  nspr=$(grep -oE 'ns/row=[0-9.]+' "$SCR/cm_out_d.txt" | cut -d= -f2)
  ins=$(grep -E '\binstructions\b' "$SCR/cm_perf_d.txt" | grep -oE '^[ ]*[0-9.]+' | tr -d ' .')
  cyc=$(grep -E '\bcycles\b' "$SCR/cm_perf_d.txt" | grep -oE '^[ ]*[0-9.]+' | tr -d ' .')
  python3 - "$D" "$ins" "$rows" "$cyc" "$nspr" <<'PY'
import sys
D,ins,rows,cyc,nspr=sys.argv[1:6]
ins=float(ins);rows=float(rows);cyc=float(cyc);nspr=float(nspr)
print(f"{D:<7} {ins/rows:14.1f} {nspr:12.3f} {ins/cyc:10.3f}")
PY
done
echo
echo "############ EXTERNAL-YIELD (I/O park) SWEEP: allcoroEY vs fusedEY, chain F=1 W=0 ############"
echo "(period P = park every P-th access; models on-disk cold-page frequency. instr/row incl. park+resume)"
printf "%-10s %8s %14s %12s %10s\n" design period instr/row "ns/row" "IPC"
for P in 0 64 8 1; do
  for d in allcoroEY fusedEY; do
    perf stat -e instructions,cycles "$BIN" "$d" 1000000 1 0 200 "$P" >"$SCR/cm_out_ey.txt" 2>"$SCR/cm_perf_ey.txt"
    rows=$(grep -oE 'rows=[0-9]+' "$SCR/cm_out_ey.txt" | cut -d= -f2)
    nspr=$(grep -oE 'ns/row=[0-9.]+' "$SCR/cm_out_ey.txt" | cut -d= -f2)
    ins=$(grep -E '\binstructions\b' "$SCR/cm_perf_ey.txt" | grep -oE '^[ ]*[0-9.]+' | tr -d ' .')
    cyc=$(grep -E '\bcycles\b' "$SCR/cm_perf_ey.txt" | grep -oE '^[ ]*[0-9.]+' | tr -d ' .')
    python3 - "$d" "$P" "$ins" "$rows" "$cyc" "$nspr" <<'PY'
import sys
d,P,ins,rows,cyc,nspr=sys.argv[1:7]
ins=float(ins);rows=float(rows);cyc=float(cyc);nspr=float(nspr)
print(f"{d:<10} {P:>8} {ins/rows:14.1f} {nspr:12.3f} {ins/cyc:10.3f}")
PY
  done
done
echo
echo "instr/row = retired instructions per emitted row (deterministic; the primary signal)."
echo "vs-legacy = extra instr/row over the virtual-Pull baseline."
