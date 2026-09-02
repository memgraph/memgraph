#!/usr/bin/env bash
# Throttle auditor: sample bench-core frequency + hottest thermal zone while a grid/sweep runs, so we
# can PROVE the CPU stayed pinned at the cap (no thermal throttling skewed the numbers). Run it in the
# background alongside phase3_sweep.sh; kill it when the sweep ends, then eyeball the min freq.
#
#   ./thermal_watch.sh [interval_s] [logfile]      (defaults: 5s, ./thermal_watch.log)
# Flags a sample when min bench-core freq drops below THROTTLE_PCT of the max-freq cap (default 98%).
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
INT="${1:-5}"; LOG="${2:-$HERE/thermal_watch.log}"
CORES="${WATCH_CORES:-0 1 2 3 4 5 6 7 8 9 10 11}"
CAP=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_max_freq)
THR=$(awk -v c="$CAP" -v p="${THROTTLE_PCT:-98}" 'BEGIN{printf "%d", c*p/100}')
echo "# thermal_watch: cap=${CAP}kHz throttle_below=${THR}kHz interval=${INT}s  $(date -u)" | tee "$LOG"
echo "# ts  min_cur_khz  max_cur_khz  hottest_zone_mC  FLAG" | tee -a "$LOG"
while true; do
  mn=99999999; mx=0
  for c in $CORES; do
    f=$(cat /sys/devices/system/cpu/cpu$c/cpufreq/scaling_cur_freq 2>/dev/null || echo 0)
    [ "$f" -lt "$mn" ] && mn=$f; [ "$f" -gt "$mx" ] && mx=$f
  done
  hot=0; for z in /sys/class/thermal/thermal_zone*/temp; do t=$(cat "$z" 2>/dev/null || echo 0); [ "$t" -gt "$hot" ] && hot=$t; done
  flag=""; [ "$mn" -lt "$THR" ] && flag="THROTTLE"
  printf '%s  %d  %d  %d  %s\n' "$(date -u +%H:%M:%S)" "$mn" "$mx" "$hot" "$flag" | tee -a "$LOG"
  sleep "$INT"
done
