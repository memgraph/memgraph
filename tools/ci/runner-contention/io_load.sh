#!/bin/bash
# Start, or stop, a competing IO load on the device the tests write to.
#
# O_DIRECT deliberately: it saturates the device without growing page cache,
# which would otherwise get the calling job killed for low memory.
#
# Usage: io_load.sh start | io_load.sh stop
set -u
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
LOADDIR=$HERE/.ioload
PIDFILE=$HERE/.ioload.pids

case "${1:-start}" in
start)
  mkdir -p "$LOADDIR"
  : > "$PIDFILE"
  for i in 1 2 3 4; do
    # setsid so each loop is its own process group and one kill takes the loop,
    # its dd, and any child together.
    setsid bash -c "while :; do
        dd if=/dev/zero of=$LOADDIR/blob_$i bs=1M count=2048 oflag=direct >/dev/null 2>&1
        dd if=$LOADDIR/blob_$i of=/dev/null bs=1M iflag=direct >/dev/null 2>&1
      done" >/dev/null 2>&1 &
    echo $! >> "$PIDFILE"
  done
  sleep 3
  echo "started $(wc -l < "$PIDFILE") io load groups"
  dd if=/dev/zero of="$LOADDIR/probe" bs=1M count=512 oflag=direct 2>&1 | tail -1
  rm -f "$LOADDIR/probe"
  ;;
stop)
  # Kill by recorded process group. Never match on a command line: this script's
  # own cmdline contains the very strings a pattern would look for.
  [ -f "$PIDFILE" ] && while read -r p; do kill -TERM -- "-$p" 2>/dev/null; done < "$PIDFILE"
  sleep 3
  [ -f "$PIDFILE" ] && while read -r p; do kill -KILL -- "-$p" 2>/dev/null; done < "$PIDFILE"
  pkill -x -KILL dd 2>/dev/null
  sleep 1
  rm -rf "$LOADDIR" "$PIDFILE"
  echo "stopped; dd still running: $(pgrep -x -c dd 2>/dev/null || echo 0)"
  ;;
*)
  echo "usage: $0 start|stop" >&2; exit 2 ;;
esac
