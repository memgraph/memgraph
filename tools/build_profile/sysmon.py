#!/usr/bin/env python3
"""Sample machine-wide memory, CPU and memory pressure while a build runs.

One JSON line per sample. Stops cleanly on SIGTERM/SIGINT.

    sysmon.py --log FILE [--interval SECONDS]
"""
import argparse
import json
import os
import signal
import sys
import time

MEMINFO_KEYS = (
    "MemTotal",
    "MemAvailable",
    "MemFree",
    "Buffers",
    "Cached",
    "Shmem",
    "SwapTotal",
    "SwapFree",
    "Dirty",
    "Writeback",
)
CGROUP_FILES = (
    "/sys/fs/cgroup/memory.current",  # v2, own namespace
    "/sys/fs/cgroup/memory/memory.usage_in_bytes",
)  # v1


def read_meminfo():
    out = {}
    with open("/proc/meminfo") as f:
        for line in f:
            key, _, rest = line.partition(":")
            if key in MEMINFO_KEYS:
                out[key.lower() + "_kb"] = int(rest.split()[0])
    return out


def read_cpu():
    with open("/proc/stat") as f:
        fields = f.readline().split()[1:]
    values = [int(x) for x in fields]
    idle = values[3] + (values[4] if len(values) > 4 else 0)
    return idle, sum(values)


def read_optional(path):
    try:
        with open(path) as f:
            return f.read()
    except OSError:
        return None


def read_pressure():
    text = read_optional("/proc/pressure/memory")
    if not text:
        return None
    out = {}
    for line in text.splitlines():
        parts = line.split()
        if parts and parts[0] in ("some", "full"):
            for kv in parts[1:]:
                k, _, v = kv.partition("=")
                if k == "avg10":
                    out[parts[0] + "_avg10"] = float(v)
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--log", required=True)
    ap.add_argument("--interval", type=float, default=0.5)
    args = ap.parse_args()

    stop = []
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.append(True))

    cgroup_file = next((p for p in CGROUP_FILES if os.path.exists(p)), None)
    prev_idle, prev_total = read_cpu()
    with open(args.log, "a") as log:
        while not stop:
            rec = {"t": round(time.time(), 3)}
            rec.update(read_meminfo())
            idle, total = read_cpu()
            if total > prev_total:
                rec["cpu_busy"] = round(1.0 - (idle - prev_idle) / (total - prev_total), 4)
            prev_idle, prev_total = idle, total
            load = read_optional("/proc/loadavg")
            if load:
                parts = load.split()
                rec["load1"] = float(parts[0])
                rec["running"] = int(parts[3].split("/")[0])
            if cgroup_file:
                raw = read_optional(cgroup_file)
                if raw and raw.strip().isdigit():
                    rec["cgroup_kb"] = int(raw) // 1024
            pressure = read_pressure()
            if pressure:
                rec.update({"psi_" + k: v for k, v in pressure.items()})
            log.write(json.dumps(rec, separators=(",", ":")) + "\n")
            log.flush()
            time.sleep(args.interval)
    return 0


if __name__ == "__main__":
    sys.exit(main())
