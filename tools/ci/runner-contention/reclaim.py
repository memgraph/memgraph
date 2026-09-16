#!/usr/bin/env python3
"""Force the kernel to reclaim clean page cache by briefly allocating anonymous
memory, then releasing it.

MemFree is ~1.4G while buff/cache holds ~49G of reclaimable cache, and the job
monitor watches MemFree, so long runs get killed even though 49G is available.
Touching a large anonymous region makes the kernel drop clean cache to satisfy
it; freeing it afterwards leaves MemFree high. Equivalent to drop_caches without
needing root.
"""
import time


def mem_mb(field):
    with open("/proc/meminfo") as fh:
        for line in fh:
            if line.startswith(field):
                return int(line.split()[1]) // 1024
    return -1


def report(tag):
    print(
        f"{tag:8} free={mem_mb('MemFree:'):6} MB  "
        f"cached={mem_mb('Cached:'):6} MB  avail={mem_mb('MemAvailable:'):6} MB",
        flush=True,
    )


report("before")

CHUNK = 512 * 1024 * 1024  # 512 MiB
TARGET_GB = 24
blocks = []
try:
    for i in range(TARGET_GB * 2):
        b = bytearray(CHUNK)
        b[::4096] = b"\x01" * (len(b) // 4096)  # touch every page
        blocks.append(b)
        if mem_mb("MemAvailable:") < 6000:
            print(f"stopping early at {len(blocks)*0.5:.1f} GiB, avail low", flush=True)
            break
except MemoryError:
    print("MemoryError, stopping", flush=True)

report("peak")
del blocks
time.sleep(2)
report("after")
