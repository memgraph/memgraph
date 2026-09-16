#!/usr/bin/env python3
"""Write the run's header record to the step log.

The first line of steps.jsonl describes the run (machine, commit, build.sh
arguments, ccache mode) so the file is self-contained: report.py and plots.py
need nothing else to label a run copied from another machine.

    meta.py --log FILE --ccache MODE [-- BUILD_SH_ARGS...]
"""
import argparse
import json
import os
import socket
import subprocess
import sys
import time


def git(*args):
    try:
        return subprocess.run(["git", *args], capture_output=True, text=True, check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return ""


def mem_total_kb():
    with open("/proc/meminfo") as f:
        for line in f:
            if line.startswith("MemTotal:"):
                return int(line.split()[1])
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--log", required=True, help="steps.jsonl to append the record to")
    ap.add_argument("--ccache", required=True, help="how ccache was configured for the run")
    ap.add_argument("build_args", nargs="*", help="arguments given to build.sh")
    args = ap.parse_args()

    rec = {
        "type": "meta",
        "started": round(time.time(), 3),
        "host": socket.gethostname(),
        "cpus": os.cpu_count(),
        "mem_total_kb": mem_total_kb(),
        "kernel": os.uname().release,
        "git_head": git("rev-parse", "--short", "HEAD"),
        "git_dirty": bool(git("status", "--porcelain")),
        "build_args": args.build_args,
        "ccache": args.ccache,
        "cwd": os.getcwd(),
    }
    with open(args.log, "a") as f:
        f.write(json.dumps(rec) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
