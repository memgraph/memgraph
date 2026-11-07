# Copyright 2023 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import atexit

"""
Stall watchdog for a single blocking benchmark client call.

If the guarded call does not return within the timeout, the stacks of the Memgraph server and of the
benchmark client processes are printed and the whole benchmark process exits with a failure, so a CI job
ends with evidence instead of hanging until it is cancelled.
"""

import glob
import os
import shutil
import subprocess
import sys
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Callable, Iterator, Optional

STALL_TIMEOUT_SECONDS = 5 * 60
GDB_TIMEOUT_SECONDS = 120


def _find_gdb() -> Optional[str]:
    gdb = shutil.which("gdb")
    if gdb:
        return gdb
    candidates = sorted(glob.glob("/opt/toolchain-*/bin/gdb"), reverse=True)
    return candidates[0] if candidates else None


def _run(args: list[str], timeout: int = GDB_TIMEOUT_SECONDS) -> str:
    try:
        ret = subprocess.run(args, capture_output=True, text=True, timeout=timeout, check=False)
    except (OSError, subprocess.TimeoutExpired) as e:
        return f"<{args[0]} failed: {e!r}>"
    return ret.stdout + ret.stderr


def _read(path: str) -> str:
    try:
        return Path(path).read_text(errors="replace")
    except OSError as e:
        return f"<{path}: {e.strerror}>"


def _child_pids(parent_pid: int) -> list[int]:
    pids = []
    for stat in glob.glob("/proc/[0-9]*/stat"):
        try:
            fields = Path(stat).read_text().rsplit(")", 1)[1].split()
        except (OSError, IndexError):
            continue
        # After the "(comm)" field: state, ppid, ...
        if int(fields[1]) == parent_pid:
            pids.append(int(stat.split("/")[2]))
    return pids


def _dump_process(label: str, pid: int, gdb: Optional[str]) -> None:
    print(f"==== STALL DUMP: {label} pid={pid} ====", flush=True)
    print(_read(f"/proc/{pid}/cmdline").replace("\0", " "), flush=True)
    print("---- ps -Lp (thread states) ----", flush=True)
    print(_run(["ps", "-Lp", str(pid), "-o", "pid,tid,stat,wchan:32,pcpu,comm"]), flush=True)
    if gdb:
        print("---- gdb thread apply all bt ----", flush=True)
        print(_run([gdb, "-p", str(pid), "-batch", "-ex", "thread apply all bt"]), flush=True)
    elif eu_stack := shutil.which("eu-stack"):
        print("---- eu-stack ----", flush=True)
        print(_run([eu_stack, "-p", str(pid)]), flush=True)
    # Always printed: still works where ptrace is denied. Kernel stacks may need root and then read as empty.
    print("---- /proc/<pid>/task/*: comm, syscall, kernel stack ----", flush=True)
    for task in sorted(glob.glob(f"/proc/{pid}/task/*")):
        print(f"-- tid {os.path.basename(task)}: {_read(task + '/comm').strip()}", flush=True)
        print(f"syscall: {_read(task + '/syscall').strip()}", flush=True)
        print(_read(task + "/stack"), flush=True)


def dump_stacks(server_pid: Optional[int]) -> None:
    gdb = _find_gdb()
    print(f"==== STALL WATCHDOG: dumping stacks, gdb={gdb} ====", flush=True)
    print(_run(["ps", "-eLo", "pid,ppid,tid,stat,wchan:32,pcpu,comm"]), flush=True)
    if server_pid is not None:
        _dump_process("memgraph server", server_pid, gdb)
    for pid in _child_pids(os.getpid()):
        if pid != server_pid:
            _dump_process("benchmark client", pid, gdb)


def _fail(server_pid: Optional[int], what: str, timeout: int) -> None:
    print(f"==== STALL WATCHDOG: '{what}' made no progress for {timeout}s ====", flush=True)
    try:
        dump_stacks(server_pid)
    finally:
        sys.stdout.flush()
        sys.stderr.flush()
        # Exit hard: the main thread is blocked in the client call and the server is left to the CI cleanup.
        os._exit(1)


@contextmanager
def stall_watchdog(
    what: str, server_pid: Callable[[], Optional[int]], timeout: int = STALL_TIMEOUT_SECONDS
) -> Iterator[None]:
    timer = threading.Timer(timeout, lambda: _fail(server_pid(), what, timeout))
    timer.daemon = True
    timer.start()
    try:
        yield
    finally:
        timer.cancel()
