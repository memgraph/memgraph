#!/usr/bin/python3 -u

# Copyright 2026 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import argparse
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
from contextlib import closing
from pathlib import Path

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "..", ".."))


def reserve_free_port() -> int:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        return sock.getsockname()[1]


def wait_for_server(proc: subprocess.Popen, port: int, timeout: float = 15.0):
    start = time.time()
    while time.time() - start < timeout:
        if proc.poll() is not None:
            raise RuntimeError("Memgraph exited before startup.")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.25):
                return
        except OSError:
            time.sleep(0.05)
    raise TimeoutError("Memgraph did not start in time.")


def run_crash_test(memgraph_binary: Path, fatal_signal: signal.Signals, expected_signal_number: int):
    with tempfile.TemporaryDirectory() as data_dir:
        bolt_port = reserve_free_port()
        command = [
            str(memgraph_binary),
            "--storage-properties-on-edges",
            "--also-log-to-stderr",
            "--log-level=TRACE",
            f"--bolt-port={bolt_port}",
            f"--data-directory={Path(data_dir)}",
        ]
        proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        wait_for_server(proc, bolt_port)
        proc.send_signal(fatal_signal)
        _, stderr = proc.communicate(timeout=10)

    expected_header = f"Memgraph fatal signal {expected_signal_number} received. Stack trace:"
    if expected_header not in stderr:
        raise AssertionError(f"Expected crash handler header not found in stderr:\n{stderr}")
    if "End of stack trace." not in stderr:
        raise AssertionError(f"Expected crash handler footer not found in stderr:\n{stderr}")
    if proc.returncode != -expected_signal_number:
        raise AssertionError(f"Expected return code {-expected_signal_number}, got {proc.returncode}.")


def main():
    default_memgraph_binary = os.path.join(PROJECT_DIR, "build", "memgraph")
    parser = argparse.ArgumentParser()
    parser.add_argument("--memgraph", default=default_memgraph_binary)
    args = parser.parse_args()

    memgraph_binary = Path(args.memgraph)
    run_crash_test(memgraph_binary, signal.SIGSEGV, signal.SIGSEGV.value)
    print("\033[1;32m~~ SIGSEGV crash handler test successful ~~\033[0m")
    run_crash_test(memgraph_binary, signal.SIGABRT, signal.SIGABRT.value)
    print("\033[1;32m~~ SIGABRT crash handler test successful ~~\033[0m")

    sys.exit(0)


if __name__ == "__main__":
    main()
