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

import subprocess
import sys
import time
import typing

import mgclient
import pytest

MEMORY_LIMIT = 1024  # MB check if workloads.yaml --memory-limit is properly configured.
THRESHOLD = 0.01  # 1% of memory limit, defines the acceptable overdrive threshold for memory usage.


def get_memgraph_pid() -> str:
    command = f"pgrep memgraph"
    try:
        output = subprocess.check_output(command, shell=True).decode("utf-8").strip()
        output = output.split("\n")
        if len(output) > 1:
            print(f"Multiple Memgraph processes running: {output}", file=sys.stderr)
            exit(1)
    except subprocess.CalledProcessError as e:
        print(f"Exception: {e}", file=sys.stderr)
        exit(1)
    return output[0]


def read_pid_peak_memory_in_MB(pid: str) -> int:
    command = f"grep ^VmHWM /proc/{pid}/status"
    output = subprocess.check_output(command, shell=True).decode("utf-8").strip()
    process_peak_memory = output.split(":")[1].strip().split(" ")[0]
    return int(process_peak_memory) / 1024


def read_pid_current_memory_in_MB(pid: str) -> int:
    command = f"ps -p {pid} -o rss="
    output = subprocess.check_output(command, shell=True)
    current_memory_usage = int(output.decode("utf-8").strip()) / 1024
    return current_memory_usage


def execute_and_fetch_all(cursor: mgclient.Cursor, query: str, params: dict = {}) -> typing.List[tuple]:
    cursor.execute(query, params)
    return cursor.fetchall()


@pytest.fixture
def connect(**kwargs) -> mgclient.Connection:
    connection = mgclient.connect(host="localhost", port=7687, **kwargs)
    connection.autocommit = True
    return connection
