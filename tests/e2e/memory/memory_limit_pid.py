import subprocess
import sys
import time

import pytest
from common import connect, execute_and_fetch_all

MEMORY_LIMIT = 1024  # MB
THRESHOLD = 0.01  # 1% of memory limit


def test_memgraph_memory_control_via_pid(connect):
    memgraph_pid = subprocess.check_output(["pgrep", "memgraph"]).decode("utf-8").strip()
    start_time = time.time()
    end_time = start_time + 30

    cursor = connect.cursor()
    counter = 0
    try:
        while True:
            command = f"ps -p {memgraph_pid} -o rss="
            output = subprocess.check_output(command, shell=True)
            current_memory_usage = int(output.decode("utf-8").strip()) / 1024
            print(
                f"Nodes created {counter * 10000} Current memory usage of process {memgraph_pid} (memgraph): {current_memory_usage} MB"
            )
            execute_and_fetch_all(cursor, "FOREACH (i IN range(0,10000) | CREATE (:Node {id: i}));")
            counter += 1
            if time.time() > end_time:
                print("Time limit exceeded, breaking loop.", file=sys.stderr)
                break
    except Exception as e:
        print(f"Exception: {e}", file=sys.stderr)
        pass

    command = f"grep ^VmPeak /proc/{memgraph_pid}/status"
    output = subprocess.check_output(command, shell=True).decode("utf-8").strip()
    process_peak_memory = output.split(":")[1].strip().split(" ")[0]
    memgraph_peak_memory_usage_mb = int(process_peak_memory) / 1024
    MEMORY_LIMIT_WITH_THRESHOLD = MEMORY_LIMIT + (MEMORY_LIMIT * THRESHOLD)
    if memgraph_peak_memory_usage_mb > MEMORY_LIMIT_WITH_THRESHOLD:
        assert False, f"""Memgraph peak memory usage is greater than memory limit {MEMORY_LIMIT} MB + 1%
                            of memory limit threshold, total {MEMORY_LIMIT_WITH_THRESHOLD}. Peak Memgraph
                            memory usage: {memgraph_peak_memory_usage_mb} MB."""
    else:
        assert True


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
