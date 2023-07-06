import subprocess
import sys
import time

import common
import pytest
from common import MEMORY_LIMIT, THRESHOLD, connect


def test_memgraph_memory_limit_write(connect):
    memgraph_pid = common.get_memgraph_pid()
    common.connect_heap_track_to_memgraph(memgraph_pid)
    start_time = time.time()
    end_time = start_time + 30

    cursor = connect.cursor()
    counter = 0

    # Take a memory sample before the test
    current_memory_usage = common.read_pid_current_memory_in_MB(memgraph_pid)
    memgraph_peak_memory_usage_mb = common.read_pid_peak_memory_in_MB(memgraph_pid)

    # Create nodes until the memory limit is reached (Exception is thrown)
    try:
        while True:
            common.execute_and_fetch_all(cursor, "FOREACH (i IN range(0,100000) | CREATE (:Node {id: i}));")
            current_memory_usage = common.read_pid_current_memory_in_MB(memgraph_pid)
            memgraph_peak_memory_usage_mb = common.read_pid_peak_memory_in_MB(memgraph_pid)
            print(f"Nodes created {(counter * 100000):,} current memory usage of Memgraph: {current_memory_usage} MB")
            print(f"Current peak memory usage of Memgraph: {memgraph_peak_memory_usage_mb} MB")
            counter += 1
            if time.time() > end_time:
                print("Time limit exceeded, breaking loop.", file=sys.stderr)
                break
    except Exception as e:
        print(f"Exception: {e}", file=sys.stderr)
        if "Memory limit exceeded" not in str(e):
            assert False, f"Unexpected exception: {e}, test not valid!"
        pass

    current_memory_usage = common.read_pid_current_memory_in_MB(memgraph_pid)
    memgraph_peak_memory_usage_mb = common.read_pid_peak_memory_in_MB(memgraph_pid)
    MEMORY_LIMIT_WITH_THRESHOLD = MEMORY_LIMIT + (MEMORY_LIMIT * THRESHOLD)
    if memgraph_peak_memory_usage_mb > MEMORY_LIMIT_WITH_THRESHOLD:
        assert False, f"""Memgraph peak memory usage is greater than memory limit {MEMORY_LIMIT} MB + 1%
                            of memory limit threshold, total {MEMORY_LIMIT_WITH_THRESHOLD}. Peak Memgraph
                            memory usage: {memgraph_peak_memory_usage_mb} MB."""
    else:
        print(f"Memory limit not exceeded, peak memory usage: {memgraph_peak_memory_usage_mb} MB")
        assert True


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
