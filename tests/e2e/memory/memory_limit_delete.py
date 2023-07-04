import subprocess
import sys
import time

import common
import pytest
from common import MEMORY_LIMIT, THRESHOLD, connect


def test_memgraph_memory_limit_delete(connect):
    memgraph_pid = common.get_memgraph_pid()
    start_time = time.time()
    end_time = start_time + 30

    cursor = connect.cursor()
    # Take a memory sample before the test
    current_memory_usage = common.read_pid_current_memory_in_MB(memgraph_pid)
    memgraph_peak_memory_usage_mb = common.read_pid_peak_memory_in_MB(memgraph_pid)

    # Create nodes until the 95% of the memory limit is reached
    counter = 0
    while current_memory_usage < (MEMORY_LIMIT * 0.95):
        common.execute_and_fetch_all(
            cursor, "FOREACH (i IN range(0, 10000) | CREATE (:Node {id: i})-[:REl]->(:Node {id: i}));"
        )
        current_memory_usage = common.read_pid_current_memory_in_MB(memgraph_pid)
        print(f"Current memory usage of Memgraph: {current_memory_usage} MB new nodes/relationships created!")
        memgraph_peak_memory_usage_mb = common.read_pid_peak_memory_in_MB(memgraph_pid)
        counter += 1

    # Take a memory sample after the 95% of the memory limit is reached
    print(f"Current memory usage of Memgraph: {current_memory_usage} MB after 95% of memory limit reached")
    print(
        f"Current peak memory usage of Memgraph: {memgraph_peak_memory_usage_mb} MB after 95% of memory limit reached"
    )
    common.execute_and_fetch_all(cursor, "FREE MEMORY;")
    print("Memory freed")
    current_memory_usage = common.read_pid_current_memory_in_MB(memgraph_pid)
    memgraph_peak_memory_usage_mb = common.read_pid_peak_memory_in_MB(memgraph_pid)

    # Take a memory sample after the memory is freed
    print(f"Current memory usage of Memgraph after FREE MEMORY: {current_memory_usage} MB")

    # Run memory intensive query
    try:
        print("Running delete query")
        common.execute_and_fetch_all(cursor, "MATCH n DETACH DELETE n;")
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
