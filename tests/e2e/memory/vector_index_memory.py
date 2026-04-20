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

import os
import sys

import interactive_mg_runner
import mgclient
import pytest
from common import connect, execute_and_fetch_all

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

BOLT_PORT = 7687
DIMENSION = 256
TOLERANCE_MIB = 10.0
INDEX_VECTOR_COUNT = 1000
INDEX_CAPACITY = 50_000

INSTANCE_200MB = {
    "vector_index_test": {
        "args": [
            "--bolt-port",
            str(BOLT_PORT),
            "--memory-limit=50",
            "--storage-gc-cycle-sec=180",
            "--log-level=WARNING",
        ],
        "log_file": "vector-index-memory-e2e.log",
        "setup_queries": [],
        "validation_queries": [],
    }
}


@pytest.fixture(autouse=True)
def cleanup_after_test():
    yield
    interactive_mg_runner.kill_all(keep_directories=False)


def get_storage_info(cursor):
    rows = execute_and_fetch_all(cursor, "SHOW STORAGE INFO")
    return {row[0]: row[1] for row in rows}


def parse_mib(value: str) -> float:
    """Parse a readable size string like '93.21MiB' into MiB as float."""
    value = value.strip()
    if value.endswith("GiB"):
        return float(value[:-3]) * 1024
    if value.endswith("MiB"):
        return float(value[:-3])
    if value.endswith("KiB"):
        return float(value[:-3]) / 1024
    if value.endswith("B"):
        return float(value[:-1]) / (1024 * 1024)
    return 0.0


def insert_vectors(cursor, count, dimension):
    for i in range(count):
        vec = [float(i)] * dimension
        execute_and_fetch_all(
            cursor,
            "CREATE (:Embedding {vec: $vec})",
            {"vec": vec},
        )


def setup_index_and_data(cursor):
    execute_and_fetch_all(
        cursor,
        f"CREATE VECTOR INDEX emb_idx ON :Embedding(vec) "
        f'WITH CONFIG {{"dimension": {DIMENSION}, "capacity": {INDEX_CAPACITY}}}',
    )
    insert_vectors(cursor, INDEX_VECTOR_COUNT, DIMENSION)
    for i in range(INDEX_VECTOR_COUNT):
        execute_and_fetch_all(cursor, "CREATE (:Data {value: $val})", {"val": i})


def test_graph_and_vector_index_tracked_sum_to_total():
    """
    vector_index_memory_tracked + graph_memory_tracked must approximately equal
    memory_tracked (the aggregate of both child trackers).

    Also checks that the RSS–tracked gap observed at startup does not grow
    significantly after inserting vectors, meaning the tracker stays honest
    relative to actual OS memory usage.
    """
    interactive_mg_runner.start_all(INSTANCE_200MB)
    cursor = connect(host="localhost", port=BOLT_PORT).cursor()

    baseline = get_storage_info(cursor)
    baseline_gap = parse_mib(baseline["memory_res"]) - parse_mib(baseline["global_memory_tracked"])
    execute_and_fetch_all(
        cursor,
        f"CREATE VECTOR INDEX emb_idx ON :Embedding(vec) "
        f'WITH CONFIG {{"dimension": {DIMENSION}, "capacity": {INDEX_CAPACITY}}}',
    )

    insert_vectors(cursor, INDEX_VECTOR_COUNT, DIMENSION)

    info = get_storage_info(cursor)
    total = parse_mib(info["global_memory_tracked"])
    graph = parse_mib(info["graph_memory_tracked"])
    vector_index = parse_mib(info["vector_index_memory_tracked"])
    rss = parse_mib(info["memory_res"])

    assert vector_index > 0, "vector_index_memory_tracked should be non-zero after vector insertions"
    assert graph > 0, "graph_memory_tracked should be non-zero"

    assert abs((graph + vector_index) - total) < TOLERANCE_MIB, (
        f"graph ({graph:.2f} MiB) + vector_index ({vector_index:.2f} MiB) = {graph + vector_index:.2f} MiB "
        f"but memory_tracked = {total:.2f} MiB (diff > {TOLERANCE_MIB} MiB)"
    )

    post_gap = rss - total
    assert abs(post_gap - baseline_gap) < TOLERANCE_MIB, (
        f"RSS–tracked gap changed too much: baseline={baseline_gap:.2f} MiB, "
        f"after inserts={post_gap:.2f} MiB (diff > {TOLERANCE_MIB} MiB)"
    )


def test_vector_insert_oom_throws_exception():
    """
    When the global memory limit is exceeded during vector insertion, Memgraph
    must raise an OutOfMemoryException to the client.
    """
    interactive_mg_runner.start_all(INSTANCE_200MB)
    cursor = connect(host="localhost", port=BOLT_PORT).cursor()

    execute_and_fetch_all(
        cursor,
        f"CREATE VECTOR INDEX emb_idx ON :Embedding(vec) "
        f'WITH CONFIG {{"dimension": {DIMENSION}, "capacity": {INDEX_CAPACITY}}}',
    )

    oom_raised = False
    try:
        insert_vectors(cursor, INDEX_CAPACITY * 10, DIMENSION)
    except mgclient.DatabaseError as e:
        assert "Memory limit exceeded" in str(e), f"Expected 'Memory limit exceeded' but got: {e}"
        oom_raised = True

    assert oom_raised, "Expected an OutOfMemoryException to be raised during vector insertion, but it was not."


def test_remove_vector_property_vector_index_unchanged():
    """
    Removing the vector property from vertices does not free vector index memory
    because usearch's bump-pointer allocator cannot free individual entries.
    """
    interactive_mg_runner.start_all(INSTANCE_200MB)
    cursor = connect(host="localhost", port=BOLT_PORT).cursor()
    setup_index_and_data(cursor)

    vi_before = parse_mib(get_storage_info(cursor)["vector_index_memory_tracked"])
    assert vi_before > 0, "vector_index_memory_tracked should be non-zero after setup"

    execute_and_fetch_all(cursor, "MATCH (n:Embedding) REMOVE n.vec")

    vi_after = parse_mib(get_storage_info(cursor)["vector_index_memory_tracked"])
    assert vi_after == vi_before, (
        f"vector_index_memory_tracked changed after removing vec property: "
        f"before={vi_before:.2f} MiB, after={vi_after:.2f} MiB"
    )


def test_delete_vertices_graph_down_vector_index_unchanged():
    """
    Deleting whole vertices frees graph memory (vertex objects) but not
    vector index memory (usearch arena stays allocated).
    """
    interactive_mg_runner.start_all(INSTANCE_200MB)
    cursor = connect(host="localhost", port=BOLT_PORT).cursor()
    setup_index_and_data(cursor)

    info_before = get_storage_info(cursor)
    graph_before = parse_mib(info_before["graph_memory_tracked"])
    vi_before = parse_mib(info_before["vector_index_memory_tracked"])

    execute_and_fetch_all(cursor, "MATCH (n:Embedding) DETACH DELETE n")
    execute_and_fetch_all(cursor, "FREE MEMORY")

    info_after = get_storage_info(cursor)
    graph_after = parse_mib(info_after["graph_memory_tracked"])
    vi_after = parse_mib(info_after["vector_index_memory_tracked"])

    assert vi_after == vi_before, (
        f"vector_index_memory_tracked changed after deleting vertices: "
        f"before={vi_before:.2f} MiB, after={vi_after:.2f} MiB"
    )
    assert graph_after < graph_before, (
        f"graph_memory_tracked should decrease after deleting vertices: "
        f"before={graph_before:.2f} MiB, after={graph_after:.2f} MiB"
    )


def test_drop_index_vector_index_zero():
    """
    Dropping the vector index destroys the usearch index object, which triggers
    TrackedVectorAllocator::deallocate() → reset() → vector_index_memory_tracker.Free().
    Vector index memory should go to 0.
    """
    interactive_mg_runner.start_all(INSTANCE_200MB)
    cursor = connect(host="localhost", port=BOLT_PORT).cursor()
    setup_index_and_data(cursor)

    vi_before = parse_mib(get_storage_info(cursor)["vector_index_memory_tracked"])
    assert vi_before > 0, "vector_index_memory_tracked should be non-zero before drop"

    execute_and_fetch_all(cursor, "DROP VECTOR INDEX emb_idx")

    vi_after = parse_mib(get_storage_info(cursor)["vector_index_memory_tracked"])
    assert vi_after < 1.0, f"vector_index_memory_tracked should be ~0 after dropping index, got {vi_after:.2f} MiB"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA", "-v"]))
