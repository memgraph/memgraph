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

"""
E2E tests for per-database memory tracking (db_memory_tracked field in SHOW STORAGE INFO).

Requires a running Memgraph instance with enterprise features enabled (CREATE DATABASE support).
Each test uses a fresh connection and checks db_memory_tracked in SHOW STORAGE INFO.
"""

import re
import sys

import mgclient
import pytest


def connect(database="memgraph"):
    conn = mgclient.connect(host="localhost", port=7687)
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(f"USE DATABASE {database}")
    return conn


def get_storage_info(cursor):
    cursor.execute("SHOW STORAGE INFO")
    return {row[0]: row[1] for row in cursor.fetchall()}


def parse_size_bytes(size_str):
    """Parse a human-readable size string produced by GetReadableSize().

    GetReadableSize() formats as '<value><unit>' with no space, e.g. '0B', '1.50MiB'.
    """
    if not size_str:
        return 0
    m = re.match(r"^(\d+(?:\.\d+)?)\s*(B|KiB|MiB|GiB|TiB)$", size_str.strip())
    if not m:
        return 0
    units = {"B": 1, "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3, "TiB": 1024**4}
    return int(float(m.group(1)) * units[m.group(2)])


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_db_memory_tracked_field_exists():
    """SHOW STORAGE INFO must contain the db_memory_tracked key."""
    conn = connect()
    cursor = conn.cursor()
    info = get_storage_info(cursor)
    assert (
        "db_memory_tracked" in info
    ), f"db_memory_tracked missing from SHOW STORAGE INFO. Got keys: {list(info.keys())}"


def test_db_memory_tracked_is_parseable_size():
    """db_memory_tracked must be a parseable human-readable size string."""
    conn = connect()
    cursor = conn.cursor()
    info = get_storage_info(cursor)
    value = info["db_memory_tracked"]
    assert isinstance(value, str) and len(value) > 0, f"db_memory_tracked should be a non-empty string, got: {value!r}"
    # Should be parseable (e.g. "0 B", "1.50 MiB")
    bytes_val = parse_size_bytes(value)
    assert bytes_val >= 0, f"Could not parse db_memory_tracked value: {value!r}"


def test_db_memory_grows_after_create():
    """After creating many nodes, db_memory_tracked must increase."""
    conn = connect()
    cursor = conn.cursor()

    before = parse_size_bytes(get_storage_info(cursor)["db_memory_tracked"])

    # Create enough nodes to trigger arena extent allocation
    cursor.execute("UNWIND range(1, 10000) AS i CREATE (:Node {id: i})")

    after = parse_size_bytes(get_storage_info(cursor)["db_memory_tracked"])

    assert after > before, f"db_memory_tracked should grow after creating 10k nodes. before={before} after={after}"

    # Cleanup
    cursor.execute("MATCH (n:Node) DETACH DELETE n")


def test_db_memory_tracked_per_database_isolation():
    """
    Per-DB tracking must be isolated: writing to DB1 must not increase DB2's tracker.

    Uses CREATE DATABASE to create a second database (enterprise feature).
    """
    conn = connect()
    cursor = conn.cursor()

    # Create a second database (enterprise feature — skip if not available)
    try:
        cursor.execute("CREATE DATABASE testdb_isolation")
    except mgclient.DatabaseError as e:
        if "already exists" in str(e).lower():
            cursor.execute("USE DATABASE testdb_isolation")
            cursor.execute("MATCH (n) DETACH DELETE n")
            cursor.execute("USE DATABASE memgraph")  # restore to default
        elif "enterprise" in str(e).lower() or "not supported" in str(e).lower():
            pytest.skip("CREATE DATABASE requires enterprise license")
        else:
            raise

    # Get baseline for both DBs (already on memgraph from connect())
    baseline_memgraph = parse_size_bytes(get_storage_info(cursor)["db_memory_tracked"])

    # Switch to testdb and get its baseline
    cursor.execute("USE DATABASE testdb_isolation")
    baseline_testdb = parse_size_bytes(get_storage_info(cursor)["db_memory_tracked"])

    # Write to testdb
    cursor.execute("UNWIND range(1, 5000) AS i CREATE (:IsolationNode {id: i})")
    after_testdb = parse_size_bytes(get_storage_info(cursor)["db_memory_tracked"])

    # Check testdb grew
    assert after_testdb > baseline_testdb, (
        f"testdb_isolation db_memory_tracked should grow after writes. "
        f"baseline={baseline_testdb} after={after_testdb}"
    )

    # Switch back to memgraph and check its tracker did NOT grow significantly
    cursor.execute("USE DATABASE memgraph")
    after_memgraph = parse_size_bytes(get_storage_info(cursor)["db_memory_tracked"])

    # Allow up to 512 KiB noise (background threads, etc.)
    assert after_memgraph <= baseline_memgraph + 512 * 1024, (
        f"memgraph db_memory_tracked should not grow when writing to testdb_isolation. "
        f"baseline={baseline_memgraph} after={after_memgraph}"
    )

    # Cleanup
    try:
        cursor.execute("USE DATABASE testdb_isolation")
        cursor.execute("MATCH (n:IsolationNode) DETACH DELETE n")
        cursor.execute("USE DATABASE memgraph")
        cursor.execute("DROP DATABASE testdb_isolation")
    except mgclient.DatabaseError:
        pass


def test_db_memory_grows_after_edge_create():
    """
    Creating edges must increase db_memory_tracked.

    Each edge is a SkipList node allocated via ArenaAwareAllocator. Additionally, vertex
    adjacency-list small_vectors spill to heap once they exceed inline capacity.
    """
    conn = connect()
    cursor = conn.cursor()

    # Create source and destination nodes first
    cursor.execute("UNWIND range(1, 500) AS i CREATE (:EdgeSrc {id: i})")
    cursor.execute("UNWIND range(1, 500) AS i CREATE (:EdgeDst {id: i})")

    before = parse_size_bytes(get_storage_info(cursor)["db_memory_tracked"])

    # Create edges — each edge is a SkipList entry in the edges_ SkipList
    cursor.execute("MATCH (s:EdgeSrc), (d:EdgeDst) WHERE s.id = d.id CREATE (s)-[:CONNECTS]->(d)")

    after = parse_size_bytes(get_storage_info(cursor)["db_memory_tracked"])

    # Cleanup
    cursor.execute("MATCH (n:EdgeSrc) DETACH DELETE n")
    cursor.execute("MATCH (n:EdgeDst) DETACH DELETE n")

    assert after > before, f"db_memory_tracked should grow after creating 500 edges. before={before} after={after}"


def test_db_memory_grows_after_label_index_create():
    """
    Creating a label index must increase db_memory_tracked.

    A label index is a SkipList<Entry, ArenaAwareAllocator<char>> per label. Creating the
    index and populating it from existing data allocates SkipList nodes in the DB arena.
    """
    conn = connect()
    cursor = conn.cursor()

    cursor.execute("UNWIND range(1, 2000) AS i CREATE (:LabelIdxNode {id: i})")

    before = parse_size_bytes(get_storage_info(cursor)["db_memory_tracked"])

    cursor.execute("CREATE INDEX ON :LabelIdxNode")

    after = parse_size_bytes(get_storage_info(cursor)["db_memory_tracked"])

    # Cleanup
    cursor.execute("DROP INDEX ON :LabelIdxNode")
    cursor.execute("MATCH (n:LabelIdxNode) DETACH DELETE n")

    assert after > before, (
        f"db_memory_tracked should grow after creating label index on 2k nodes. " f"before={before} after={after}"
    )


def test_db_memory_grows_after_label_property_index_create():
    """
    Creating a label+property index must increase db_memory_tracked.

    The label-property index has a SkipList<Entry> per (label, property) pair, populated
    via the async indexer thread (DbAwareThread) which inherits the DB arena idx.
    """
    conn = connect()
    cursor = conn.cursor()

    cursor.execute("UNWIND range(1, 2000) AS i CREATE (:LPIdxNode {val: i})")

    before = parse_size_bytes(get_storage_info(cursor)["db_memory_tracked"])

    cursor.execute("CREATE INDEX ON :LPIdxNode(val)")

    after = parse_size_bytes(get_storage_info(cursor)["db_memory_tracked"])

    # Cleanup
    cursor.execute("DROP INDEX ON :LPIdxNode(val)")
    cursor.execute("MATCH (n:LPIdxNode) DETACH DELETE n")

    assert after > before, (
        f"db_memory_tracked should grow after creating label-property index on 2k nodes. "
        f"before={before} after={after}"
    )


def test_db_memory_grows_after_edge_type_index_create():
    """
    Creating an edge-type index must increase db_memory_tracked.

    The edge-type index has a SkipList<Entry, ArenaAwareAllocator<char>> per edge type.
    """
    conn = connect()
    cursor = conn.cursor()

    cursor.execute("UNWIND range(1, 500) AS i CREATE (:ETIdxSrc {id: i})-[:ET_IDX_REL]->(:ETIdxDst {id: i})")

    before = parse_size_bytes(get_storage_info(cursor)["db_memory_tracked"])

    cursor.execute("CREATE EDGE INDEX ON :ET_IDX_REL")

    after = parse_size_bytes(get_storage_info(cursor)["db_memory_tracked"])

    # Cleanup
    cursor.execute("DROP EDGE INDEX ON :ET_IDX_REL")
    cursor.execute("MATCH (n:ETIdxSrc) DETACH DELETE n")
    cursor.execute("MATCH (n:ETIdxDst) DETACH DELETE n")

    assert after > before, (
        f"db_memory_tracked should grow after creating edge-type index on 500 edges. " f"before={before} after={after}"
    )


def test_db_memory_shrinks_after_gc():
    """
    After deleting nodes and waiting for GC to run, db_memory_tracked must decrease.

    This is the end-to-end proof of the Bug A fix: ArenaMemoryResource::do_deallocate and
    PageAlignedAllocator::deallocate now use ::operator delete (tracked path), so GC
    correctly discharges the transaction tracker and the extent hooks fire the dealloc path.

    GC runs on a 1-second default interval. We sleep briefly then check.
    """
    import time

    conn = connect()
    cursor = conn.cursor()

    cursor.execute("UNWIND range(1, 10000) AS i CREATE (:GcNode {id: i})")
    after_create = parse_size_bytes(get_storage_info(cursor)["db_memory_tracked"])

    cursor.execute("MATCH (n:GcNode) DETACH DELETE n")

    # Wait for GC to collect deleted vertices and free arena extents.
    # Default GC interval is 1s; we wait 3s to be safe.
    time.sleep(3)

    after_gc = parse_size_bytes(get_storage_info(cursor)["db_memory_tracked"])

    # Allow up to 512 KiB residual (SkipList sentinel nodes, jemalloc slab rounding)
    tolerance = 512 * 1024
    assert after_gc <= after_create - (after_create // 2), (
        f"db_memory_tracked should drop significantly after GC collects 10k deleted nodes. "
        f"after_create={after_create} after_gc={after_gc} tolerance={tolerance}"
    )


def test_db_memory_grows_after_trigger_create():
    """
    Creating a trigger must increase db_memory_tracked.

    Trigger entries are stored in SkipList<Trigger, ArenaAwareAllocator<char>>. Creating
    a trigger inserts a SkipList node allocated in the DB arena.
    """
    conn = connect()
    cursor = conn.cursor()

    before = parse_size_bytes(get_storage_info(cursor)["db_memory_tracked"])

    cursor.execute(
        "CREATE TRIGGER mem_track_trigger ON CREATE AFTER COMMIT EXECUTE "
        "UNWIND createdVertices AS v SET v.triggered = true"
    )

    after = parse_size_bytes(get_storage_info(cursor)["db_memory_tracked"])

    # Cleanup
    try:
        cursor.execute("DROP TRIGGER mem_track_trigger")
    except mgclient.DatabaseError:
        pass

    assert after > before, f"db_memory_tracked should grow after creating a trigger. before={before} after={after}"


def test_db_memory_grows_from_trigger_writes():
    """
    Objects created inside an after-commit trigger must be attributed to db_memory_tracked.

    The after-commit trigger pool thread is fully pinned to the DB arena via je_mallctl
    (database.cpp). Any vertex/edge created by the trigger Cypher goes to the DB arena.
    """
    import time

    conn = connect()
    cursor = conn.cursor()

    # Create a trigger that creates a TriggerResult node for every TriggerInput node created
    cursor.execute(
        "CREATE TRIGGER mem_track_write_trigger ON () CREATE AFTER COMMIT EXECUTE "
        "UNWIND createdVertices AS v "
        "WITH v WHERE v:TriggerInput "
        "CREATE (:TriggerResult {src_id: v.id})"
    )

    before = parse_size_bytes(get_storage_info(cursor)["db_memory_tracked"])

    cursor.execute("UNWIND range(1, 1000) AS i CREATE (:TriggerInput {id: i})")

    # Give the after-commit pool thread time to execute the trigger
    time.sleep(1)

    after = parse_size_bytes(get_storage_info(cursor)["db_memory_tracked"])

    # Cleanup
    try:
        cursor.execute("DROP TRIGGER mem_track_write_trigger")
    except mgclient.DatabaseError:
        pass
    cursor.execute("MATCH (n:TriggerInput) DETACH DELETE n")
    cursor.execute("MATCH (n:TriggerResult) DETACH DELETE n")

    assert after > before, (
        f"db_memory_tracked should grow when trigger creates 1k TriggerResult nodes. " f"before={before} after={after}"
    )


def test_db_memory_grows_after_index_population():
    """
    Populating a label-property index on pre-existing data must increase db_memory_tracked.

    Index population runs on DbAwareThread (async_indexer), which inherits the DB arena idx
    via tls_db_arena_idx. SkipList entries for each indexed vertex are allocated in the arena.
    """
    conn = connect()
    cursor = conn.cursor()

    # Create data BEFORE the index exists — population happens on index creation
    cursor.execute("UNWIND range(1, 3000) AS i CREATE (:PopIdxNode {score: i % 100})")

    before = parse_size_bytes(get_storage_info(cursor)["db_memory_tracked"])

    # Creating the index triggers async population of all existing PopIdxNode vertices
    cursor.execute("CREATE INDEX ON :PopIdxNode(score)")

    after = parse_size_bytes(get_storage_info(cursor)["db_memory_tracked"])

    # Cleanup
    cursor.execute("DROP INDEX ON :PopIdxNode(score)")
    cursor.execute("MATCH (n:PopIdxNode) DETACH DELETE n")

    assert after > before, (
        f"db_memory_tracked should grow during index population of 3k pre-existing nodes. "
        f"before={before} after={after}"
    )


def test_db_memory_stable_with_parallel_execution():
    """
    db_memory_tracked must NOT grow when a read query runs with USING PARALLEL EXECUTION.

    Storage-object memory and query-runtime memory are tracked separately. Query execution
    buffers (sort/aggregate) are runtime allocations and should not be attributed to the DB's
    long-lived memory tracker.
    """
    conn = connect()
    cursor = conn.cursor()

    # Seed data so the parallel operators actually allocate (sort/aggregate buffers)
    cursor.execute("UNWIND range(1, 5000) AS i CREATE (:ParNode {val: i})")

    before = parse_size_bytes(get_storage_info(cursor)["db_memory_tracked"])

    # Force parallel execution: ORDER BY triggers OrderByParallelCursor (branches 1..N on
    # worker pool threads), USING PARALLEL EXECUTION is the pre-query directive.
    cursor.execute("USING PARALLEL EXECUTION MATCH (n:ParNode) RETURN n.val ORDER BY n.val")
    cursor.fetchall()  # drain results so the cursor fully executes

    after = parse_size_bytes(get_storage_info(cursor)["db_memory_tracked"])

    # Cleanup
    cursor.execute("MATCH (n:ParNode) DETACH DELETE n")

    # Allow up to 512 KiB noise (background threads, jemalloc slab rounding, etc.)
    tolerance = 512 * 1024
    assert after <= before + tolerance, (
        f"db_memory_tracked should not grow during a parallel read query (runtime allocs are "
        f"tracked separately). before={before} after={after} tolerance={tolerance}"
    )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA", "-v"]))
