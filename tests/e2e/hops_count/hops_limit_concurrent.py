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

"""USING HOPS LIMIT combined with MVCC delta replay on the expanded vertex.

A hops-limited edge expansion must replay the vertex's snapshot deltas on the full
edge copy and only then apply the hops budget. Applying the budget before the replay
truncates the list the replay mutates: an undone concurrent edge creation is searched
for in a list that no longer holds it (SIGSEGV in release, DMG_ASSERT in debug), an
undone deletion is appended past the budget, and a truncated list may leak into the
per-transaction many-deltas cache consulted by later, unbounded reads.

Each test starts from an empty instance (the e2e runner gives every workload its own
instance) and asserts the snapshot-visible result of the bounded read.
"""

import sys

import pytest
from neo4j import GraphDatabase

URI = "bolt://localhost:7687"
AUTH = ("", "")

# v:S with five committed out-edges.
SETUP_FIVE_OUT = (
    "CREATE (v:S) WITH v UNWIND range(1, 5) AS i CREATE (v)-[:E]->(:W {i: i})"
)
# v:Q with five committed out-edges, used by the own-transaction delta test.
SETUP_FIVE_OUT_Q = (
    "CREATE (v:Q) WITH v UNWIND range(1, 5) AS i CREATE (v)-[:E]->(:Z {i: i})"
)
# v:S with five committed in-edges.
SETUP_FIVE_IN = (
    "CREATE (v:S) WITH v UNWIND range(1, 5) AS i CREATE (:W {i: i})-[:E]->(v)"
)
# Three concurrently committed out-edges of v:S (one REMOVE_OUT_EDGE undo delta each).
CONCURRENT_CREATE_OUT = (
    "MATCH (v:S) WITH v UNWIND range(1, 3) AS i CREATE (v)-[:E]->(:W {i: 100 + i})"
)
# Three concurrently committed in-edges of v:S.
CONCURRENT_CREATE_IN = (
    "MATCH (v:S) WITH v UNWIND range(1, 3) AS i CREATE (:W {i: 100 + i})-[:E]->(v)"
)

COUNT_OUT = "MATCH (v:S)-[r]->(w) RETURN count(*) AS c"
COUNT_IN = "MATCH (v:S)<-[r]-(w) RETURN count(*) AS c"
HOPS_OUT = "USING HOPS LIMIT 2 MATCH (v:S)-[r]->(w) RETURN count(*) AS c"
HOPS_IN = "USING HOPS LIMIT 2 MATCH (v:S)<-[r]-(w) RETURN count(*) AS c"
HOPS_TYPED_OUT = "USING HOPS LIMIT 2 MATCH (v:S)-[:E]->(w) RETURN count(*) AS c"


def _new_driver():
    return GraphDatabase.driver(URI, auth=AUTH)


def _reset(driver):
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n").consume()


def _write(driver, query):
    """Autocommit write on its own session, so it is a separate committed transaction."""
    with driver.session() as session:
        session.run(query).consume()


def _bounded_read(tx, query):
    """Run a bounded read and return (rows, metadata).

    The pre-fix bug kills the instance inside this read, so returning None there lets the test
    fail on its own assertion (snapshot-visible rows) instead of on the client-side exception.
    """
    try:
        result = tx.run(query)
        rows = result.data()
        return rows, result.consume().metadata
    except Exception:  # noqa: BLE001 - the bug under test drops the connection
        return None, None


def _probe_alive(driver):
    """A follow-up query proves the instance survived the bounded read."""
    with driver.session() as session:
        return session.run("RETURN 1 AS one").single()["one"] == 1


def _close(driver):
    try:
        driver.close()
    except Exception:  # noqa: BLE001 - a crashed instance can fail the close itself
        pass


def test_hops_limit_survives_concurrent_commit_out():
    """Untyped out-expansion after a concurrent commit: 2 snapshot edges, instance alive."""
    driver = _new_driver()
    try:
        _reset(driver)
        _write(driver, SETUP_FIVE_OUT)
        with driver.session() as session_a:
            tx = session_a.begin_transaction()
            assert tx.run(COUNT_OUT).single()["c"] == 5
            _write(driver, CONCURRENT_CREATE_OUT)
            rows, _ = _bounded_read(tx, HOPS_OUT)
            assert rows == [{"c": 2}], "hops-limited out-expansion must return the 2 snapshot-visible edges"
            tx.commit()
        assert _probe_alive(driver)
    finally:
        _close(driver)


def test_hops_limit_survives_concurrent_commit_typed():
    """Typed out-expansion after a concurrent commit: 2 snapshot edges and a live instance."""
    driver = _new_driver()
    try:
        _reset(driver)
        _write(driver, SETUP_FIVE_OUT)
        with driver.session() as session_a:
            tx = session_a.begin_transaction()
            assert tx.run(COUNT_OUT).single()["c"] == 5
            _write(driver, CONCURRENT_CREATE_OUT)
            rows, metadata = _bounded_read(tx, HOPS_TYPED_OUT)
            assert rows == [{"c": 2}], "typed hops-limited out-expansion must return the 2 snapshot-visible edges"
            # Post-replay traversal count, not the size of the truncated result list.
            assert metadata is not None and metadata["number_of_hops"] >= 2
            tx.commit()
        assert _probe_alive(driver)
    finally:
        _close(driver)


def test_hops_limit_survives_concurrent_commit_in():
    """In-expansion after a concurrent commit: 2 snapshot edges, instance alive."""
    driver = _new_driver()
    try:
        _reset(driver)
        _write(driver, SETUP_FIVE_IN)
        with driver.session() as session_a:
            tx = session_a.begin_transaction()
            assert tx.run(COUNT_IN).single()["c"] == 5
            _write(driver, CONCURRENT_CREATE_IN)
            rows, _ = _bounded_read(tx, HOPS_IN)
            assert rows == [{"c": 2}], "hops-limited in-expansion must return the 2 snapshot-visible edges"
            tx.commit()
        assert _probe_alive(driver)
    finally:
        _close(driver)


def test_hops_limit_own_command_delta():
    """Same-statement created edges replayed under the budget still yield 2 rows."""
    driver = _new_driver()
    try:
        _reset(driver)
        _write(driver, SETUP_FIVE_OUT_Q)
        # A variable-length pattern reads the old view, so the edges created by this
        # command come back as undo deltas on the vertex the expansion reads.
        with driver.session() as session:
            result = session.run(
                "USING HOPS LIMIT 2 MATCH (v:Q) "
                "CREATE (v)-[:E]->(:Z {i: 101}), (v)-[:E]->(:Z {i: 102}), (v)-[:E]->(:Z {i: 103}) "
                "WITH v MATCH (v)-[:E*1]->(z) RETURN count(*) AS c"
            )
            assert result.single()["c"] == 2
        assert _probe_alive(driver)
    finally:
        _close(driver)


def test_hops_limit_no_cache_store():
    """A bounded out-expansion must not leave its list in the many-deltas cache."""
    driver = _new_driver()
    try:
        _reset(driver)
        _write(driver, SETUP_FIVE_OUT)
        with driver.session() as session_a:
            tx = session_a.begin_transaction()
            assert tx.run(COUNT_OUT).single()["c"] == 5
            # Two concurrent deletions leave two ADD_OUT_EDGE undo deltas on v:S; with the
            # --delta-chain-cache-threshold=2 workload config the bounded expansion would cache
            # its list, which the second expansion of v:S would then wrongly be served.
            _write(driver, "MATCH (v:S)-[r]->(:W) WITH r LIMIT 2 DELETE r")
            result = tx.run(
                "USING HOPS LIMIT 2 "
                "MATCH (v:S)-[r]->(w) "
                "MATCH (v)-[r2]->(w2) RETURN count(*) AS c"
            )
            assert result.single()["c"] == 0
            tx.commit()
        assert _probe_alive(driver)
    finally:
        _close(driver)


def test_hops_limit_no_cache_hit_bypass():
    """A bounded in-expansion must not read the cache the previous expansion filled."""
    driver = _new_driver()
    try:
        _reset(driver)
        _write(driver, SETUP_FIVE_IN)
        with driver.session() as session_a:
            tx = session_a.begin_transaction()
            assert tx.run(COUNT_IN).single()["c"] == 5
            # Same shape on the in-edge cache: two concurrent deletions leave replayed
            # additions, and the second expansion must not see the first one's list.
            _write(driver, "MATCH (v:S)<-[r]-(:W) WITH r LIMIT 2 DELETE r")
            result = tx.run(
                "USING HOPS LIMIT 2 "
                "MATCH (v:S)<-[r]-(w) "
                "MATCH (v)<-[r2]-(w2) RETURN count(*) AS c"
            )
            assert result.single()["c"] == 0
            tx.commit()
        assert _probe_alive(driver)
    finally:
        _close(driver)


def test_hops_limit_budget_exhausted_remove():
    """An exhausted budget must not let a replayed deletion grow the result list."""
    driver = _new_driver()
    try:
        _reset(driver)
        _write(driver, "CREATE (a:A) WITH a UNWIND range(1, 2) AS i CREATE (a)-[:E]->(:W {i: i})")
        _write(driver, "CREATE (b:B) WITH b CREATE (b)-[:E]->(:W {i: 99})")
        with driver.session() as session_a:
            tx = session_a.begin_transaction()
            assert (
                tx.run("MATCH (n) WHERE n:A OR n:B MATCH (n)-[r]->(w) RETURN count(*) AS c").single()["c"] == 3
            )
            # The only edge of b:B is deleted concurrently; its replayed undo is an addition.
            _write(driver, "MATCH (b:B)-[r]->(:W) DELETE r")
            result = tx.run(
                "USING HOPS LIMIT 2 "
                "MATCH (a:A)-[:E]->(x) WITH a, x "
                "MATCH (b:B)-[r]->(y) RETURN count(*) AS c"
            )
            # The first expansion spends the whole budget, so b:B contributes no rows.
            assert result.single()["c"] == 0
            tx.commit()
        assert _probe_alive(driver)
    finally:
        _close(driver)


def test_hops_limit_disk_reads_survive():
    """On-disk storage keeps its early return: the bounded read stays correct and alive."""
    driver = _new_driver()
    try:
        _reset(driver)
        _write(driver, "STORAGE MODE ON_DISK_TRANSACTIONAL")
        _write(driver, SETUP_FIVE_OUT)
        with driver.session() as session_a:
            tx = session_a.begin_transaction()
            assert tx.run(COUNT_OUT).single()["c"] == 5
            _write(driver, CONCURRENT_CREATE_OUT)
            assert tx.run(HOPS_OUT).single()["c"] == 2
            tx.commit()
        assert _probe_alive(driver)
    finally:
        _close(driver)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA", *sys.argv[1:]]))
