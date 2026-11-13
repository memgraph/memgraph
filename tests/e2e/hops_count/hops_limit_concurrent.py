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

"""USING HOPS LIMIT on a vertex whose snapshot deltas must be replayed (concurrent commit).

The delta replay must run on the full edge copy before the hops budget is applied, and a bounded
expansion must neither store into nor read from the many-deltas cache.
"""

import sys

import pytest
from neo4j import GraphDatabase

URI = "bolt://localhost:7687"
AUTH = ("", "")

# v:S with five committed out-edges.
SETUP_FIVE_OUT = "CREATE (v:S) WITH v UNWIND range(1, 5) AS i CREATE (v)-[:E]->(:W {i: i})"

DIRECTIONS = {
    "out": {
        "setup": SETUP_FIVE_OUT,
        "write": "MATCH (v:S) WITH v UNWIND range(1, 3) AS i CREATE (v)-[:E]->(:W {i: 100 + i})",
        "count": "MATCH (v:S)-[r]->(w) RETURN count(*) AS c",
        "bounded": "USING HOPS LIMIT 2 MATCH (v:S)-[r]->(w) RETURN count(*) AS c",
    },
    "in": {
        "setup": "CREATE (v:S) WITH v UNWIND range(1, 5) AS i CREATE (:W {i: i})-[:E]->(v)",
        "write": "MATCH (v:S) WITH v UNWIND range(1, 3) AS i CREATE (:W {i: 100 + i})-[:E]->(v)",
        "count": "MATCH (v:S)<-[r]-(w) RETURN count(*) AS c",
        "bounded": "USING HOPS LIMIT 2 MATCH (v:S)<-[r]-(w) RETURN count(*) AS c",
    },
}


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

    The pre-fix bug kills the instance inside this read, so the client sees the connection drop
    instead of a result. Returning None there lets the test fail on its own assertion - the bounded
    read must produce the snapshot-visible rows - rather than on the client-side exception.
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


@pytest.mark.parametrize("direction", ["out", "in"])
def test_hops_limit_survives_concurrent_commit(direction):
    """Untyped expansion after a concurrent commit: 2 snapshot edges, instance alive."""
    q = DIRECTIONS[direction]
    driver = _new_driver()
    try:
        _reset(driver)
        _write(driver, q["setup"])
        with driver.session() as session_a:
            tx = session_a.begin_transaction()
            assert tx.run(q["count"]).single()["c"] == 5
            _write(driver, q["write"])
            rows, _ = _bounded_read(tx, q["bounded"])
            assert rows == [{"c": 2}], "hops-limited expansion must return the 2 snapshot-visible edges"
            tx.commit()
        assert _probe_alive(driver)
    finally:
        _close(driver)


def test_hops_limit_survives_concurrent_commit_typed():
    """Typed out-expansion after a concurrent commit: 2 snapshot edges and a live instance."""
    q = DIRECTIONS["out"]
    driver = _new_driver()
    try:
        _reset(driver)
        _write(driver, q["setup"])
        with driver.session() as session_a:
            tx = session_a.begin_transaction()
            assert tx.run(q["count"]).single()["c"] == 5
            _write(driver, q["write"])
            rows, metadata = _bounded_read(tx, "USING HOPS LIMIT 2 MATCH (v:S)-[:E]->(w) RETURN count(*) AS c")
            assert rows == [{"c": 2}], "typed hops-limited out-expansion must return the 2 snapshot-visible edges"
            assert metadata is not None and metadata["number_of_hops"] == 2
            tx.commit()
        assert _probe_alive(driver)
    finally:
        _close(driver)


def test_hops_limit_typed_ignores_hidden_edge_of_other_type():
    """A replayed edge of another type must not consume a hop of a typed expansion."""
    driver = _new_driver()
    try:
        _reset(driver)
        _write(driver, "CREATE (v:S) WITH v UNWIND range(1, 3) AS i CREATE (v)-[:E]->(:W {i: i})")
        with driver.session() as session_a:
            tx = session_a.begin_transaction()
            assert tx.run("MATCH (v:S)-[r]->(w) RETURN count(*) AS c").single()["c"] == 3
            _write(driver, "MATCH (v:S)-[r:E]->(:W) WITH r LIMIT 1 DELETE r")
            _write(driver, "MATCH (v:S) CREATE (v)-[:F]->(:W)")
            rows, _ = _bounded_read(tx, "USING HOPS LIMIT 3 MATCH (v:S)-[:E]->(w) RETURN count(*) AS c")
            assert rows == [{"c": 3}], "the hidden :F edge must not consume a hop"
            tx.commit()
        assert _probe_alive(driver)
    finally:
        _close(driver)


def test_hops_limit_no_cache_leak():
    """A bounded expansion neither stores nor reads the many-deltas cache."""
    driver = _new_driver()
    try:
        _reset(driver)
        _write(driver, SETUP_FIVE_OUT)
        with driver.session() as session_a:
            tx = session_a.begin_transaction()
            assert tx.run(DIRECTIONS["out"]["count"]).single()["c"] == 5
            _write(driver, "MATCH (v:S)-[r]->(:W) WITH r LIMIT 2 DELETE r")
            rows, _ = _bounded_read(
                tx, "USING HOPS LIMIT 10 MATCH (v:S)-[r]->() MATCH (v)-[r2]->() RETURN count(*) AS c"
            )
            # First expansion spends 5 hops; the second has 5 left and must not get a cached list.
            assert rows == [{"c": 5}]
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
            assert tx.run("MATCH (n) WHERE n:A OR n:B MATCH (n)-[r]->(w) RETURN count(*) AS c").single()["c"] == 3
            # The only edge of b:B is deleted concurrently; its replayed undo is an addition.
            _write(driver, "MATCH (b:B)-[r]->(:W) DELETE r")
            result = tx.run(
                "USING HOPS LIMIT 2 MATCH (a:A)-[:E]->(x) WITH a, x MATCH (b:B)-[r]->(y) RETURN count(*) AS c"
            )
            # The first expansion spends the whole budget, so b:B contributes no rows.
            assert result.single()["c"] == 0
            tx.commit()
        assert _probe_alive(driver)
    finally:
        _close(driver)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA", *sys.argv[1:]]))
