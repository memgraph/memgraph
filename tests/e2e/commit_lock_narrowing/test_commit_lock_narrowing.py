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
Regression test for parked commit/main-lock admissions under --experimental-enabled=commit-lock-narrowing.
Needs server flags: --experimental-enabled=commit-lock-narrowing --scheduler=priority_queue
--bolt-num-workers=2 --storage-access-timeout-sec=30 --log-level=TRACE (more client threads than Bolt workers; the
park proof greps the TRACE log). Standalone: MEMGRAPH_BOLT_PORT=<port> python3 -m pytest -x -q <this file>
"""

import sys
import time
from collections.abc import Iterator

import pytest
from helpers import URI, DdlCounter, LogProbe, assert_writes_landed_once, run_clients, start_contender
from neo4j import Driver, GraphDatabase
from neo4j.exceptions import TransientError

MAX_ITERATIONS = 40
NODES_PER_WRITE = 2000
DDL_PAUSE_S = 0.0
CYPHER_BEGIN_N_CLIENTS = 4
CYPHER_BEGIN_MAX_ITERATIONS = 500
CYPHER_BEGIN_MIN_DDL = 1
CYPHER_BEGIN_NODES_PER_WRITE = 500
CYPHER_BEGIN_DDL_PAUSE_S = 0.2
WRITE_QUERY = "UNWIND range(1, $n) AS i CREATE (:N {c: $c, k: $k, i: i})"


@pytest.fixture(autouse=True)
def clean():
    def wipe():
        with GraphDatabase.driver(URI, auth=None) as d, d.session() as s:
            for attempt in range(5):
                try:
                    s.run("MATCH (n:N) DETACH DELETE n").consume()
                    break
                except TransientError:
                    if attempt == 4:
                        raise
                    time.sleep(0.5)
            try:
                s.run("DROP INDEX ON :L(p)").consume()
            except Exception:  # noqa: BLE001 - index may not exist
                pass

    wipe()
    yield
    wipe()


@pytest.fixture
def contender(request: pytest.FixtureRequest) -> Iterator[DdlCounter | None]:
    """Indirect param: None = no contender, else the pause (s) between the contender's index DDL statements."""
    if request.param is None:
        yield None
    else:
        yield from start_contender(request.param)


def assert_landed_once(acked: set[tuple[int, int]], ok: int, nodes: int) -> None:
    assert ok == len(acked) >= 1, f"{ok} acked calls, {len(acked)} distinct"
    assert_writes_landed_once(acked, ok, nodes)


def write_autocommit(d: Driver, i: int, k: int, nodes: int) -> int:
    with d.session() as s:
        return s.run(WRITE_QUERY, n=nodes, c=i, k=k).consume().counters.nodes_created


def write_bolt_begin(d: Driver, i: int, k: int, nodes: int) -> int:
    with d.session() as s, s.begin_transaction() as tx:  # closed (rolled back if uncommitted) on any exit
        created = tx.run(WRITE_QUERY, n=nodes, c=i, k=k).consume().counters.nodes_created
        tx.commit()
        return created


def write_cypher_begin(d: Driver, i: int, k: int, nodes: int) -> int:
    with d.session() as s:
        try:
            s.run("BEGIN").consume()
            created = s.run(WRITE_QUERY, n=nodes, c=i, k=k).consume().counters.nodes_created
            s.run("COMMIT").consume()
            return created
        except Exception:
            try:  # leave the pooled connection outside any transaction
                s.run("ROLLBACK").consume()
            except Exception:  # noqa: BLE001 - no transaction may be open
                pass
            raise


# Every case runs until it has parked at least once (log proof) and, with a contender, overlapped >=1 DDL, so
# contention holds by construction; every attempted write must be acked exactly once, with no error tolerated.
# Without a contender only commit contention can park (2 workers vs many large commits); with one, BEGIN parks.
@pytest.mark.parametrize("contender", [None, DDL_PAUSE_S], indirect=True, ids=["no_ddl", "ddl"])
@pytest.mark.parametrize("write", [write_autocommit, write_bolt_begin], ids=["autocommit", "bolt_begin"])
def test_park_acknowledged(write, contender: DdlCounter | None):
    n_clients = 16 if contender is None else 8
    needle = "commit would block: park" if contender is None else "begin would block: park"
    probe = LogProbe()
    if contender is not None:
        assert contender.first.wait(30), "contender never completed a DDL statement"
    ddl_at_start = 0 if contender is None else contender.ok

    def ddl_during() -> int:
        return 0 if contender is None else contender.ok - ddl_at_start

    acked: set[tuple[int, int]] = set()
    ok = sum(
        run_clients(
            write,
            n_clients,
            MAX_ITERATIONS,
            NODES_PER_WRITE,
            keep_going=lambda: not (probe.seen(needle) and (contender is None or ddl_during() >= 1)),
            acked=acked,
        )
    )
    assert_landed_once(acked, ok, NODES_PER_WRITE)
    assert probe.seen(needle), f"no '{needle}' in the memgraph log: the load never hit the would-block path"
    if contender is not None:
        assert ddl_during() >= 1, "contender completed no DDL during the client phase: no contention"


def is_access_timeout(e: Exception) -> bool:
    return isinstance(e, TransientError) and "Cannot get shared access to the storage" in str(e)


@pytest.mark.parametrize("contender", [CYPHER_BEGIN_DDL_PAUSE_S], indirect=True)
def test_cypher_begin_under_contention(contender: DdlCounter):
    """Cypher-text BEGIN blocks a worker by design, so light load only: any error but the access timeout, and any
    hang, fails (run_clients re-raises everything tolerate() rejects)."""
    # Tolerates the access timeout (master parity): Cypher BEGIN cannot park, so 2 workers can deadlock until it fires.
    assert contender.first.wait(30), "contender never completed a DDL statement"
    ddl_at_start = contender.ok
    acked: set[tuple[int, int]] = set()
    # Clients iterate until the contender has overlapped them, so contention holds by construction.
    # A single pooled connection keeps Cypher BEGIN / write / COMMIT on the same Bolt session.
    done = run_clients(
        write_cypher_begin,
        CYPHER_BEGIN_N_CLIENTS,
        CYPHER_BEGIN_MAX_ITERATIONS,
        CYPHER_BEGIN_NODES_PER_WRITE,
        pool_size=1,
        keep_going=lambda: contender.ok - ddl_at_start < CYPHER_BEGIN_MIN_DDL,
        tolerate=is_access_timeout,
        acked=acked,
    )
    ddl_during = contender.ok - ddl_at_start
    assert sum(done) == len(acked) >= 1, f"{sum(done)} acked calls"
    with GraphDatabase.driver(URI, auth=None) as d, d.session() as s:
        rows = s.run("MATCH (n:N) RETURN n.c AS c, n.k AS k, count(*) AS m").data()
    landed = {(r["c"], r["k"]): r["m"] for r in rows}
    bad = {pair: landed.get(pair) for pair in acked if landed.get(pair) != CYPHER_BEGIN_NODES_PER_WRITE}
    assert not bad, f"acked writes with multiplicity != {CYPHER_BEGIN_NODES_PER_WRITE}: {list(bad.items())[:5]}"
    assert ddl_during >= CYPHER_BEGIN_MIN_DDL, f"only {ddl_during} DDL statements overlapped the clients"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
