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
--bolt-num-workers=2 --storage-access-timeout-sec=30 (more client threads than Bolt workers). Standalone: MEMGRAPH_BOLT_PORT=<port> python3 -m pytest -x -q <this file>
"""

import os
import sys
import threading
import time
from collections.abc import Callable

import pytest
from neo4j import Driver, GraphDatabase
from neo4j.exceptions import TransientError

URI = f"bolt://127.0.0.1:{os.getenv('MEMGRAPH_BOLT_PORT', '7687')}"
CALL_TIMEOUT_S = 60.0
N_CLIENTS = 16
ITERATIONS = 5
NODES_PER_WRITE = 2000
BEGIN_N_CLIENTS = 8
BEGIN_ITERATIONS = 20
BEGIN_NODES_PER_WRITE = 2000
DDL_PAUSE_S = 0.0
CYPHER_BEGIN_N_CLIENTS = 4
CYPHER_BEGIN_MAX_ITERATIONS = 50
CYPHER_BEGIN_MIN_DDL = 1
CYPHER_BEGIN_NODES_PER_WRITE = 500
CYPHER_BEGIN_DDL_PAUSE_S = 0.2
WRITE_QUERY = "UNWIND range(1, $n) AS i CREATE (:N {c: $c, k: $k, i: i})"

# (driver, client index, iteration, nodes) -> nodes_created reported by the write's summary
WriteFn = Callable[[Driver, int, int, int], int]


def run_bounded(fn: Callable[[], object], timeout: float = CALL_TIMEOUT_S) -> object:
    """Run fn in a daemon thread; fail (instead of hanging CI) if it does not return in time."""
    out: dict = {}

    def target() -> None:
        try:
            out["value"] = fn()
        except Exception as e:  # noqa: BLE001 - reported to the caller
            out["error"] = e

    t = threading.Thread(target=target, daemon=True)
    t.start()
    t.join(timeout)
    assert not t.is_alive(), f"call did not return within {timeout}s (hung client / lost acknowledgement)"
    if "error" in out:
        raise out["error"]
    return out.get("value")


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


class DdlCounter:
    """Successful contender DDL statements; written by the contender thread only."""

    ok = 0
    first = threading.Event()  # set after the first successful DDL


def start_contender(pause_s: float):
    """Back-to-back (pause_s apart) index DDL, i.e. unique access, to keep main_lock_ contended; yields the counter."""
    stop = threading.Event()
    counter = DdlCounter()

    def loop() -> None:
        with GraphDatabase.driver(URI, auth=None) as d:
            while not stop.is_set():
                for q in ("CREATE INDEX ON :L(p)", "DROP INDEX ON :L(p)"):
                    try:
                        with d.session() as s:
                            s.run(q).consume()
                        counter.ok += 1
                        counter.first.set()
                    except Exception:  # noqa: BLE001 - DDL races are expected; contention is asserted via the counter
                        pass
                    stop.wait(pause_s)

    t = threading.Thread(target=loop, daemon=True)
    t.start()
    yield counter
    stop.set()
    t.join(CALL_TIMEOUT_S)


@pytest.fixture
def main_lock_contender():
    yield from start_contender(DDL_PAUSE_S)


@pytest.fixture
def light_main_lock_contender():
    yield from start_contender(CYPHER_BEGIN_DDL_PAUSE_S)


def run_clients(
    write: WriteFn,
    n_clients: int,
    iterations: int,
    nodes: int,
    pool_size: int | None = None,
    keep_going: Callable[[], bool] = lambda: True,
    tolerate: Callable[[Exception], bool] = lambda e: False,
    acked: set[tuple[int, int]] | None = None,
) -> list[int]:
    """Each client does bounded writes, at most `iterations`, stopping early once keep_going() is False (after at
    least one). Any error or hang fails, except errors accepted by tolerate() (counted as not-ok). Acked (c,k) pairs
    are added to `acked` if given. Returns the successful call count per client."""
    done: list[int] = [0] * n_clients
    errors: list[str] = []
    pool = {} if pool_size is None else {"max_connection_pool_size": pool_size}

    def client(i: int) -> None:
        try:
            with GraphDatabase.driver(URI, auth=None, max_transaction_retry_time=0, **pool) as d:
                for k in range(iterations):
                    if k > 0 and not keep_going():
                        break
                    try:
                        created = run_bounded(lambda: write(d, i, k, nodes))
                    except Exception as e:  # noqa: BLE001
                        if tolerate(e):
                            continue
                        raise
                    assert created == nodes, f"summary reported {created} created nodes (client {i}, iteration {k})"
                    done[i] += 1
                    if acked is not None:
                        acked.add((i, k))
        except Exception as e:  # noqa: BLE001
            errors.append(repr(e))

    threads = [threading.Thread(target=client, args=(i,), daemon=True) for i in range(n_clients)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(CALL_TIMEOUT_S * iterations)
    assert not any(t.is_alive() for t in threads), "client thread hung"
    assert not errors, f"client errors: {errors[:5]}"
    return done


def assert_writes_landed_once(pairs: set[tuple[int, int]], ok: int, nodes: int) -> None:
    with GraphDatabase.driver(URI, auth=None) as d, d.session() as s:
        rows = s.run("MATCH (n:N) RETURN n.c AS c, n.k AS k, count(*) AS m").data()
    assert {(r["c"], r["k"]) for r in rows} == pairs
    assert len(rows) == ok
    assert all(r["m"] == nodes for r in rows), f"per-(c,k) multiplicity != {nodes}: {rows[:5]}"


def assert_every_write_landed_once(ok: int, n_clients: int, iterations: int, nodes: int) -> None:
    assert_writes_landed_once({(c, k) for c in range(n_clients) for k in range(iterations)}, ok, nodes)


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


@pytest.mark.parametrize("mode", ["autocommit", "explicit"])
def test_commit_park_acknowledged(mode: str):
    write = write_autocommit if mode == "autocommit" else write_bolt_begin
    ok = sum(run_clients(write, N_CLIENTS, ITERATIONS, NODES_PER_WRITE))
    assert ok == N_CLIENTS * ITERATIONS
    assert_every_write_landed_once(ok, N_CLIENTS, ITERATIONS, NODES_PER_WRITE)


@pytest.mark.parametrize("begin", ["bolt_begin", "autocommit_run"])
def test_begin_park_acknowledged(begin: str, main_lock_contender: DdlCounter):
    """BEGIN / RUN-first may park on main_lock_; strict: no storage-access error is tolerated."""
    # On master / flag OFF the same load can starve too (both Bolt workers blocked in BEGIN); the park path must not.
    assert main_lock_contender.first.wait(30), "contender never completed a DDL statement"
    write = write_bolt_begin if begin == "bolt_begin" else write_autocommit
    ddl_at_start = main_lock_contender.ok
    ok = sum(run_clients(write, BEGIN_N_CLIENTS, BEGIN_ITERATIONS, BEGIN_NODES_PER_WRITE))
    ddl_during = main_lock_contender.ok - ddl_at_start
    assert ok == BEGIN_N_CLIENTS * BEGIN_ITERATIONS
    assert_every_write_landed_once(ok, BEGIN_N_CLIENTS, BEGIN_ITERATIONS, BEGIN_NODES_PER_WRITE)
    assert ddl_during >= 1, "contender completed no DDL during the client phase: no contention"


def is_access_timeout(e: Exception) -> bool:
    return isinstance(e, TransientError) and "Cannot get shared access to the storage" in str(e)


def test_cypher_begin_under_contention(light_main_lock_contender: DdlCounter):
    """Cypher-text BEGIN blocks a worker by design, so light load only: other errors and hangs still fail."""
    # Tolerates the access timeout (master parity): Cypher BEGIN cannot park, so 2 workers can deadlock until it fires.
    assert light_main_lock_contender.first.wait(30), "contender never completed a DDL statement"
    ddl_at_start = light_main_lock_contender.ok
    acked: set[tuple[int, int]] = set()
    # Clients iterate until the contender has overlapped them, so contention holds by construction.
    # A single pooled connection keeps Cypher BEGIN / write / COMMIT on the same Bolt session.
    done = run_clients(
        write_cypher_begin,
        CYPHER_BEGIN_N_CLIENTS,
        CYPHER_BEGIN_MAX_ITERATIONS,
        CYPHER_BEGIN_NODES_PER_WRITE,
        pool_size=1,
        keep_going=lambda: light_main_lock_contender.ok - ddl_at_start < CYPHER_BEGIN_MIN_DDL,
        tolerate=is_access_timeout,
        acked=acked,
    )
    ddl_during = light_main_lock_contender.ok - ddl_at_start
    assert sum(done) == len(acked) >= 1, f"{sum(done)} acked calls"
    with GraphDatabase.driver(URI, auth=None) as d, d.session() as s:
        rows = s.run("MATCH (n:N) RETURN n.c AS c, n.k AS k, count(*) AS m").data()
    landed = {(r["c"], r["k"]): r["m"] for r in rows}
    bad = {pair: landed.get(pair) for pair in acked if landed.get(pair) != CYPHER_BEGIN_NODES_PER_WRITE}
    assert not bad, f"acked writes with multiplicity != {CYPHER_BEGIN_NODES_PER_WRITE}: {list(bad.items())[:5]}"
    assert ddl_during >= CYPHER_BEGIN_MIN_DDL, f"only {ddl_during} DDL statements overlapped the clients"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
