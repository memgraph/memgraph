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
from typing import Any

import pytest
from neo4j import GraphDatabase
from neo4j.exceptions import TransientError

URI = f"bolt://127.0.0.1:{os.getenv('MEMGRAPH_BOLT_PORT', '7687')}"
CALL_TIMEOUT_S = 60.0
N_CLIENTS = 16
ITERATIONS = 5
NODES_PER_WRITE = 2000
BEGIN_N_CLIENTS = 4
BEGIN_ITERATIONS = 3
BEGIN_NODES_PER_WRITE = 500
DDL_PAUSE_S = 0.2
ACCESS_TIMEOUT_MSG = "Cannot get shared access to the storage"
WRITE_QUERY = "UNWIND range(1, $n) AS i CREATE (:N {c: $c, k: $k, i: i})"


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


def count_nodes() -> int:
    with GraphDatabase.driver(URI, auth=None) as d, d.session() as s:
        return s.run("MATCH (n:N) RETURN count(n) AS c").single()["c"]


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
def main_lock_contender():
    """Periodically takes unique access (index DDL, throttled) to create main_lock_ contention windows."""
    stop = threading.Event()

    def loop() -> None:
        with GraphDatabase.driver(URI, auth=None) as d:
            while not stop.is_set():
                for q in ("CREATE INDEX ON :L(p)", "DROP INDEX ON :L(p)"):
                    try:
                        with d.session() as s:
                            s.run(q).consume()
                    except Exception:  # noqa: BLE001 - DDL races are expected and irrelevant here
                        pass
                    stop.wait(DDL_PAUSE_S)

    t = threading.Thread(target=loop, daemon=True)
    t.start()
    yield
    stop.set()
    t.join(CALL_TIMEOUT_S)


def tolerate_access_timeout(body: Callable[[], None], cleanup: Callable[[], None] | None = None) -> bool:
    """Run body; True on success, False on a storage-access TransientError. Anything else propagates."""
    try:
        body()
        return True
    except TransientError as e:
        if ACCESS_TIMEOUT_MSG not in str(e):
            raise
        if cleanup is not None:
            cleanup()
        return False


def run_clients(client_fn: Callable[[int], int], n_clients: int = N_CLIENTS, iterations: int = ITERATIONS) -> int:
    """Run n_clients threads of client_fn(i) -> successful writes; every thread is bounded."""
    results: list = [None] * n_clients
    errors: list = []

    def target(i: int) -> None:
        try:
            results[i] = client_fn(i)
        except Exception as e:  # noqa: BLE001
            errors.append(repr(e))

    threads = [threading.Thread(target=target, args=(i,), daemon=True) for i in range(n_clients)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(CALL_TIMEOUT_S * iterations)
    assert not any(t.is_alive() for t in threads), "client thread hung"
    assert not errors, f"client errors: {errors[:5]}"
    return sum(results)


def test_concurrent_autocommit_writers():
    def client(i: int) -> int:
        ok = 0
        with GraphDatabase.driver(URI, auth=None, max_transaction_retry_time=0) as d:
            for k in range(ITERATIONS):

                def call() -> None:
                    with d.session() as s:
                        s.run(WRITE_QUERY, n=NODES_PER_WRITE, c=i, k=k).consume()

                run_bounded(call)
                ok += 1
        return ok

    ok = run_clients(client)
    assert ok == N_CLIENTS * ITERATIONS
    assert count_nodes() == ok * NODES_PER_WRITE


def test_concurrent_explicit_transactions():
    def client(i: int) -> int:
        ok = 0
        with GraphDatabase.driver(URI, auth=None, max_transaction_retry_time=0) as d:
            for k in range(ITERATIONS):

                def call() -> None:
                    with d.session() as s:
                        tx = s.begin_transaction()
                        tx.run(WRITE_QUERY, n=NODES_PER_WRITE, c=i, k=k).consume()
                        tx.commit()

                run_bounded(call)
                ok += 1
        return ok

    ok = run_clients(client)
    assert ok == N_CLIENTS * ITERATIONS
    assert count_nodes() == ok * NODES_PER_WRITE


def test_cypher_begin_commit_under_contention(main_lock_contender):
    """Cypher-text BEGIN under main_lock_ contention: only a storage-access TransientError is tolerated."""

    def client(i: int) -> int:
        ok = 0
        # A single pooled connection keeps BEGIN / write / COMMIT on the same Bolt session.
        with GraphDatabase.driver(URI, auth=None, max_transaction_retry_time=0, max_connection_pool_size=1) as d:
            for k in range(BEGIN_ITERATIONS):

                def call() -> bool:
                    with d.session() as s:

                        def body() -> None:
                            s.run("BEGIN").consume()
                            s.run(WRITE_QUERY, n=BEGIN_NODES_PER_WRITE, c=i, k=k).consume()
                            s.run("COMMIT").consume()

                        def rollback() -> None:  # leave the pooled connection outside any transaction
                            try:
                                s.run("ROLLBACK").consume()
                            except Exception:  # noqa: BLE001 - no transaction may be open
                                pass

                        return tolerate_access_timeout(body, rollback)

                if run_bounded(call):
                    ok += 1
        return ok

    ok = run_clients(client, BEGIN_N_CLIENTS, BEGIN_ITERATIONS)
    assert ok >= 1, "no BEGIN/COMMIT write succeeded"
    assert count_nodes() == ok * BEGIN_NODES_PER_WRITE


def _contended_write_clients(write: Callable[[Any, int, int], None]) -> int:
    def client(i: int) -> int:
        ok = 0
        with GraphDatabase.driver(URI, auth=None, max_transaction_retry_time=0) as d:
            for k in range(BEGIN_ITERATIONS):

                def call() -> bool:
                    with d.session() as s:
                        return tolerate_access_timeout(lambda: write(s, i, k))

                if run_bounded(call):
                    ok += 1
        return ok

    return run_clients(client, BEGIN_N_CLIENTS, BEGIN_ITERATIONS)


def test_bolt_begin_message_under_main_lock_contention(main_lock_contender):
    """Bolt BEGIN message (driver begin_transaction) may park on main_lock_; only storage-access TransientError is tolerated."""

    def write(s: Any, i: int, k: int) -> None:
        with s.begin_transaction() as tx:  # closed (rolled back if uncommitted) on any exit
            tx.run(WRITE_QUERY, n=BEGIN_NODES_PER_WRITE, c=i, k=k).consume()
            tx.commit()

    ok = _contended_write_clients(write)
    assert ok >= 1, "no Bolt BEGIN write succeeded"
    assert count_nodes() == ok * BEGIN_NODES_PER_WRITE


def test_autocommit_first_run_under_main_lock_contention(main_lock_contender):
    """RUN-first autocommit (implicit BEGIN) may park on main_lock_; only storage-access TransientError is tolerated."""

    def write(s: Any, i: int, k: int) -> None:
        s.run(WRITE_QUERY, n=BEGIN_NODES_PER_WRITE, c=i, k=k).consume()

    ok = _contended_write_clients(write)
    assert ok >= 1, "no autocommit write succeeded"
    assert count_nodes() == ok * BEGIN_NODES_PER_WRITE


def test_summary_counters_under_contention():
    def client(i: int) -> int:
        with GraphDatabase.driver(URI, auth=None, max_transaction_retry_time=0) as d:
            for k in range(ITERATIONS):

                def call() -> int:
                    with d.session() as s:
                        summary = s.run(WRITE_QUERY, n=NODES_PER_WRITE, c=i, k=k).consume()
                        return summary.counters.nodes_created

                created = run_bounded(call)
                assert created == NODES_PER_WRITE, f"summary reported {created} created nodes"
        return ITERATIONS

    ok = run_clients(client)
    assert count_nodes() == ok * NODES_PER_WRITE


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
