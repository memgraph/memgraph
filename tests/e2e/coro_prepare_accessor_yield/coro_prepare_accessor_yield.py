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

# e2e for the coroutine-park Prepare path (--experimental-coro-prepare-accessor-yield), Session-
# surgery Stage B. See opencode-work/resource-lock-starvation/coro-prepare/ip1-design.md.
#
# The server (see workloads.yaml) runs with:
#   --bolt-num-workers=2                              (small, deliberately easy to starve)
#   --storage-access-timeout-sec=2                    (bounded, deterministic timeout)
#   --experimental-coro-prepare-accessor-yield=true   (flag ON: contended LOW-priority accessor
#                                                       acquires park their pool worker instead of
#                                                       blocking it in try_lock_for)
#
# With only 2 pool workers, a query that BLOCKS one of them for the full ~2s timeout can easily
# starve every other query dispatched onto that same small pool. The scenario below holds a plain
# accessor open on one connection well past the timeout, while several unrelated connections fire
# ordinary queries and one more connection tries a genuinely contending, LOW-priority, READ_ONLY-
# needing query (CREATE INDEX). If parking works, the ordinary queries stay fast (their pool workers
# are never wedged) and the contending query still fails with the SAME timeout exception message the
# blocking path raises today -- parking must not change observable timeout semantics.

import sys
import threading
import time
import typing

import mgclient
import pytest

HOLD_SECONDS = 3.0
TIMEOUT_SEC = 2.0  # must match workloads.yaml's --storage-access-timeout-sec
ORDINARY_CONNECTIONS = 4
ORDINARY_QUERIES_PER_CONNECTION = 5


def execute_and_fetch_all(cursor: mgclient.Cursor, query: str, params: dict = {}) -> typing.List[tuple]:
    cursor.execute(query, params)
    return cursor.fetchall()


def make_connection(autocommit: bool = True) -> mgclient.Connection:
    connection = mgclient.connect(host="localhost", port=7687)
    connection.autocommit = autocommit
    return connection


@pytest.fixture(autouse=True)
def clean_database():
    conn = make_connection()
    cursor = conn.cursor()
    execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n")
    try:
        execute_and_fetch_all(cursor, "DROP INDEX ON :ParkTestLabel(prop)")
    except Exception:
        pass
    conn.close()
    yield


def test_park_keeps_pool_responsive_and_preserves_timeout():
    # --- Connection that holds a plain (WRITE-type) accessor open for HOLD_SECONDS. ---
    holder = make_connection(autocommit=False)
    hc = holder.cursor()
    execute_and_fetch_all(hc, "RETURN 1")  # takes and holds the shared storage accessor

    release_holder = threading.Event()

    def hold_worker():
        release_holder.wait(HOLD_SECONDS)
        holder.commit()

    hold_thread = threading.Thread(target=hold_worker)
    hold_thread.start()

    # --- Ordinary connections/queries that need no contended accessor: must stay fast regardless
    # --- of the holder or the contending query below. ---
    ordinary_durations = []
    ordinary_lock = threading.Lock()

    def ordinary_worker():
        conn = make_connection()
        cur = conn.cursor()
        start = time.time()
        for _ in range(ORDINARY_QUERIES_PER_CONNECTION):
            execute_and_fetch_all(cur, "RETURN 1")
        elapsed = time.time() - start
        with ordinary_lock:
            ordinary_durations.append(elapsed)
        conn.close()

    ordinary_threads = [threading.Thread(target=ordinary_worker) for _ in range(ORDINARY_CONNECTIONS)]

    # --- The genuinely-contending query: LOW priority (not a Cypher query, not in the hand-picked
    # --- HIGH-priority allowlist -- see SessionHL::ApproximateQueryPriority) and READ_ONLY access
    # --- type (IndexQuery::Action::CREATE) -- exactly the shape that can park. ---
    contender_result = {}

    def contender_worker():
        conn = make_connection()
        cur = conn.cursor()
        start = time.time()
        try:
            execute_and_fetch_all(cur, "CREATE INDEX ON :ParkTestLabel(prop)")
            contender_result["error"] = None
        except Exception as e:
            contender_result["error"] = str(e)
        contender_result["elapsed"] = time.time() - start
        conn.close()

    contender_thread = threading.Thread(target=contender_worker)

    # Start the contender first so its (parked) acquire attempt is already registered before the
    # ordinary queries pile onto the same 2-worker pool -- this is exactly the scenario where,
    # without parking, the ordinary queries could get stuck behind a worker blocked in the
    # contender's try_lock_for.
    contender_thread.start()
    time.sleep(0.3)
    for t in ordinary_threads:
        t.start()

    for t in ordinary_threads:
        t.join(timeout=HOLD_SECONDS + 10)
        assert not t.is_alive(), "an ordinary query thread did not finish -- the pool appears wedged"
    contender_thread.join(timeout=HOLD_SECONDS + 10)
    assert not contender_thread.is_alive(), "the contending query did not finish"

    release_holder.set()
    hold_thread.join(timeout=HOLD_SECONDS + 10)
    holder.close()

    # Assertion 1: the server stayed responsive -- ordinary queries completed well before the
    # holder released, i.e. the 2 pool workers were never both wedged blocking on the contended
    # accessor acquire.
    assert len(ordinary_durations) == ORDINARY_CONNECTIONS
    for d in ordinary_durations:
        assert d < HOLD_SECONDS - 0.5, f"an ordinary query took {d:.2f}s -- pool workers were not responsive"

    # Assertion 2: timeout semantics preserved -- the contending query truly cannot acquire within
    # --storage-access-timeout-sec (the holder outlives it) and must still fail with the same
    # message the blocking path raises today, not hang indefinitely or succeed early.
    assert contender_result.get("error") is not None, "CREATE INDEX should have timed out while the accessor was held"
    assert contender_result["error"].startswith(
        "Cannot get read-only access to the storage."
    ), f"unexpected error message: {contender_result['error']}"
    assert (
        contender_result["elapsed"] >= TIMEOUT_SEC * 0.5
    ), f"timeout fired implausibly fast ({contender_result['elapsed']:.2f}s)"
    assert (
        contender_result["elapsed"] < HOLD_SECONDS
    ), f"timeout should fire before the holder releases ({contender_result['elapsed']:.2f}s)"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
