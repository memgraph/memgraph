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

"""Shared scenario for the parkable-Prepare accessor-yield e2e tests
(--experimental-coro-prepare-accessor-yield).

Mechanism: READ and WRITE storage accessors coexist, but READ_ONLY (what CREATE INDEX takes) and
UNIQUE are contended against a WRITE. An explicit BEGIN'd transaction holds a WRITE accessor. All
these queries are LOW priority once parsed, so enough concurrent CREATE INDEX contenders can pin
every worker -- if their acquire blocks the worker instead of parking it, a trivial RETURN 1 can't
be dispatched until a worker frees.

NUM_CONTENDERS is bolt-num-workers + 1 (not bolt-num-workers): --bolt-num-workers sizes only the LP
tier; there is always one extra HP worker. A request's first dispatch is HIGH-tagged (priority is
only known after parsing), so a HIGH probe queued behind busy LP workers gets work-stolen onto the
HP thread and returns fast regardless of flag state. The +1 saturates the HP thread too, closing
that escape hatch.

Discriminating scenario: hold a WRITE accessor (H), fire enough CREATE INDEX contenders to
saturate every worker incl. HP, then measure a RETURN 1 (P). Under parking P is fast (workers
freed immediately); under blocking P is stuck behind pinned threads.

Contenders run as separate processes, not threads: mgclient does not release the GIL during a
blocking execute() (pymgclient issue #44), so thread-based contenders serialize onto one real
connection. ProcessPoolExecutor gives each its own GIL and genuine server-side concurrency.
"""

import concurrent.futures
import threading
import time
import typing

import mgclient

BOLT_PORT = 7687

# Must match --storage-access-timeout-sec in workloads.yaml.
TIMEOUT_SEC = 6.0

# How long H holds the WRITE accessor. Below TIMEOUT_SEC: a worker can only free before H releases
# via parking (flag ON) -- under blocking, every contender is still in try_lock_for when H commits.
HOLD_SECONDS = 5.0

# For the timeout-preserved check: held past TIMEOUT_SEC so the contender's own timeout fires first.
TIMEOUT_PRESERVED_HOLD_SECONDS = TIMEOUT_SEC + 3.0

# Settle window: lets the contender processes connect, parse, and reach their acquire before P.
SETTLE_SECONDS = 1.5

# bolt-num-workers + 1 to also saturate the always-present HP thread (see docstring).
BOLT_NUM_WORKERS = 2  # must match --bolt-num-workers in workloads.yaml
NUM_CONTENDERS = BOLT_NUM_WORKERS + 1

# Discriminating thresholds, with a wide margin so the contrast is decisive on a loaded box.
RESPONSIVE_THRESHOLD_SECONDS = 1.0
BLOCKED_THRESHOLD_SECONDS = 3.0


def execute_and_fetch_all(
    cursor: "mgclient.Cursor", query: str, params: typing.Optional[dict] = None
) -> typing.List[tuple]:
    cursor.execute(query, params or {})
    return cursor.fetchall()


def make_connection(autocommit: bool = True) -> "mgclient.Connection":
    connection = mgclient.connect(host="localhost", port=BOLT_PORT)
    connection.autocommit = autocommit
    return connection


def clean_database() -> None:
    conn = make_connection()
    cursor = conn.cursor()
    execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n")
    for i in range(NUM_CONTENDERS):
        try:
            execute_and_fetch_all(cursor, f"DROP INDEX ON :Blocker{i}(p)")
        except Exception:
            pass
    try:
        execute_and_fetch_all(cursor, "DROP INDEX ON :TimeoutBlocker(p)")
    except Exception:
        pass
    conn.close()


def hold_write_accessor(
    hold_seconds: float,
) -> typing.Tuple["mgclient.Connection", threading.Thread, threading.Event]:
    """Opens an explicit transaction and writes, holding a WRITE accessor for `hold_seconds` (or
    until `release_event` is set). Returns (connection, release_thread, release_event); the caller
    must .set() the event (or let it time out) and .join() the thread before closing the connection.
    """
    holder = make_connection(autocommit=False)
    hc = holder.cursor()
    execute_and_fetch_all(hc, "CREATE (:HolderMark {p: 1})")  # acquires + holds the WRITE accessor

    release_event = threading.Event()

    def release_worker() -> None:
        release_event.wait(hold_seconds)
        holder.commit()

    release_thread = threading.Thread(target=release_worker)
    release_thread.start()
    return holder, release_thread, release_event


def run_contender(label: str) -> dict:
    """Fires one conflicting `CREATE INDEX ON :{label}(p)` (READ_ONLY, LOW priority) and returns
    {"error": str | None, "elapsed": float}. Top-level/picklable so it can be a ProcessPoolExecutor
    target (see the module docstring for why a process, not a thread)."""
    conn = make_connection()
    cur = conn.cursor()
    start = time.time()
    try:
        execute_and_fetch_all(cur, f"CREATE INDEX ON :{label}(p)")
        return {"error": None, "elapsed": time.time() - start}
    except Exception as e:
        return {"error": str(e), "elapsed": time.time() - start}
    finally:
        conn.close()


def probe_latency(query: str = "RETURN 1") -> float:
    """Measures the wall-clock latency of a single trivial query on a fresh connection."""
    conn = make_connection()
    cur = conn.cursor()
    start = time.time()
    execute_and_fetch_all(cur, query)
    elapsed = time.time() - start
    conn.close()
    return elapsed


def run_responsiveness_scenario() -> typing.Tuple[float, typing.Dict[int, dict]]:
    """Runs the discriminating scenario shared by the flag-ON and flag-OFF-control tests: H holds a
    WRITE accessor; NUM_CONTENDERS CREATE INDEX contenders saturate every worker; after a settle
    window P (RETURN 1) is measured; then H is released and contenders must finish (not hang).
    Returns (p_elapsed_seconds, contender_results); the caller asserts the latency threshold.
    """
    holder, release_thread, release_event = hold_write_accessor(HOLD_SECONDS)

    # Separate OS processes, not threads (see docstring: the mgclient GIL gap would serialize them).
    with concurrent.futures.ProcessPoolExecutor(max_workers=NUM_CONTENDERS) as executor:
        futures = [executor.submit(run_contender, f"Blocker{i}") for i in range(NUM_CONTENDERS)]

        # Settle window so the contenders reach their acquire before P is dispatched.
        time.sleep(SETTLE_SECONDS)

        p_elapsed = probe_latency("RETURN 1")

        # Release H; contenders must wrap up inside a generous timeout. We do NOT assert they
        # *succeed* -- the two flag states resolve differently: flag OFF, H's COMMIT is itself
        # queued behind the saturated pool, so contenders ride out ~storage-access-timeout-sec;
        # flag ON, they park (freeing workers), H commits promptly, and its WRITE release wakes the
        # parked READ_ONLY contenders early (F5). We assert only that nothing hangs. The timeout is
        # scaled by NUM_CONTENDERS since a queued contender can chain an extra timeout.
        release_event.set()
        join_timeout = NUM_CONTENDERS * TIMEOUT_SEC + 10.0
        contender_results: typing.Dict[int, dict] = {}
        for i, fut in enumerate(futures):
            try:
                contender_results[i] = fut.result(timeout=join_timeout)
            except concurrent.futures.TimeoutError as exc:
                raise AssertionError(f"contending CREATE INDEX process {i} did not finish -- looks wedged") from exc
            print(
                f"contender {i}: elapsed={contender_results[i]['elapsed']:.3f}s error={contender_results[i]['error']!r}"
            )

    release_thread.join(timeout=join_timeout)
    assert not release_thread.is_alive(), "the holder's release thread did not finish"
    holder.close()

    return p_elapsed, contender_results


def run_timeout_preserved_scenario(hold_seconds: float = TIMEOUT_PRESERVED_HOLD_SECONDS) -> dict:
    """H holds a WRITE accessor past TIMEOUT_SEC; a conflicting CREATE INDEX must still time out
    with the same ReadOnlyAccessTimeout message at ~TIMEOUT_SEC. Parking changes HOW a contended
    acquire waits, not the timeout outcome or message."""
    holder, release_thread, release_event = hold_write_accessor(hold_seconds)

    # A single contender, so a plain thread is fine here (no GIL contention, unlike the
    # multi-contender scenario above).
    result_holder: typing.List[dict] = []
    contender_thread = threading.Thread(target=lambda: result_holder.append(run_contender("TimeoutBlocker")))
    contender_thread.start()
    contender_thread.join(timeout=hold_seconds + 10.0)
    assert not contender_thread.is_alive(), "the timeout contender did not finish"

    release_event.set()
    release_thread.join(timeout=hold_seconds + 10.0)
    assert not release_thread.is_alive(), "the holder's release thread did not finish"
    holder.close()

    return result_holder[0]
