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

"""Shared scenario for the parkable-Prepare accessor-yield e2e tests.

THE SCENARIO: hold a WRITE accessor open (H), fire enough conflicting CREATE INDEX contenders to
saturate every worker thread (C0..Ck), then measure how long an unrelated `RETURN 1` (P) takes.
Under parking, P is fast regardless of the contenders, because every worker they touched was freed
almost immediately. Under blocking, P waits behind the pinned workers.

WHY IT DISCRIMINATES (ground truth: utils/resource_lock.hpp, SessionHL::ApproximateQueryPriority):

  * READ and WRITE accessors COEXIST -- can_acquire<READ>() checks only that no UNIQUE is pending,
    never w_count. So `RETURN 1` neither blocks nor is blocked by an open write transaction.
  * READ_ONLY (what `CREATE INDEX` takes on IN_MEMORY_TRANSACTIONAL) and UNIQUE are the only types
    contended against a concurrent WRITE: can_acquire<READ_ONLY>() requires w_count == 0.
  * A BEGIN'd transaction takes WRITE unless the driver marks it read-only, which mgclient never
    does -- so any explicit transaction holds WRITE whatever runs inside it. The holder still issues
    a real `CREATE`, as the least ambiguous way to force WRITE.
  * CREATE INDEX and RETURN 1 are both LOW priority once parsed, so enough concurrent contenders can
    pin every worker -- if their acquire blocks the worker instead of parking it, nothing else LOW
    can be dispatched until one frees.

TWO EMPIRICAL FINDINGS, both of which cost a broken version of this test to learn:

  1. NUM_CONTENDERS is bolt-num-workers + 1, NOT bolt-num-workers. `--bolt-num-workers=N` sizes only
     the LP tier; there is always one additional fixed HP thread. A request's FIRST dispatch happens
     before parsing, so it falls through to the HIGH default -- and a HIGH-tagged task queued behind
     a busy LP worker is exactly what the HP thread's steal loop looks for. With exactly N
     contenders, the probe gets stolen onto the HP thread and returns fast REGARDLESS of flag state:
     measured at ~7ms with the flag OFF while every LP worker was provably blocked for 6-12s. The
     +1 saturates the HP thread too, so the probe has nowhere to be stolen.
  2. The contenders are separate PROCESSES, not threads. mgclient does not release the GIL during a
     blocking execute()/response-read (https://github.com/memgraph/pymgclient/issues/44). As
     threads, server TRACE logs showed only ONE contender's query dispatched -- the others' Bolt
     connections were not even accepted until the first blocking call returned up to 6s later,
     starving the threads meant to fire the probe and release the holder on schedule.
"""

import concurrent.futures
import glob
import os
import threading
import time
import typing

import mgclient

BOLT_PORT = 7687

# Must match --storage-access-timeout-sec in workloads.yaml.
TIMEOUT_SEC = 6.0

# How long connection H (the WRITE-accessor holder) stays open during the responsiveness
# scenario. Deliberately well below TIMEOUT_SEC: the only way a bolt worker can free up before H
# releases is via parking (flag ON) -- under blocking (flag OFF) every contender is still
# waiting in try_lock_for when H commits, none of their own per-acquire timeouts have fired yet.
HOLD_SECONDS = 5.0

# H is held this far past TIMEOUT_SEC for the timeout-preserved check, so the contender's own
# --storage-access-timeout-sec fires first and H is still open when it does.
TIMEOUT_PRESERVED_HOLD_SECONDS = TIMEOUT_SEC + 3.0

# Deterministic settle window: gives the NUM_CONTENDERS processes time to start up, connect, get
# parsed, and reach their (blocked-or-parked) accessor acquire before P is dispatched. Generous
# relative to process-startup/connection/parse overhead, small relative to HOLD_SECONDS/TIMEOUT_SEC.
SETTLE_SECONDS = 1.5

# --bolt-num-workers (from workloads.yaml) is the LP-tier size only; there is always one more,
# fixed HP thread on top of it (see the module docstring's "IMPORTANT ESCAPE HATCH" section) that
# a fresh, not-yet-parsed dispatch (like the probe's RETURN 1) can be work-stolen onto even while
# every LP worker is pinned. NUM_CONTENDERS = bolt-num-workers + 1 saturates the LP tier AND that
# HP thread, so the probe has nowhere left to be stolen onto under blocking.
BOLT_NUM_WORKERS = 2  # must match --bolt-num-workers in workloads.yaml
NUM_CONTENDERS = BOLT_NUM_WORKERS + 1

# The discriminating thresholds. Kept with a large margin against each other (and against
# TIMEOUT_SEC) so the contrast is decisive rather than a coin flip on a loaded box.
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
    """Opens an explicit transaction on its own connection and executes a write, taking and
    holding a WRITE-type storage accessor for `hold_seconds` (or until `release_event` is set,
    whichever comes first -- see the returned Event). WRITE conflicts with the READ_ONLY
    accessor CREATE INDEX needs but coexists with plain READ queries; see the module docstring.

    Returns (connection, release_thread, release_event). The caller must eventually .set() the
    release_event (or just let it time out after hold_seconds) and .join() the release_thread
    before closing the connection.
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
    """Fires a single conflicting `CREATE INDEX ON :{label}(p)` (READ_ONLY, LOW priority) and
    returns {"error": str | None, "elapsed": float}.

    Must be a top-level, picklable function: it is the target of a
    `concurrent.futures.ProcessPoolExecutor` submission (see the module docstring for why a
    process, not a thread), so both it and its (picklable) arguments/return value cross a process
    boundary."""
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
    """Runs the discriminating scenario shared by the flag-ON and flag-OFF-control tests:

      1. H opens an explicit txn and writes, holding a WRITE accessor for HOLD_SECONDS.
      2. NUM_CONTENDERS (== bolt-num-workers + 1) connections each fire a conflicting
         CREATE INDEX (READ_ONLY) -- enough to pin every LP pool worker AND the one
         always-present HP thread if their accessor acquire blocks instead of parking (see the
         module docstring's "IMPORTANT ESCAPE HATCH" section for why +1 is required).
      3. After a settle window, P fires a trivial RETURN 1 and we measure its latency.
      4. H is released; contenders are joined and must finish (succeed OR fail, but not hang)
         well inside a generous timeout -- see the comment further down for why they currently
         are not expected to reliably *succeed* here in either flag state.

    Returns (p_elapsed_seconds, contender_results). The caller decides what latency threshold is
    expected (fast under parking, slow under blocking).
    """
    holder, release_thread, release_event = hold_write_accessor(HOLD_SECONDS)

    # NUM_CONTENDERS separate OS processes (see the module docstring for why not threads -- the
    # mgclient GIL-release gap would otherwise serialize them onto a single real connection at a
    # time). `with` guarantees the pool (and its child processes) is torn down even if a
    # `.result()` below raises on a genuine wedge.
    with concurrent.futures.ProcessPoolExecutor(max_workers=NUM_CONTENDERS) as executor:
        futures = [executor.submit(run_contender, f"Blocker{i}") for i in range(NUM_CONTENDERS)]

        # Deterministic settle window: NUM_CONTENDERS processes have had time to start up,
        # connect, get parsed, and land in their (blocked-or-parked) accessor acquire before P is
        # dispatched.
        time.sleep(SETTLE_SECONDS)

        p_elapsed = probe_latency("RETURN 1")

        # Release H; the contenders (and the holder's own release thread) must wrap up -- one way
        # or another -- well inside a generous join timeout. NOTE: we deliberately do NOT assert
        # that the contenders *succeed* here -- the two flag states resolve the contention very
        # differently:
        #   * flag OFF: H's own COMMIT is itself a task on the *same* saturated worker pool as the
        #     contenders' blocking try_lock_shared_for calls -- with every worker (LP and HP)
        #     already pinned, H's COMMIT can't be dispatched until one frees, which (under
        #     blocking) only happens when a contender's own timeout fires. So the contenders ride
        #     out ~--storage-access-timeout-sec. This is an emergent, expected property of
        #     deliberately saturating the whole pool (exactly what the task requires), not a bug.
        #   * flag ON: the contenders PARK (freeing their workers), so H's COMMIT dispatches
        #     promptly, and H's WRITE release then wakes the parked READ_ONLY contenders early via
        #     `Storage::Accessor::~Accessor()` -> `NotifyMainLockReleased()` (F5 fix: EVERY release
        #     mode notifies now, not just UNIQUE/READ_ONLY). Observed: contenders resolve at ~the
        #     hold duration, well before the timeout. (Before F5, a READ_ONLY waiter parked behind
        #     a WRITE holder had no early-wake source and rode DeadlineParkRegistry's deadline-only
        #     sweep to the full timeout -- now fixed.)
        # Either way, what we assert is only that nothing hangs -- the pool must never wedge. The
        # timeout is scaled by NUM_CONTENDERS: a contender that lands queued behind another
        # (rather than dispatched directly to a free thread) can chain a full extra
        # --storage-access-timeout-sec wait on top of its own (observed empirically: one contender
        # took ~2x TIMEOUT_SEC).
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
    """H holds a WRITE accessor well past TIMEOUT_SEC; a single conflicting CREATE INDEX must
    still time out with the same ReadOnlyAccessTimeout message the blocking path raises, firing
    at roughly TIMEOUT_SEC (well before H would otherwise release). Parking must change HOW a
    contended acquire waits, not the eventual timeout outcome or its error message."""
    holder, release_thread, release_event = hold_write_accessor(hold_seconds)

    # A single contender with nothing else contending for the main process's GIL at the same
    # time (H's release_thread only wakes at hold_seconds, well after this one resolves) -- a
    # plain thread is fine here, unlike run_responsiveness_scenario's multi-contender case (see
    # the module docstring).
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


# ---------------------------------------------------------------------------
# Log assertions, shared by the flag-on and flag-off arms. Both arms assert on the SAME log lines
# (the on-arm that a diagnostic survives parking, the off-arm that it was there to begin with), so
# these live here rather than being copied into each -- they were byte-identical duplicates.
# ---------------------------------------------------------------------------


def active_log_path(stem: str) -> str:
    """Newest log file for `stem`. Each arm has its OWN log name (set by its workloads.yaml entry),
    so the stem is a parameter -- hardcoding one arm's name silently reads the other arm's log."""
    pattern = os.path.join(
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")),
        "e2e",
        "logs",
        f"{stem}_2*.log",
    )
    candidates = glob.glob(pattern)
    assert candidates, f"memgraph did not create a log file matching {pattern}"
    return max(candidates, key=os.path.getmtime)


def read_appended(log_path, start_offset, *, expect, timeout=5.0) -> str:
    """Polls the log from `start_offset` until every string in `expect` appears, or `timeout`."""
    deadline = time.monotonic() + timeout
    while True:
        with open(log_path, "r") as f:
            f.seek(start_offset)
            content = f.read()
        if all(s in content for s in expect):
            return content
        if time.monotonic() >= deadline:
            return content
        time.sleep(0.05)
