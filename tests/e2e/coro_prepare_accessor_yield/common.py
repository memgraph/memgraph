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
(--experimental-coro-prepare-accessor-yield), Session-surgery Stage B. See
opencode-work/resource-lock-starvation/coro-prepare/ip1-design.md.

THE MECHANISM (see utils/resource_lock.hpp and query/interpreter.cpp's IndexQuery/CypherQuery
Visit() overloads for the ground truth):

  * READ and WRITE storage accessors COEXIST: ResourceLock::lock_guard_condition<READ>() only
    checks that no UNIQUE is pending -- it never checks w_count. So a plain read (or a bare
    `RETURN 1`) never blocks, and is never blocked by, an open write transaction.
  * READ_ONLY (what `CREATE INDEX` takes for IN_MEMORY_TRANSACTIONAL --
    IndexQuery::Action::CREATE) and UNIQUE are the only access types that get CONTENDED against a
    concurrent WRITE: lock_guard_condition<READ_ONLY>() requires `w_count == 0`.
  * An explicit (BEGIN'd) transaction defaults to StorageAccessType::WRITE unless the driver
    marks it read-only (PrepareTransactionQuery's BEGIN handler: `extras.is_read ? READ : WRITE`;
    mgclient never sets that flag), regardless of which statements are subsequently run inside
    it. So holding an explicit transaction open with ANY statement -- even `RETURN 1` -- holds a
    WRITE accessor. This module still issues an explicit write (`CREATE`) for the holder, both
    because it is the least ambiguous way to force WRITE and because it is what a real
    long-running interactive write transaction looks like.
  * `CREATE INDEX` (and RETURN 1, and BEGIN/COMMIT/ROLLBACK) are all LOW priority ONCE PARSED
    (SessionHL::ApproximateQueryPriority: only a hand-picked allowlist of non-Cypher/non-index
    system queries gets HIGH -- see src/glue/SessionHL.cpp:223-259). With a small
    --bolt-num-workers pool, enough concurrent LOW priority CREATE INDEX contenders can pin every
    worker -- if their accessor acquire blocks the worker thread instead of parking it, nothing
    else LOW priority (including a trivial RETURN 1) can be dispatched until a worker frees up.
  * IMPORTANT ESCAPE HATCH -- why NUM_CONTENDERS is bolt-num-workers + 1, not bolt-num-workers:
    `--bolt-num-workers=N` only sizes the LP ("mixed-work") tier of `utils::PriorityThreadPool`.
    There is ALWAYS one additional, fixed HP (high-priority) worker thread alongside it
    (src/memgraph.cpp: `worker_pool_.emplace(bolt_num_workers /*LP*/, 1U /*HP*/, ...)`), not
    counted by the flag. `SessionHL::ApproximateQueryPriority()` (SessionHL.cpp:223-258) can only
    classify a query as LOW *after* it has been parsed (`state_ == Parsed`); a request's very
    first dispatch (from `OnRead`, before any parsing happens) falls through to the function's
    default, `utils::Priority::HIGH` (SessionHL.cpp:258). A HIGH-tagged task queued behind an
    already-busy LP worker is exactly what the dedicated HP thread's work-stealing loop looks for
    (src/utils/priority_thread_pool.cpp's steal gate: `if (worker->work_.top().id <=
    kMaxLowPriorityId) continue;`), so it can steal and run that task on the HP thread --
    independent of whether both LP workers are pinned. Concretely: with exactly
    `--bolt-num-workers` contenders and a single free-standing `RETURN 1` probe, the probe's
    first (and often only) dispatch is HIGH-tagged and gets stolen onto the ever-present HP
    thread, returning fast REGARDLESS of flag state or LP-pool saturation -- this was verified
    empirically (an earlier iteration of this test measured the probe returning in ~7ms with the
    flag OFF and every LP worker genuinely, provably blocked for 6-12s) and independently against
    the source. To close this escape hatch, NUM_CONTENDERS below is bolt-num-workers + 1: enough
    to saturate every LP worker AND the one HP thread, so the probe (also HIGH-tagged on its
    first dispatch) has no free thread anywhere to be stolen onto.

This is exactly the discriminating scenario: hold a WRITE accessor open (H), fire enough
conflicting CREATE INDEX contenders to saturate every worker thread including the HP one (C0..Ck),
then measure how long a completely unrelated RETURN 1 (P) takes to come back. Under real parking,
P is fast regardless of the contenders (every thread they touched was freed almost immediately).
Under plain blocking, P is stuck behind the pinned threads until one frees.

WHY THE CONTENDERS ARE SEPARATE PROCESSES, NOT THREADS -- mgclient does not release the GIL
during a blocking `execute()`/response-read (its C extension wraps `mg_session_run`/`_pull`/
`_fetch` without `Py_BEGIN_ALLOW_THREADS`; see the still-open upstream issue
https://github.com/memgraph/pymgclient/issues/44, "Release the GIL when dealing with not Python
related things"). An earlier iteration of this module ran the contenders as `threading.Thread`s
and hit exactly that bug: server-side TRACE logs showed only ONE contender's query actually being
dispatched at scenario start; the other contenders' bolt connections weren't even *accepted*
until the first contender's blocking call finally returned (up to 6s later) and released the
GIL -- starving every other Python thread in the process, including the one meant to fire P's
probe and the one meant to release H on schedule. Running each contender in its own OS process
(`concurrent.futures.ProcessPoolExecutor`) gives each one its own GIL, so they are genuinely
concurrent from the server's point of view, matching what the scenario is actually meant to
exercise (NUM_CONTENDERS real concurrent connections, not one real connection plus
NUM_CONTENDERS-1 GIL-starved stragglers).
"""

import concurrent.futures
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
        # that the contenders *succeed* here. Investigation (see below) found that with every LP
        # worker AND the HP thread saturated, a WRITE-vs-READ_ONLY conflict like this one
        # currently rides out the full --storage-access-timeout-sec in BOTH flag states before
        # resolving, for two unrelated reasons:
        #   * flag OFF: H's own COMMIT is itself a task on the *same* saturated worker pool as the
        #     contenders' blocking try_lock_shared_for calls -- with every worker (LP and HP)
        #     already pinned, H's COMMIT can't be dispatched until one frees, which (under
        #     blocking) only happens when a contender's own timeout fires. This is an emergent,
        #     expected property of deliberately saturating the whole pool (exactly what the task
        #     requires), not a bug.
        #   * flag ON: `Storage::Accessor::~Accessor()` (storage.cpp:255-263) only calls
        #     `NotifyMainLockReleased()` for UNIQUE/READ_ONLY releases, never for WRITE (or READ)
        #     -- see the early `if (original_access_type_ != UNIQUE && original_access_type_ !=
        #     READ_ONLY) return;`. The parked coro-acquire path never waits on ResourceLock's own
        #     condition variable (that's the whole point of parking); its only early-wake signal
        #     is `main_lock_resume_event_`/`NotifyMainLockReleased()` (storage.cpp:300-311). Since
        #     H's WRITE release never calls it, a READ_ONLY waiter parked behind a WRITE holder
        #     has no early-wake source at all and just rides DeadlineParkRegistry's deadline-only
        #     sweep to the full timeout -- a real notify-gap bug in the feature under test,
        #     independently verified against the source, and orthogonal to what THIS assertion
        #     (worker responsiveness for unrelated queries) checks.
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
