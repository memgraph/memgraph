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

# Graceful shutdown with a query PARKED, against a real process.
#
# Why a self-managed instance rather than a workloads.yaml cluster: the thing under test IS the
# shutdown sequence, so the test has to own the process lifetime and time the stop. That is the
# interactive_mg_runner pattern the durability suite uses.
#
# WHAT THIS DOES AND DOES NOT PIN -- measured, not assumed.
#
# It pins the end-to-end property: a graceful stop with a query parked completes in milliseconds rather
# than hanging. The failure mode is not subtle -- a parked coroutine that is never resumed keeps its
# Session alive, that Session keeps a DatabaseAccess, and ~Gatekeeper then waits five minutes for
# Gatekeeper<Database>::count_ to drop. That is the symptom this whole feature started from.
#
# It does NOT pin any of the three park drains, and I established that by mutation rather than
# inferring it. Removing the per-storage drain in src/memgraph.cpp, the pool's park_registry_.Drain(),
# and Storage::StopAllBackgroundTasks()'s drain -- individually AND all three together -- leaves this
# test passing. The reason is a fourth mechanism that pre-empts all of them in this scenario: tearing
# down the holder's session releases its WRITE accessor, and that release goes through ResourceLock's
# admit observer and wakes the parked contender by the ordinary notify path.
#
# So the drains are only load-bearing for a park with NO conflicting holder left to release -- e.g. one
# that lost a re-probe race and re-parked while the lock was already free. This test cannot construct
# that state over Bolt, so it stays uncovered, and the drains stay justified by reasoning. Do not
# strengthen the claims in this file without re-running those mutations.
#
# THE CONTENDER MUST BE A SEPARATE PROCESS. mgclient does not release the GIL while a statement is in
# flight (pymgclient#44), so a contender on a *thread* holds the GIL for its entire parked wait and
# freezes this process -- including the sleep meant to let it park and the stop() meant to happen while
# it is parked. My first version of this file did exactly that: the main thread only woke when the
# contender's access timeout expired, so shutdown began with NO park in flight and the test passed
# while proving nothing. common.py's module docstring documents the same trap for the same reason.

import multiprocessing
import os
import sys
import time

import interactive_mg_runner
import mgclient
import pytest

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

BOLT_PORT = 7691
INSTANCE = "park_shutdown_main"

# Long on purpose: the contender must still be PARKED when the stop begins, so that shutdown -- not the
# deadline sweep -- is what resolves it. It also makes an "access timeout" outcome positive proof that
# the park was NOT in flight at stop time, which is what the assertion at the end checks.
ACCESS_TIMEOUT_SEC = 120

# Deliberately BELOW interactive_mg_runner's own stop timeout, which polls for 15s and then asserts
# (tests/e2e/memgraph.py). At 30s this assertion could never fire -- the runner's would always win --
# so it was decoration. At 10s a hang of 10-15s fails here with this message; anything longer fails in
# the runner with its own. Either way the test fails, which is what matters; the point of the lower
# number is that the assertion below is reachable and therefore means something.
SHUTDOWN_BUDGET_SEC = 10.0

PARK_SETTLE_SEC = 3.0


def instance_config(test_name):
    return {
        INSTANCE: {
            "args": [
                "--bolt-port",
                str(BOLT_PORT),
                "--log-level=TRACE",
                "--bolt-num-workers=2",
                f"--storage-access-timeout-sec={ACCESS_TIMEOUT_SEC}",
                "--experimental-coro-prepare-accessor-yield=true",
            ],
            "log_file": f"park_shutdown/{test_name}.log",
            "data_directory": f"{interactive_mg_runner.BUILD_DIR}/e2e/data/park_shutdown_{test_name}",
            "setup_queries": [],
        }
    }


@pytest.fixture
def test_name(request):
    return request.node.name


@pytest.fixture(autouse=True)
def cleanup_instances():
    interactive_mg_runner.kill_all()
    yield
    interactive_mg_runner.kill_all()


def _contend(result_queue):
    """Runs in its OWN PROCESS (see the module comment): fires a READ_ONLY query that must park."""
    try:
        conn = mgclient.connect(host="localhost", port=BOLT_PORT)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("CREATE INDEX ON :ShutdownBlocked(p)")
        cur.fetchall()
        result_queue.put({"error": None})
    except Exception as e:  # noqa: BLE001 - the instance is going away underneath it
        result_queue.put({"error": str(e)})


def test_graceful_shutdown_with_a_parked_query(test_name):
    interactive_mg_runner.start_all(instance_config(test_name), keep_directories=False)

    # H holds a WRITE accessor open in an explicit transaction, which blocks READ_ONLY.
    holder = mgclient.connect(host="localhost", port=BOLT_PORT)
    holder.autocommit = True
    holder_cur = holder.cursor()
    holder_cur.execute("BEGIN")
    holder_cur.fetchall()
    holder_cur.execute("CREATE (:ShutdownHolder {p: 1})")
    holder_cur.fetchall()

    # C wants READ_ONLY, cannot get it, and parks for up to ACCESS_TIMEOUT_SEC.
    result_queue = multiprocessing.Queue()
    contender = multiprocessing.Process(target=_contend, args=(result_queue,), daemon=True)
    contender.start()
    time.sleep(PARK_SETTLE_SEC)

    start = time.monotonic()
    interactive_mg_runner.stop(instance_config(test_name), INSTANCE, keep_directories=False)
    elapsed = time.monotonic() - start

    assert elapsed < SHUTDOWN_BUDGET_SEC, (
        f"graceful shutdown with a parked query took {elapsed:.1f}s. The failure mode this guards is "
        f"~Gatekeeper waiting ~5 minutes for Gatekeeper<Database>::count_ to reach zero, which is what "
        f"happens when a parked coroutine is never resumed and keeps its Session (and its "
        f"DatabaseAccess) alive."
    )

    # Vacuity check, and precisely what it does and does not establish. It rules out the two ways the
    # contender could have stopped being parked before the stop: succeeding (it got the lock) and timing
    # out (its 120s budget expired -- impossible at ~3s, but assert it rather than argue it). It does NOT
    # distinguish "was parked" from "was queued and had not started", because a dropped connection looks
    # the same from here and there is no server-side signal for parked-ness -- PrepareCoro defers
    # SetupInterpreterTransaction, so a parked query is deliberately invisible to SHOW TRANSACTIONS.
    contender.join(timeout=30.0)
    assert not contender.is_alive(), "contender outlived the process it was talking to"
    assert not result_queue.empty(), "contender reported nothing"
    outcome = result_queue.get()
    assert outcome["error"] is not None, (
        "the contender's CREATE INDEX SUCCEEDED, so it was never parked at stop time -- the shutdown "
        "measurement above proved nothing"
    )
    assert "access to the storage" not in outcome["error"], (
        f"the contender hit its access timeout ({ACCESS_TIMEOUT_SEC}s) rather than being resolved by the "
        f"shutdown, which means it was no longer parked when the stop began and the measurement above "
        f"was vacuous. error was: {outcome['error']!r}"
    )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA", "-s"]))
