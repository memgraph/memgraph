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
Stress regression test for the InMemoryStorage::SetStorageMode double-unlock fix.

SetStorageMode() used to hand FreeMemory() a second std::unique_lock adopting main_lock_ that
unique_accessor's guard already owned -- two owners each unlocking once => double-unlock, breaking
mutual exclusion between UNIQUE (mode change) and SHARED accessors. The fix
(Accessor::ReleaseUniqueGuard()) hands FreeMemory() the same lock by move.

Belt-and-suspenders: hammers the observable symptom (flip mode in a loop while other connections
read/write) and asserts the server survives without crash/hang/unexpected error.
"""

import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import mgclient
import pytest
from common import connect, execute_and_fetch_all

DURATION_SECONDS = 5
NUM_WRITERS = 4
NUM_READERS = 4
JOIN_TIMEOUT_SECONDS = 60


def _flip_storage_mode(stop_event: threading.Event, errors: list) -> None:
    connection = mgclient.connect(host="localhost", port=7687)
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        while not stop_event.is_set():
            execute_and_fetch_all(cursor, "STORAGE MODE IN_MEMORY_ANALYTICAL;")
            execute_and_fetch_all(cursor, "STORAGE MODE IN_MEMORY_TRANSACTIONAL;")
    except Exception as exc:  # noqa: BLE001 - any exception here is itself the failure signal
        errors.append(("storage-mode-flipper", repr(exc)))
    finally:
        connection.close()


def _hammer_writes(worker_id: int, stop_event: threading.Event, errors: list) -> None:
    connection = mgclient.connect(host="localhost", port=7687)
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        while not stop_event.is_set():
            execute_and_fetch_all(cursor, f"CREATE (:StorageModeStress {{writer: {worker_id}}})")
    except Exception as exc:  # noqa: BLE001
        errors.append((f"writer-{worker_id}", repr(exc)))
    finally:
        connection.close()


def _hammer_reads(worker_id: int, stop_event: threading.Event, errors: list) -> None:
    connection = mgclient.connect(host="localhost", port=7687)
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        while not stop_event.is_set():
            execute_and_fetch_all(cursor, "MATCH (n:StorageModeStress) RETURN count(n)")
    except Exception as exc:  # noqa: BLE001
        errors.append((f"reader-{worker_id}", repr(exc)))
    finally:
        connection.close()


def test_storage_mode_flip_under_concurrent_load(connect):
    """Flip storage mode under concurrent read/write load; assert no crash/hang/error. The pre-fix
    binary manifests the double-unlock only racily. A hang (TimeoutError) is as valid a failure
    signal as an exception -- broken mutual exclusion can present as either."""
    stop_event = threading.Event()
    errors: list = []

    with ThreadPoolExecutor(max_workers=1 + NUM_WRITERS + NUM_READERS) as pool:
        futures = [pool.submit(_flip_storage_mode, stop_event, errors)]
        futures += [pool.submit(_hammer_writes, i, stop_event, errors) for i in range(NUM_WRITERS)]
        futures += [pool.submit(_hammer_reads, i, stop_event, errors) for i in range(NUM_READERS)]

        time.sleep(DURATION_SECONDS)
        stop_event.set()

        for future in as_completed(futures, timeout=JOIN_TIMEOUT_SECONDS):
            future.result()  # re-raise anything a worker thread didn't catch itself

    assert not errors, f"Concurrent storage-mode flips produced unexpected errors: {errors}"

    # Final connectivity check: the server must still be alive and responsive.
    cursor = connect.cursor()
    execute_and_fetch_all(cursor, "STORAGE MODE IN_MEMORY_TRANSACTIONAL;")
    result = execute_and_fetch_all(cursor, "RETURN 1")
    assert result == [(1,)]


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
