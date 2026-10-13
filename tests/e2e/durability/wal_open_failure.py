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

# A commit that cannot create its WAL file (descriptor exhaustion, EMFILE) fails with an error instead of aborting the
# instance, keeps nothing of the failed transaction, and succeeds again once files can be created. The exhaustion is
# simulated by tests/e2e/durability/emfile_preload.so, which fails creating files under a `wal/` directory with EMFILE
# while its trigger file exists.

import os
import shutil
import sys

import interactive_mg_runner
import pytest
from common import connect, execute_and_fetch_all, get_data_path, get_logs_path
from mg_utils import mg_sleep_and_assert

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

FILE = "wal_open_failure"
PRELOAD_LIBRARY = os.path.join(interactive_mg_runner.SCRIPT_DIR, "emfile_preload.so")
E2E_DIR = os.path.join(interactive_mg_runner.BUILD_DIR, "e2e")
# Not the default ports, so that the test cannot collide with another suite running on the same machine.
MAIN_PORT = 17687
REPLICA_PORT = 17688
REPLICATION_PORT = 17001
NODES_PER_TXN = 200
PADDING = "y" * 64


@pytest.fixture(autouse=True)
def cleanup_after_test():
    yield
    interactive_mg_runner.kill_all(keep_directories=False)


@pytest.fixture
def test_name(request):
    return request.node.name


def instance_description(test_name, narrowing, port=MAIN_PORT, name="main", extra_args=()):
    return {
        name: {
            "args": [
                f"--bolt-port={port}",
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
                # A 1 KiB WAL rotates on nearly every commit, so each commit creates a WAL file.
                "--storage-wal-file-size-kib=1",
                "--storage-wal-file-flush-every-n-tx=1",
            ]
            + (["--experimental-enabled=commit-lock-narrowing"] if narrowing else [])
            + list(extra_args),
            "log_file": f"{get_logs_path(FILE, test_name)}/{name}.log",
            "data_directory": f"{get_data_path(FILE, test_name)}/{name}",
            "setup_queries": [],
        },
    }


def data_directory(test_name, name="main"):
    return os.path.join(E2E_DIR, "data", get_data_path(FILE, test_name), name)


def log_text(test_name, name="main"):
    log_directory = os.path.join(E2E_DIR, "logs", get_logs_path(FILE, test_name))
    return "".join(
        open(os.path.join(log_directory, file)).read()
        for file in sorted(os.listdir(log_directory))
        if file.startswith(f"{name}_")
    )


def trigger_path(test_name):
    return os.path.join(E2E_DIR, "data", f"{FILE}_{test_name}_trigger")


def reset(test_name):
    shutil.rmtree(os.path.join(E2E_DIR, "data", get_data_path(FILE, test_name)), ignore_errors=True)
    if os.path.exists(trigger_path(test_name)):
        os.remove(trigger_path(test_name))


def start_with_shim(instances, name, test_name, monkeypatch):
    assert os.path.isfile(PRELOAD_LIBRARY), f"Missing {PRELOAD_LIBRARY}"
    # The instance inherits the environment, so the shim is loaded but stays inert until the trigger exists.
    monkeypatch.setenv("LD_PRELOAD", PRELOAD_LIBRARY)
    monkeypatch.setenv("MG_TEST_EMFILE_TRIGGER", trigger_path(test_name))
    interactive_mg_runner.start(instances, name)
    monkeypatch.delenv("LD_PRELOAD")


def write_txn(cursor):
    execute_and_fetch_all(cursor, f"UNWIND range(1, {NODES_PER_TXN}) AS i CREATE (:W {{pad: '{PADDING}'}})")


def count_nodes(cursor):
    return execute_and_fetch_all(cursor, "MATCH (n) RETURN count(n)")[0][0]


def wal_files(test_name):
    directory = os.path.join(data_directory(test_name), "wal")
    return [os.path.join(directory, name) for name in os.listdir(directory)]


@pytest.mark.parametrize("narrowing", [False, True], ids=["narrowing_off", "narrowing_on"])
@pytest.mark.parametrize("armed_from", ["first_wal", "rotation"])
def test_commit_fails_cleanly_when_wal_cannot_be_created(test_name, narrowing, armed_from, monkeypatch):
    reset(test_name)
    instances = instance_description(test_name, narrowing)
    start_with_shim(instances, "main", test_name, monkeypatch)
    instance = interactive_mg_runner.MEMGRAPH_INSTANCES["main"]
    cursor = connect(host="localhost", port=MAIN_PORT).cursor()

    expected = 0
    if armed_from == "rotation":
        write_txn(cursor)  # the first WAL exists and is finalised by this commit; the next commit needs a new one
        expected = NODES_PER_TXN

    open(trigger_path(test_name), "w").close()
    for _ in range(3):  # a persisting failure fails every attempt, and none of them takes the process down
        with pytest.raises(Exception, match="persist(e|a)nce error"):
            write_txn(cursor)
        assert instance.is_running()
        assert count_nodes(cursor) == expected
    assert "could not be created" in log_text(test_name)
    assert all(os.path.getsize(path) > 0 for path in wal_files(test_name)), wal_files(test_name)

    os.remove(trigger_path(test_name))
    write_txn(cursor)
    expected += NODES_PER_TXN
    assert count_nodes(cursor) == expected

    # A WAL sequence number burnt by a failed attempt would make recovery fail on the missing file.
    interactive_mg_runner.stop(instances, "main")
    interactive_mg_runner.start(instances, "main")
    cursor = connect(host="localhost", port=MAIN_PORT).cursor()
    assert count_nodes(cursor) == expected


@pytest.mark.parametrize("narrowing", [False, True], ids=["narrowing_off", "narrowing_on"])
def test_sync_replica_survives_wal_creation_failure(test_name, narrowing, monkeypatch):
    reset(test_name)
    main_description = instance_description(test_name, narrowing, MAIN_PORT, "main")
    replica_description = instance_description(test_name, narrowing, REPLICA_PORT, "replica")
    instances = {**main_description, **replica_description}
    interactive_mg_runner.start(instances, "main")
    start_with_shim(instances, "replica", test_name, monkeypatch)
    main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"]
    replica = interactive_mg_runner.MEMGRAPH_INSTANCES["replica"]

    replica_cursor = connect(host="localhost", port=REPLICA_PORT).cursor()
    execute_and_fetch_all(replica_cursor, f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORT}")
    main_cursor = connect(host="localhost", port=MAIN_PORT).cursor()
    execute_and_fetch_all(main_cursor, f'REGISTER REPLICA r SYNC TO "127.0.0.1:{REPLICATION_PORT}"')

    write_txn(main_cursor)
    mg_sleep_and_assert(NODES_PER_TXN, lambda: count_nodes(connect(host="localhost", port=REPLICA_PORT).cursor()))

    open(trigger_path(test_name), "w").close()
    try:
        # The replica cannot create the WAL file for this write; whether the main reports it is not what is under test.
        try:
            write_txn(main_cursor)
        except Exception:
            pass
        assert main.is_running()
        assert replica.is_running()
        assert "could not be created" in log_text(test_name, "replica")
    finally:
        os.remove(trigger_path(test_name))

    # Once files can be created again the replica catches up with the main.
    write_txn(main_cursor)
    main_count = count_nodes(main_cursor)
    assert main_count >= 2 * NODES_PER_TXN
    mg_sleep_and_assert(
        main_count, lambda: count_nodes(connect(host="localhost", port=REPLICA_PORT).cursor()), max_duration=60
    )


@pytest.mark.parametrize("narrowing", [False, True], ids=["narrowing_off", "narrowing_on"])
def test_automatic_index_creation_is_retried_until_wal_can_be_created(test_name, narrowing, monkeypatch):
    reset(test_name)
    instances = instance_description(
        test_name, narrowing, extra_args=["--storage-automatic-label-index-creation-enabled=true"]
    )
    start_with_shim(instances, "main", test_name, monkeypatch)
    cursor = connect(host="localhost", port=MAIN_PORT).cursor()

    execute_and_fetch_all(cursor, "MATCH (n:Auto) RETURN n")  # requests an index on :Auto once a write commits
    # The first WAL creation, by the commit below, is let through; the one the index creation needs is not.
    with open(trigger_path(test_name), "w") as trigger:
        trigger.write("1")
    execute_and_fetch_all(cursor, f"CREATE (:Auto {{pad: '{PADDING * 32}'}})")

    mg_sleep_and_assert(True, lambda: "could not be created" in log_text(test_name))
    assert interactive_mg_runner.MEMGRAPH_INSTANCES["main"].is_running()
    assert execute_and_fetch_all(cursor, "SHOW INDEX INFO") == []

    os.remove(trigger_path(test_name))
    mg_sleep_and_assert(1, lambda: len(execute_and_fetch_all(cursor, "SHOW INDEX INFO")), max_duration=30)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
