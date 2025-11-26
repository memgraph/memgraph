# Copyright 2025 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import os
import sys
import tempfile

import interactive_mg_runner
import pytest
from common import connect, execute_and_fetch_all

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))


def memgraph_instances(dir):
    return {
        "analytical": {
            "args": [
                "--log-level=TRACE",
                "--also-log-to-stderr",
                "--data-recovery-on-startup=false",
                "--storage-wal-enabled=true",
                "--storage-mode",
                "IN_MEMORY_ANALYTICAL",
            ],
            "log_file": "indices_not_persisted_analytical.log",
            "data_directory": dir,
        },
        "analytical_recover": {
            "args": [
                "--log-level=TRACE",
                "--also-log-to-stderr",
                "--data-recovery-on-startup=true",
                "--storage-wal-enabled=true",
                "--storage-mode",
                "IN_MEMORY_ANALYTICAL",
            ],
            "log_file": "indices_not_persisted_analytical_recover.log",
            "data_directory": dir,
        },
    }


def test_indices_not_persisted_in_analytical_mode():
    data_directory = tempfile.TemporaryDirectory()

    interactive_mg_runner.start(memgraph_instances(data_directory.name), "analytical")
    connection = connect(host="localhost", port=7687)
    cursor = connection.cursor()

    execute_and_fetch_all(cursor, "CREATE INDEX ON :Node;")
    execute_and_fetch_all(cursor, "CREATE INDEX ON :Node(id);")
    execute_and_fetch_all(cursor, "CREATE INDEX ON :Person(name);")
    execute_and_fetch_all(cursor, "CREATE EDGE INDEX ON :KNOWS;")

    result = execute_and_fetch_all(cursor, "SHOW INDEX INFO;")
    print(result)
    assert len(result) == 4, "Expected 4 indices before restart"

    interactive_mg_runner.kill_all()

    interactive_mg_runner.start(memgraph_instances(data_directory.name), "analytical_recover")
    connection = connect(host="localhost", port=7687)
    cursor = connection.cursor()

    result = execute_and_fetch_all(cursor, "SHOW INDEX INFO;")
    assert len(result) == 0, "Expected no indices after restart in analytical mode"

    interactive_mg_runner.kill_all()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
