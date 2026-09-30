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

import os
import sys

import interactive_mg_runner
import pytest
from common import connect, execute_and_fetch_all, get_data_path

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

FILE = "durability_with_desc_index"


@pytest.fixture(autouse=True)
def cleanup_after_test():
    yield
    interactive_mg_runner.kill_all(keep_directories=False)


@pytest.fixture
def test_name(request):
    return request.node.name


def _index_types_for(cursor, label, prop):
    rows = execute_and_fetch_all(cursor, "SHOW INDEX INFO;")
    return sorted(row[0] for row in rows if row[1] == label and row[2] == [prop])


def test_selective_drop_desc_index_survives_restart(test_name):
    # Semantics of WITH CONFIG {"order": ...} are covered in gql_behave/indices.feature.
    # This test verifies that a selective per-order drop is persisted via WAL so the
    # right order is still absent after recovery.
    data_directory = get_data_path(FILE, test_name)
    desc = {
        "main": {
            "args": ["--log-level=TRACE", "--data-recovery-on-startup=true"],
            "log_file": "selective_drop_survives_restart.log",
            "data_directory": data_directory,
        },
    }
    interactive_mg_runner.start(desc, "main")
    cursor = connect(host="localhost", port=7687).cursor()

    execute_and_fetch_all(cursor, "CREATE INDEX ON :L(prop);")
    execute_and_fetch_all(cursor, 'CREATE INDEX ON :L(prop) WITH CONFIG {"order": "DESC"};')
    execute_and_fetch_all(cursor, 'DROP INDEX ON :L(prop) WITH CONFIG {"order": "ASC"};')
    assert _index_types_for(cursor, "L", "prop") == ["label+property (DESC)"]

    interactive_mg_runner.kill(desc, "main")
    interactive_mg_runner.start(desc, "main")
    cursor = connect(host="localhost", port=7687).cursor()

    assert _index_types_for(cursor, "L", "prop") == ["label+property (DESC)"]


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
