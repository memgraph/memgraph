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

FILE = "parameters_durability"


def instances(data_dir):
    return {
        "no_recovery": {
            "args": ["--log-level=TRACE", "--data-recovery-on-startup=false"],
            "log_file": "parameters_no_recovery.log",
            "data_directory": data_dir,
        },
        "recovery": {
            "args": ["--log-level=TRACE", "--data-recovery-on-startup=true"],
            "log_file": "parameters_recovery.log",
            "data_directory": data_dir,
        },
    }


@pytest.fixture
def test_name(request):
    return request.node.name


def test_parameters_recovered_with_recovery(test_name):
    data_dir = get_data_path(FILE, test_name)
    interactive_mg_runner.start(instances(data_dir), "no_recovery")
    conn = connect(host="localhost", port=7687)
    cursor = conn.cursor()
    execute_and_fetch_all(cursor, 'SET GLOBAL PARAMETER foo="bar";')
    execute_and_fetch_all(cursor, 'SET GLOBAL PARAMETER x="1";')
    interactive_mg_runner.kill_all()

    interactive_mg_runner.start(instances(data_dir), "recovery")
    conn = connect(host="localhost", port=7687)
    cursor = conn.cursor()
    rows = execute_and_fetch_all(cursor, "SHOW PARAMETERS;")
    assert len(rows) == 2
    names = {r[0] for r in rows}
    assert names == {"foo", "x"}
    interactive_mg_runner.kill_all()


def test_global_and_database_parameters_recovered_with_recovery(test_name):
    """Global and database-scoped parameters are both recovered after restart with recovery."""
    data_dir = get_data_path(FILE, test_name)
    interactive_mg_runner.start(instances(data_dir), "no_recovery")
    conn = connect(host="localhost", port=7687)
    cursor = conn.cursor()
    execute_and_fetch_all(cursor, 'SET GLOBAL PARAMETER global_p="g";')
    execute_and_fetch_all(cursor, 'SET PARAMETER db_p="d";')  # current database (default)
    interactive_mg_runner.kill_all()

    interactive_mg_runner.start(instances(data_dir), "recovery")
    conn = connect(host="localhost", port=7687)
    cursor = conn.cursor()
    rows = execute_and_fetch_all(cursor, "SHOW PARAMETERS;")
    assert len(rows) == 2
    by_name = {r[0]: (r[1], r[2]) for r in rows}  # name -> (value, scope)
    assert by_name["global_p"] == ('"g"', "global")
    assert by_name["db_p"] == ('"d"', "database")
    interactive_mg_runner.kill_all()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
