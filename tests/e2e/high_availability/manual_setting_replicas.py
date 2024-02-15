# Copyright 2024 Memgraph Ltd.
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
from common import execute_and_fetch_all
from mg_utils import mg_sleep_and_assert

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

MEMGRAPH_INSTANCES_DESCRIPTION = {
    "instance_3": {
        "args": [
            "--experimental-enabled=high-availability",
            "--bolt-port",
            "7687",
            "--log-level",
            "TRACE",
            "--coordinator-server-port",
            "10013",
        ],
        "log_file": "main.log",
        "setup_queries": [],
    },
}


def test_no_manual_setup_on_main(connection):
    # Goal of this test is to check that all manual registration actions are disabled on instances with coordiantor server port

    # 1
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    any_main = connection(7687, "instance_3").cursor()
    with pytest.raises(Exception) as e:
        execute_and_fetch_all(any_main, "REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:10001';")
    assert str(e.value) == "Can't register replica manually on instance with coordinator server port."

    with pytest.raises(Exception) as e:
        execute_and_fetch_all(any_main, "DROP REPLICA replica_1;")
    assert str(e.value) == "Can't drop replica manually on instance with coordinator server port."

    with pytest.raises(Exception) as e:
        execute_and_fetch_all(any_main, "SET REPLICATION ROLE TO REPLICA WITH PORT 10002;")
    assert str(e.value) == "Can't set role manually on instance with coordinator server port."


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
