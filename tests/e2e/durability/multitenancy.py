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

import interactive_mg_runner
import pytest
from common import connect, execute_and_fetch_all, get_data_path, get_logs_path

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))
file = "multitenancy"


def instance_description(test_name: str):
    return {
        "instance_1": {
            "args": [
                "--bolt-port",
                "7687",
                "--log-level",
                "TRACE",
                "--replication-restore-state-on-startup=true",
                "--data-recovery-on-startup=true",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_1.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_1",
            "setup_queries": [],
        },
    }


@pytest.fixture(autouse=True)
def cleanup_after_test():
    # Run the test
    yield
    # Stop and delete directories after cleaning the test
    interactive_mg_runner.kill_all(keep_directories=False)


@pytest.fixture
def test_name(request):
    return request.node.name


def test_mt_with_hidden_files(test_name):
    instance = instance_description(
        test_name=test_name,
    )

    build_dir = os.path.join(interactive_mg_runner.PROJECT_DIR, "build", "e2e", "data")
    data_dir_instance_1 = f"{build_dir}/{get_data_path(file, test_name)}/instance_1"
    filename = ".snapshot"

    # Create an empty .snapshot file in the root of data directory
    root_file = f"{data_dir_instance_1}/{filename}"
    root_dir = os.path.dirname(root_file)
    if not os.path.exists(root_dir):
        os.makedirs(root_dir)

    with open(root_file, "a"):
        pass

    default_db_file = f"{data_dir_instance_1}/databases/memgraph/{filename}"
    default_db_dir = os.path.dirname(default_db_file)
    if not os.path.exists(default_db_dir):
        os.makedirs(default_db_dir)

    # Create an empty .snapshot file in the default DB data directory
    with open(default_db_file, "a"):
        pass

    interactive_mg_runner.start_all(instance, keep_directories=False)

    cursor = connect(host="localhost", port=7687).cursor()
    execute_and_fetch_all(cursor, "RETURN 0")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
