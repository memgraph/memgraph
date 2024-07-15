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
from typing import Any, Dict

import interactive_mg_runner
import mgclient
import pytest
from mg_utils import mg_sleep_and_assert_collection


def execute_and_fetch_all(cursor, query: str) -> Any:
    cursor.execute(query)
    return cursor.fetchall()


def connect(**kwargs) -> mgclient.Connection:
    connection = mgclient.connect(**kwargs)
    connection.autocommit = True
    return connection


interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))


def test_durability_with_compression_on(connection):
    # Goal: That data is correctly restored while compression is used.
    # 0/ Setup the database
    # 1/ MAIN CREATE Vertex with compressible property
    # 2/ Validate property is present
    # 3/ Kill MAIN
    # 4/ Start MAIN
    # 5/ Validate property is present

    MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--storage-property-store-compression-enabled=true",
                "--data-recovery-on-startup=true",
            ],
            "log_file": "main.log",
        },
    }

    # 0/
    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL)
    cursor = connection.cursor()

    # 1/
    execute_and_fetch_all(
        cursor, "CREATE ({ prop: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' });"
    )

    # 2/
    def get_properties(cursor):
        return execute_and_fetch_all(cursor, f"MATCH (n) RETURN n.prop;")

    properties = get_properties(cursor)
    assert len(properties) == 1
    assert properties == [("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",)]

    # 3/
    interactive_mg_runner.kill("main")

    # 4/
    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL)

    # 5/
    cursor = connection.cursor()
    properties = get_properties(cursor)

    assert len(properties) == 1
    assert properties == [("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",)]

    interactive_mg_runner.stop("main")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
