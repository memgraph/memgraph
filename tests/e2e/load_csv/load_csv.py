# Copyright 2022 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import gzip
import os
import sys
from pathlib import Path

import pytest
from content_server import ContentServer
from gqlalchemy import Memgraph
from mgclient import DatabaseError

SIMPLE_CSV_FILE = "simple.csv"


def get_file_path(file: str) -> str:
    return os.path.join(Path(__file__).parent.absolute(), file)


def test_given_two_rows_in_db_when_load_csv_after_match_then_throw_exception():
    memgraph = Memgraph("localhost", 7687)

    with pytest.raises(DatabaseError):
        next(
            memgraph.execute_and_fetch(
                f"""MATCH (n) LOAD CSV
            FROM '{get_file_path(SIMPLE_CSV_FILE)}' WITH HEADER AS row
            CREATE (:Person {{name: row.name}})
            """
            )
        )


def test_given_one_row_in_db_when_load_csv_after_match_then_pass():
    memgraph = Memgraph("localhost", 7687)

    results = memgraph.execute_and_fetch(
        f"""MATCH (n {{prop: 1}}) LOAD CSV
        FROM '{get_file_path(SIMPLE_CSV_FILE)}' WITH HEADER AS row
        CREATE (:Person {{name: row.name}})
        RETURN n
        """
    )

    assert len(list(results)) == 4


def test_can_load_from_http_source():
    memgraph = Memgraph("localhost", 7687)

    with open(get_file_path(SIMPLE_CSV_FILE), "r") as file:
        content = file.read()

    with ContentServer(content).http_server() as server:
        host = server.server_address[0]
        port = server.server_address[1]
        endpoint = f"http://{host}:{port}"
        results = memgraph.execute_and_fetch(
            f"""LOAD CSV FROM '{endpoint}' WITH HEADER AS row
            CREATE (n:Person {{name: row.name}})
            RETURN n
            """
        )
        assert len(list(results)) == 4


def test_can_load_from_http_source_with_gzip_contents():
    memgraph = Memgraph("localhost", 7687)

    with open(get_file_path(SIMPLE_CSV_FILE), "r") as file:
        content = file.read()

    with ContentServer(content, gzip.compress).http_server() as server:
        host = server.server_address[0]
        port = server.server_address[1]
        endpoint = f"http://{host}:{port}"
        results = memgraph.execute_and_fetch(
            f"""LOAD CSV FROM '{endpoint}' WITH HEADER AS row
            CREATE (n:Person {{name: row.name}})
            RETURN n
            """
        )
        assert len(list(results)) == 4


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
