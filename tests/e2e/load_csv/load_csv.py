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

import os
import sys
from pathlib import Path

import pytest
from gqlalchemy import Memgraph
from mgclient import DatabaseError
from neo4j import GraphDatabase

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


def test_creating_labels_with_load_csv_variable():
    memgraph = Memgraph("localhost", 7687)

    results = list(
        memgraph.execute_and_fetch(
            f"""LOAD CSV FROM '{get_file_path(SIMPLE_CSV_FILE)}' WITH HEADER AS row
        CREATE (p:row.name)
        RETURN p
        """
        )
    )

    assert len(results) == 4
    assert results[0]["p"]._labels == {"Joseph"}
    assert results[1]["p"]._labels == {"Peter"}
    assert results[2]["p"]._labels == {"Ella"}
    assert results[3]["p"]._labels == {"Joe"}


def test_create_relationships_with_load_csv_variable2():
    memgraph = Memgraph("localhost", 7687)

    results = list(
        memgraph.execute_and_fetch(
            f"""LOAD CSV FROM '{get_file_path(SIMPLE_CSV_FILE)}' WITH HEADER AS row
        CREATE (p:row.name:Person:row.id)
        RETURN p
        """
        )
    )

    assert len(results) == 4
    assert results[0]["p"]._labels == {"Joseph", "Person", "1"}
    assert results[1]["p"]._labels == {"Peter", "Person", "2"}
    assert results[2]["p"]._labels == {"Ella", "Person", "3"}
    assert results[3]["p"]._labels == {"Joe", "Person", "4"}


def test_load_csv_with_parameters():
    URI = "bolt://localhost:7687"
    AUTH = ("", "")

    with GraphDatabase.driver(URI, auth=AUTH) as client:
        with client.session(database="memgraph") as session:
            results = session.run(
                f"""MATCH (n {{prop: 1}}) LOAD CSV
                FROM $file WITH HEADER AS row
                CREATE (:Person {{name: row.name}})
                RETURN n""",
                file=get_file_path(SIMPLE_CSV_FILE),
            )
            assert len(list(results)) == 4


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
