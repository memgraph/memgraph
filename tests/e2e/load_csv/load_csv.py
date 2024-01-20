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


def test_load_csv_with_parameters():
    memgraph = Memgraph("localhost", 7687)

    results = memgraph.execute_and_fetch(
        f"""LOAD CSV
        FROM $file WITH HEADER AS row
        RETURN row.name as name
        """,
        parameters={"file": get_file_path(SIMPLE_CSV_FILE)},
    )

    assert len(list(results)) == 4


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
