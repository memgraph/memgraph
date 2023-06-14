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

NULLIF_CSV_FILE = "nullif.csv"


def get_file_path(file: str) -> str:
    parent_path = Path(__file__).parent.absolute()
    return os.path.join(parent_path, file)


def test_given_csv_when_nullif_then_all_identical_rows_are_null():
    memgraph = Memgraph("localhost", 7687)

    results = list(
        memgraph.execute_and_fetch(
            f"""LOAD CSV FROM '{get_file_path(NULLIF_CSV_FILE)}'
            WITH HEADER NULLIF 'N/A' AS row
            CREATE (n:Person {{name: row.name, age: row.age,
            percentage: row.percentage, works_in_IT: row.works_in_IT}})
            RETURN n
            """
        )
    )

    expected_properties = [
        {"age": "10", "percentage": "15.0", "works_in_IT": "false"},
        {"name": "John", "percentage": "35.4", "works_in_IT": "false"},
        {"name": "Milewa", "age": "34", "works_in_IT": "false"},
        {"name": "Lucas", "age": "50", "percentage": "12.5"},
    ]
    properties = [result["n"]._properties for result in results]

    assert expected_properties == properties


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
