# Copyright 2023 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import sys

import pytest
from common import Row, cursor, execute_and_fetch_all


def test_show_index_info(cursor):
    index_info = execute_and_fetch_all(cursor, "SHOW INDEX INFO;")
    expected_index_info = {
        ("label", "Anatomy", None),
        ("label", "Compound", None),
        ("label", "Disease", None),
        ("label", "Gene", None),
        ("label+property", "Compound", "id"),
        ("label+property", "Compound", "inchikey"),
        ("label+property", "Compound", "mgid"),
        ("label+property", "Gene", "i5"),
        ("label+property", "Gene", "id"),
    }
    assert set(index_info) == expected_index_info


def test_index_info_sorted(cursor):
    index_info = execute_and_fetch_all(cursor, "SHOW INDEX INFO;")
    assert index_info == sorted(
        index_info,
        key=lambda index: (
            index[Row.INDEX_TYPE],
            index[Row.LABEL],
            index[Row.PROPERTY],
        ),
    )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
