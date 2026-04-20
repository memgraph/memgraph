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
from common import connect, execute_and_fetch_all


def check_storage_info(cursor, expected_values):
    cursor.execute("SHOW STORAGE INFO")
    config = cursor.fetchall()

    for conf in config:
        conf_name = conf[0]
        if conf_name in expected_values:
            assert expected_values[conf_name] == conf[1]


def test_analytical_mode_objects_are_actually_deleted_when_asked(connect):
    """Tests objects are actually freed when deleted in analytical mode."""

    expected_values = {
        "vertex_count": 0,
    }

    cursor = connect.cursor()
    check_storage_info(cursor, expected_values)

    cursor.execute("STORAGE MODE IN_MEMORY_ANALYTICAL;")
    cursor.execute("MERGE (n) DELETE n;")
    cursor.execute("FREE MEMORY;")

    check_storage_info(cursor, expected_values)


def test_analytical_mode_objects_are_actually_deleted_when_storage_mode_changes(connect):
    """Tests objects are actually freed when deleted in analytical mode."""

    expected_values = {
        "vertex_count": 0,
    }

    cursor = connect.cursor()
    check_storage_info(cursor, expected_values)

    cursor.execute("STORAGE MODE IN_MEMORY_ANALYTICAL;")
    cursor.execute("MERGE (n) DELETE n;")
    cursor.execute("STORAGE MODE IN_MEMORY_TRANSACTIONAL;")

    check_storage_info(cursor, expected_values)


def test_analytical_mode_label_property_index_cleanup_on_free_memory(connect):
    """Tests stale label+property index entries are cleaned up by FREE MEMORY in analytical mode.

    Without cleanup, overwriting the indexed property leaves the original entries behind, so
    SHOW INDEX INFO reports 2x the number of vertices. FREE MEMORY must reclaim them.
    """

    node_count = 100_000

    cursor = connect.cursor()
    cursor.execute("STORAGE MODE IN_MEMORY_ANALYTICAL;")
    cursor.execute(f'UNWIND range(1, {node_count}) AS x CREATE (:Node {{str: "abcdefghijklomnpqrstuvwxyz"}});')
    cursor.execute("CREATE INDEX ON :Node;")
    cursor.execute("CREATE INDEX ON :Node(str);")
    cursor.execute("MATCH (n) SET n.str = 1;")
    cursor.execute("FREE MEMORY;")

    cursor.execute("SHOW INDEX INFO;")
    index_info = cursor.fetchall()

    label_property_rows = [
        row for row in index_info if row[0] == "label+property" and row[1] == "Node" and row[2] == ["str"]
    ]
    assert len(label_property_rows) == 1, f"Expected exactly one :Node(str) index row, got {index_info}"
    assert (
        label_property_rows[0][3] == node_count
    ), f"Expected {node_count} entries in :Node(str) index, got {label_property_rows[0][3]}"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
