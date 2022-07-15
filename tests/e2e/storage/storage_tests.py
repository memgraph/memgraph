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

import sys

import os
import pytest

from mg_utils import mg_sleep_and_assert
import mgclient
import interactive_mg_runner
import inspect

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))


def memgraph_instances_description():
    return {
        "main": {
            "args": ["--bolt-port", "7687", "--log-level=TRACE"],
            "log_file": f"main{inspect.stack()[1][3]}.log",
            "setup_queries": [],
        },
    }


def test_create_and_drop_indexes_correctly_label():
    interactive_mg_runner.start_all(memgraph_instances_description())

    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("CREATE INDEX ON :Number1;")
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW INDEX INFO;")
    assert len(res_from_main) == 1, f"Incorect result: {res_from_main}"

    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("CREATE INDEX ON :Number2;")
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW INDEX INFO;")
    assert len(res_from_main) == 2, f"Incorect result: {res_from_main}"

    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("DROP INDEX ON :Number2;")
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW INDEX INFO;")
    assert len(res_from_main) == 1, f"Incorect result: {res_from_main}"


def test_create_index_label_twice():
    interactive_mg_runner.start_all(memgraph_instances_description())

    QUERY_INDEX_CREATION = "CREATE INDEX ON :Number;"

    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_INDEX_CREATION)
    res_from_main_before_second_creation = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW INDEX INFO;")
    assert len(res_from_main_before_second_creation) == 1, f"Incorect result: {res_from_main_before_second_creation}"
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_INDEX_CREATION)
    res_from_main_after_second_creation = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW INDEX INFO;")
    assert res_from_main_before_second_creation == res_from_main_after_second_creation


def test_create_and_drop_indexes_correctly_property():
    interactive_mg_runner.start_all(memgraph_instances_description())

    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("CREATE INDEX ON :Number(value1);")
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW INDEX INFO;")
    assert len(res_from_main) == 1, f"Incorect result: {res_from_main}"

    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("CREATE INDEX ON :Number(value2);")
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW INDEX INFO;")
    assert len(res_from_main) == 2, f"Incorect result: {res_from_main}"

    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("DROP INDEX ON :Number(value1);")
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW INDEX INFO;")
    assert len(res_from_main) == 1, f"Incorect result: {res_from_main}"


def test_create_index_property_twice():
    interactive_mg_runner.start_all(memgraph_instances_description())

    QUERY_INDEX_CREATION = "CREATE INDEX ON :Number(value1);"

    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_INDEX_CREATION)
    res_from_main_before_second_creation = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW INDEX INFO;")
    assert len(res_from_main_before_second_creation) == 1, f"Incorect result: {res_from_main_before_second_creation}"

    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_INDEX_CREATION)
    res_from_main_after_second_creation = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW INDEX INFO;")
    assert res_from_main_before_second_creation == res_from_main_after_second_creation


def test_drop_indexes_on_label_while_non_existing():
    interactive_mg_runner.start_all(memgraph_instances_description())

    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("DROP INDEX ON :Number;")

    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW INDEX INFO;")
    assert len(res_from_main) == 0, f"Incorect result: {res_from_main}"


def test_drop_index_on_property_while_non_existing():
    interactive_mg_runner.start_all(memgraph_instances_description())

    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("DROP INDEX ON :Number(value);")

    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW INDEX INFO;")
    assert len(res_from_main) == 0, f"Incorect result: {res_from_main}"


def test_create_and_drop_existence_constraint_correctly():
    interactive_mg_runner.start_all(memgraph_instances_description())

    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(
        "CREATE CONSTRAINT ON (Number:mylabel) ASSERT EXISTS (Number.value1);"
    )
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW CONSTRAINT INFO;")
    assert len(res_from_main) == 1, f"Incorect result: {res_from_main}"

    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(
        "CREATE CONSTRAINT ON (Number:mylabel) ASSERT EXISTS (Number.value2);"
    )
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW CONSTRAINT INFO;")
    assert len(res_from_main) == 2, f"Incorect result: {res_from_main}"

    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("CREATE (p:Number:mylabel {value1:1, value2:2});")
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("MATCH (node) return node;")
    assert len(res_from_main) == 1, f"Incorect result: {res_from_main}"

    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(
        "DROP CONSTRAINT ON (Number:mylabel) ASSERT EXISTS (Number.value2);"
    )
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW CONSTRAINT INFO;")
    assert len(res_from_main) == 1, f"Incorect result: {res_from_main}"


def test_create_existence_constraint_twice():
    interactive_mg_runner.start_all(memgraph_instances_description())

    QUERY_CONSTRAINT_CREATION = "CREATE CONSTRAINT ON (Number:mylabel) ASSERT EXISTS (Number.value1);"

    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_CONSTRAINT_CREATION)
    res_from_main_before_second_creation = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(
        "SHOW CONSTRAINT INFO;"
    )
    assert len(res_from_main_before_second_creation) == 1, f"Incorect result: {res_from_main_before_second_creation}"

    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_CONSTRAINT_CREATION)
    res_from_main_after_second_creation = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(
        "SHOW CONSTRAINT INFO;"
    )
    assert res_from_main_before_second_creation == res_from_main_after_second_creation


def test_create_existence_constraint_while_vertex_already_existing():
    interactive_mg_runner.start_all(memgraph_instances_description())

    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("CREATE (p:Number:mylabel {value_very_different:1});")
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("MATCH (node) return node;")
    assert len(res_from_main) == 1, f"Incorect result: {res_from_main}"

    with pytest.raises(mgclient.DatabaseError):
        interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(
            "CREATE CONSTRAINT ON (Number:mylabel) ASSERT EXISTS (Number.value1);"
        )

    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW CONSTRAINT INFO;")
    assert len(res_from_main) == 0, f"Incorect result: {res_from_main}"


def test_drop_existence_constraint_while_not_existing():
    interactive_mg_runner.start_all(memgraph_instances_description("test_drop_existence_constraint_not_existing"))

    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW CONSTRAINT INFO;")
    assert len(res_from_main) == 0, f"Incorect result: {res_from_main}"

    with pytest.raises(mgclient.DatabaseError):
        interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(
            "DROP CONSTRAINT ON (Number:mylabel) ASSERT EXISTS (Number.value1);"
        )


def test_create_and_drop_existence_constraint_correctly():
    interactive_mg_runner.start_all(memgraph_instances_description())

    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(
        "CREATE CONSTRAINT ON (Number:mylabel) ASSERT EXISTS (Number.value1);"
    )
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW CONSTRAINT INFO;")
    assert len(res_from_main) == 1, f"Incorect result: {res_from_main}"

    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(
        "CREATE CONSTRAINT ON (Number:mylabel) ASSERT EXISTS (Number.value2);"
    )
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW CONSTRAINT INFO;")
    assert len(res_from_main) == 2, f"Incorect result: {res_from_main}"

    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("CREATE (p:Number:mylabel {value1:1, value2:2});")
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("MATCH (node) return node;")
    assert len(res_from_main) == 1, f"Incorect result: {res_from_main}"

    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(
        "DROP CONSTRAINT ON (Number:mylabel) ASSERT EXISTS (Number.value2);"
    )
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW CONSTRAINT INFO;")
    assert len(res_from_main) == 1, f"Incorect result: {res_from_main}"


def test_create_existence_constraint_twice():
    interactive_mg_runner.start_all(memgraph_instances_description())

    QUERY_CONSTRAINT_CREATION = "CREATE CONSTRAINT ON (Number:mylabel) ASSERT EXISTS (Number.value1);"

    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_CONSTRAINT_CREATION)
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW CONSTRAINT INFO;")
    assert len(res_from_main) == 1, f"Incorect result: {res_from_main}"

    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_CONSTRAINT_CREATION)
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW CONSTRAINT INFO;")
    assert len(res_from_main) == 1, f"Incorect result: {res_from_main}"


def test_create_existence_constraint_while_vertex_already_existing():
    interactive_mg_runner.start_all(memgraph_instances_description())

    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("CREATE (p:Number:mylabel {value_very_different:1});")
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("MATCH (node) return node;")
    assert len(res_from_main) == 1, f"Incorect result: {res_from_main}"

    QUERY_CONSTRAINT_CREATION = "CREATE CONSTRAINT ON (Number:mylabel) ASSERT EXISTS (Number.value1);"

    with pytest.raises(mgclient.DatabaseError):
        interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_CONSTRAINT_CREATION)

    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW CONSTRAINT INFO;")
    assert len(res_from_main) == 0, f"Incorect result: {res_from_main}"


def test_drop_existence_constraint_while_not_existing():
    interactive_mg_runner.start_all(memgraph_instances_description())

    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW CONSTRAINT INFO;")
    assert len(res_from_main) == 0, f"Incorect result: {res_from_main}"

    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(
        "DROP CONSTRAINT ON (Number:mylabel) ASSERT EXISTS (Number.value1);"
    )


def test_create_and_drop_unique_constraint_correctly():
    interactive_mg_runner.start_all(memgraph_instances_description())

    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(
        "CREATE CONSTRAINT ON (Number:mylabel) ASSERT Number.value1 IS UNIQUE;"
    )
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW CONSTRAINT INFO;")
    assert len(res_from_main) == 1, f"Incorect result: {res_from_main}"

    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(
        "CREATE CONSTRAINT ON (Number:mylabel) ASSERT Number.value2 IS UNIQUE;"
    )
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW CONSTRAINT INFO;")
    assert len(res_from_main) == 2, f"Incorect result: {res_from_main}"

    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("CREATE (p:Number:mylabel {value1:1, value2:2});")
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("MATCH (node) return node;")
    assert len(res_from_main) == 1, f"Incorect result: {res_from_main}"

    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(
        "DROP CONSTRAINT ON (Number:mylabel) ASSERT Number.value2 IS UNIQUE;"
    )
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW CONSTRAINT INFO;")
    assert len(res_from_main) == 1, f"Incorect result: {res_from_main}"


def test_create_unique_constraint_twice():
    interactive_mg_runner.start_all(memgraph_instances_description())

    QUERY_CONSTRAINT_CREATION = "CREATE CONSTRAINT ON (Number:mylabel) ASSERT Number.value1 IS UNIQUE;"

    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_CONSTRAINT_CREATION)
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW CONSTRAINT INFO;")
    assert len(res_from_main) == 1, f"Incorect result: {res_from_main}"

    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_CONSTRAINT_CREATION)
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW CONSTRAINT INFO;")
    assert len(res_from_main) == 1, f"Incorect result: {res_from_main}"


def test_create_unique_constraint_while_vertices_already_existing():
    interactive_mg_runner.start_all(memgraph_instances_description())

    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("CREATE (p:Number:mylabel {value1:1});")
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("CREATE (p:Number:mylabel {value1:1});")
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("MATCH (node) return node;")
    assert len(res_from_main) == 2, f"Incorect result: {res_from_main}"

    QUERY_CONSTRAINT_CREATION = "CREATE CONSTRAINT ON (Number:mylabel) ASSERT Number.value1 IS UNIQUE;"

    with pytest.raises(mgclient.DatabaseError):
        interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_CONSTRAINT_CREATION)

    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW CONSTRAINT INFO;")
    assert len(res_from_main) == 0, f"Incorect result: {res_from_main}"


def test_drop_unique_constraint_while_not_existing():
    interactive_mg_runner.start_all(memgraph_instances_description())

    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW CONSTRAINT INFO;")
    assert len(res_from_main) == 0, f"Incorect result: {res_from_main}"

    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(
        "DROP CONSTRAINT ON (Number:mylabel) ASSERT Number.value1 IS UNIQUE;"
    )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
