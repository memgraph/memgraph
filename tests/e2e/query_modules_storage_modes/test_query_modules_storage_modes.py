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

# isort: off
from multiprocessing import Process
import sys
import pytest

from common import cursor, connect

SWITCH_TO_ANALYTICAL = "STORAGE MODE IN_MEMORY_ANALYTICAL;"


def modify_graph(query):
    subprocess_cursor = connect()
    subprocess_cursor.execute(query)


def execute_test(cursor, deleter_query, cursor_query):
    deleter = Process(
        target=modify_graph,
        args=(deleter_query),
    )
    deleter.start()
    cursor.execute(cursor_query)
    deleter.join()
    return cursor.fetchall()


@pytest.mark.parametrize("api", ["c", "cpp", "python"])
def test_function_delete_result(cursor, api):
    cursor.execute(SWITCH_TO_ANALYTICAL)

    deleter_query = "MATCH (m:Component {id: 'A7422'})-[e:PART_OF]->(n:Component {id: '7X8X0'}) DELETE e;"
    cursor_query = f"MATCH (m)-[e]->(n) RETURN {api}_api.pass_relationship(e);"
    result = execute_test(cursor, deleter_query, cursor_query)

    assert len(result) == 1 and len(result[0]) == 1 and result[0][0].type == "DEPENDS_ON"


@pytest.mark.parametrize("api", ["c", "cpp", "python"])
def test_function_delete_only_result(cursor, api):
    cursor.execute(SWITCH_TO_ANALYTICAL)
    cursor.execute("MATCH (m:Component {id: '7X8X0'})-[e:DEPENDS_ON]->(n:Component {id: 'A7422'}) DELETE e;")

    deleter_query = "MATCH (m:Component {id: 'A7422'})-[e:PART_OF]->(n:Component {id: '7X8X0'}) DELETE e;"
    cursor_query = f"MATCH (m)-[e]->(n) RETURN {api}_api.pass_relationship(e);"
    result = execute_test(cursor, deleter_query, cursor_query)

    assert len(result) == 1 and len(result[0]) == 1 and result[0][0] is None


@pytest.mark.parametrize("api", ["c", "cpp", "python"])
def test_procedure_delete_result(cursor, api):
    cursor.execute(SWITCH_TO_ANALYTICAL)

    deleter_query = "MATCH (n {id: 'A7422'}) DETACH DELETE n;"
    cursor_query = f"""MATCH (n)
        CALL {api}_api.pass_node_with_id(n)
        YIELD node, id
        RETURN node, id;"""
    result = execute_test(cursor, deleter_query, cursor_query)

    assert len(result) == 1 and len(result[0]) == 2 and result[0][0].properties["id"] == "7X8X0"


@pytest.mark.parametrize("api", ["c", "cpp", "python"])
def test_procedure_delete_only_result(cursor, api):
    cursor.execute(SWITCH_TO_ANALYTICAL)
    cursor.execute("MATCH (n {id: '7X8X0'}) DETACH DELETE n;")

    deleter_query = "MATCH (n {id: 'A7422'}) DETACH DELETE n;"
    cursor_query = f"MATCH (n) CALL {api}_api.pass_node_with_id(n) YIELD node, id RETURN node, id;"
    result = execute_test(cursor, deleter_query, cursor_query)

    assert len(result) == 0


def test_deleted_node(cursor):
    cursor.execute(SWITCH_TO_ANALYTICAL)

    deleter_query = "MATCH (n:Component {id: 'A7422'}) DETACH DELETE n;"
    cursor_query = "MATCH (n: Component {id: 'A7422'}) RETURN python_api.pass_node(n);"
    result = execute_test(cursor, deleter_query, cursor_query)

    assert len(result) == 1 and len(result[0]) == 1 and result[0][0] is None


def test_deleted_relationship(cursor):
    cursor.execute(SWITCH_TO_ANALYTICAL)

    deleter_query = "MATCH (:Component {id: 'A7422'})-[e:PART_OF]->(:Component {id: '7X8X0'}) DELETE e;"
    cursor_query = "MATCH (:Component {id: 'A7422'})-[e:PART_OF]->(:Component {id: '7X8X0'}) RETURN python_api.pass_relationship(e);"
    result = execute_test(cursor, deleter_query, cursor_query)

    assert len(result) == 1 and len(result[0]) == 1 and result[0][0] is None


def test_deleted_node_in_path(cursor):
    cursor.execute(SWITCH_TO_ANALYTICAL)

    deleter_query = "MATCH (n:Component {id: 'A7422'}) DETACH DELETE n;"
    cursor_query = "MATCH path=(n {id: 'A7422'})-[e]->(m) RETURN python_api.pass_path(path);"
    result = execute_test(cursor, deleter_query, cursor_query)

    assert len(result) == 1 and len(result[0]) == 1 and result[0][0] is None


def test_deleted_relationship_in_path(cursor):
    cursor.execute(SWITCH_TO_ANALYTICAL)

    deleter_query = "MATCH (:Component {id: 'A7422'})-[e:PART_OF]->(:Component {id: '7X8X0'}) DELETE e;"
    cursor_query = "MATCH path=(n {id: 'A7422'})-[e]->(m) RETURN python_api.pass_path(path);"
    result = execute_test(cursor, deleter_query, cursor_query)

    assert len(result) == 1 and len(result[0]) == 1 and result[0][0] is None


def test_deleted_value_in_list(cursor):
    cursor.execute(SWITCH_TO_ANALYTICAL)

    deleter_query = "MATCH (:Component {id: 'A7422'})-[e:PART_OF]->(:Component {id: '7X8X0'}) DELETE e;"
    cursor_query = "MATCH (n)-[e]->() WITH collect(n) + collect(e) as list RETURN python_api.pass_list(list);"
    result = execute_test(cursor, deleter_query, cursor_query)

    assert len(result) == 1 and len(result[0]) == 1 and result[0][0] is None


def test_deleted_value_in_map(cursor):
    cursor.execute(SWITCH_TO_ANALYTICAL)

    deleter_query = "MATCH (:Component {id: 'A7422'})-[e:PART_OF]->(:Component {id: '7X8X0'}) DELETE e;"
    cursor_query = (
        "MATCH (n {id: 'A7422'})-[e]->() WITH {node: n, relationship: e} AS map RETURN python_api.pass_map(map);"
    )
    result = execute_test(cursor, deleter_query, cursor_query)

    assert len(result) == 1 and len(result[0]) == 1 and result[0][0] is None


@pytest.mark.parametrize("storage_mode", ["IN_MEMORY_TRANSACTIONAL", "IN_MEMORY_ANALYTICAL"])
def test_function_none_deleted(storage_mode):
    cursor = connect()

    cursor.execute(f"STORAGE MODE {storage_mode};")
    cursor.execute("CREATE (m:Component {id: 'A7422'}), (n:Component {id: '7X8X0'});")

    cursor.execute(
        """MATCH (n)
        RETURN python_api.pass_node(n);"""
    )

    result = cursor.fetchall()
    cursor.execute("MATCH (n) DETACH DELETE n;")

    assert len(result) == 2


@pytest.mark.parametrize("storage_mode", ["IN_MEMORY_TRANSACTIONAL", "IN_MEMORY_ANALYTICAL"])
def test_procedure_none_deleted(storage_mode):
    cursor = connect()

    cursor.execute(f"STORAGE MODE {storage_mode};")
    cursor.execute("CREATE (m:Component {id: 'A7422'}), (n:Component {id: '7X8X0'});")

    cursor.execute(
        """MATCH (n)
        CALL python_api.pass_node_with_id(n)
        YIELD node, id
        RETURN node, id;"""
    )

    result = cursor.fetchall()
    cursor.execute("MATCH (n) DETACH DELETE n;")

    assert len(result) == 2


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
