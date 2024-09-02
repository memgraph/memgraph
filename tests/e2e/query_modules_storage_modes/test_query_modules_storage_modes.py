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

import struct
import sys
from multiprocessing import Manager, Process

import pytest
from common import connect, cursor

SWITCH_TO_ANALYTICAL = "STORAGE MODE IN_MEMORY_ANALYTICAL;"


def connect_and_execute(query):
    cursor = connect()
    cursor.execute(query)


def execute_and_compare(query, check_correctness, success):
    cursor = connect()
    cursor.execute(query)
    success.value = check_correctness(cursor.fetchall())


def complete_execution_on_new_thread(query):
    process = Process(target=connect_and_execute, args=(query,))
    process.start()
    process.join()


def execute_test(cursor, write_query, read_query, api, check_correctness):
    manager = Manager()
    success = manager.Value(bool, False)

    reader = Process(
        target=execute_and_compare,
        args=(read_query, check_correctness, success),
    )
    writer = Process(
        target=connect_and_execute,
        args=(write_query,),
    )

    reader.start()
    writer.start()

    writer.join()
    reader.join()

    cursor.execute(f"CALL {api}_api.reset()")

    return success.value


@pytest.mark.parametrize("api", ["c", "cpp", "python"])
def test_function_delete_result(cursor, api):
    cursor.execute(SWITCH_TO_ANALYTICAL)

    write_query = (
        f"MATCH (m:Component {{id: 'A7422'}})-[e:PART_OF]->(n:Component {{id: '7X8X0'}}) CALL {api}_api.delete_edge(e);"
    )
    read_query = f"MATCH (m)-[e]->(n) RETURN {api}_api.pass_relationship(e);"
    check_correctness = lambda result: len(result) == 1 and len(result[0]) == 1 and result[0][0].type == "DEPENDS_ON"

    result = execute_test(cursor, write_query, read_query, api, check_correctness)
    assert result


@pytest.mark.parametrize("api", ["c", "cpp", "python"])
def test_function_delete_only_result(cursor, api):
    cursor.execute(SWITCH_TO_ANALYTICAL)
    cursor.execute("MATCH (m:Component {id: '7X8X0'})-[e:DEPENDS_ON]->(n:Component {id: 'A7422'}) DELETE e;")

    write_query = (
        f"MATCH (m:Component {{id: 'A7422'}})-[e:PART_OF]->(n:Component {{id: '7X8X0'}}) CALL {api}_api.delete_edge(e);"
    )
    read_query = f"MATCH (m)-[e]->(n) RETURN {api}_api.pass_relationship(e);"
    check_correctness = lambda result: len(result) == 1 and len(result[0]) == 1 and result[0][0] is None

    result = execute_test(cursor, write_query, read_query, api, check_correctness)
    assert result


@pytest.mark.parametrize("api", ["c", "cpp", "python"])
def test_procedure_delete_result(cursor, api):
    cursor.execute(SWITCH_TO_ANALYTICAL)

    write_query = f"MATCH (n {{id: 'A7422'}}) CALL {api}_api.delete_vertex(n);"
    read_query = f"""MATCH (n)
        CALL {api}_api.pass_node_with_id(n)
        YIELD node, id
        RETURN node, id;"""
    check_correctness = (
        lambda result: len(result) == 1 and len(result[0]) == 2 and result[0][0].properties["id"] == "7X8X0"
    )

    result = execute_test(cursor, write_query, read_query, api, check_correctness)
    assert result


@pytest.mark.parametrize("api", ["c", "cpp", "python"])
def test_procedure_delete_only_result(cursor, api):
    cursor.execute(SWITCH_TO_ANALYTICAL)
    cursor.execute("MATCH (n {id: '7X8X0'}) DETACH DELETE n;")

    write_query = f"MATCH (n {{id: 'A7422'}}) CALL {api}_api.delete_vertex(n);"
    read_query = f"MATCH (n) CALL {api}_api.pass_node_with_id(n) YIELD node, id RETURN node, id;"
    check_correctness = lambda result: len(result) == 0

    result = execute_test(cursor, write_query, read_query, api, check_correctness)
    assert result


def test_deleted_node(cursor):
    cursor.execute(SWITCH_TO_ANALYTICAL)

    write_query = "MATCH (n:Component {id: 'A7422'}) CALL python_api.delete_vertex(n);"
    read_query = "MATCH (n: Component {id: 'A7422'}) RETURN python_api.pass_node(n);"
    check_correctness = lambda result: len(result) == 1 and len(result[0]) == 1 and result[0][0] is None

    result = execute_test(cursor, write_query, read_query, "python", check_correctness)
    assert result


def test_deleted_relationship(cursor):
    cursor.execute(SWITCH_TO_ANALYTICAL)

    write_query = (
        "MATCH (:Component {id: 'A7422'})-[e:PART_OF]->(:Component {id: '7X8X0'}) CALL python_api.delete_edge(e);"
    )
    read_query = "MATCH (:Component {id: 'A7422'})-[e:PART_OF]->(:Component {id: '7X8X0'}) RETURN python_api.pass_relationship(e);"
    check_correctness = lambda result: len(result) == 1 and len(result[0]) == 1 and result[0][0] is None

    result = execute_test(cursor, write_query, read_query, "python", check_correctness)
    assert result


def test_deleted_node_in_path(cursor):
    cursor.execute(SWITCH_TO_ANALYTICAL)

    write_query = "MATCH (n:Component {id: 'A7422'}) CALL python_api.delete_vertex(n);"
    read_query = "MATCH path=(n {id: 'A7422'})-[e]->(m) RETURN python_api.pass_path(path);"
    check_correctness = lambda result: len(result) == 1 and len(result[0]) == 1 and result[0][0] is None

    result = execute_test(cursor, write_query, read_query, "python", check_correctness)
    assert result


def test_deleted_relationship_in_path(cursor):
    cursor.execute(SWITCH_TO_ANALYTICAL)

    write_query = (
        "MATCH (:Component {id: 'A7422'})-[e:PART_OF]->(:Component {id: '7X8X0'}) CALL python_api.delete_edge(e);"
    )
    read_query = "MATCH path=(n {id: 'A7422'})-[e]->(m) RETURN python_api.pass_path(path);"
    check_correctness = lambda result: len(result) == 1 and len(result[0]) == 1 and result[0][0] is None

    result = execute_test(cursor, write_query, read_query, "python", check_correctness)
    assert result


def test_deleted_value_in_list(cursor):
    cursor.execute(SWITCH_TO_ANALYTICAL)

    write_query = (
        "MATCH (:Component {id: 'A7422'})-[e:PART_OF]->(:Component {id: '7X8X0'}) CALL python_api.delete_edge(e);"
    )
    read_query = "MATCH (n)-[e]->() WITH collect(n) + collect(e) as list RETURN python_api.pass_list(list);"
    check_correctness = lambda result: len(result) == 1 and len(result[0]) == 1 and result[0][0] is None

    result = execute_test(cursor, write_query, read_query, "python", check_correctness)
    assert result


def test_deleted_value_in_map(cursor):
    cursor.execute(SWITCH_TO_ANALYTICAL)

    write_query = (
        "MATCH (:Component {id: 'A7422'})-[e:PART_OF]->(:Component {id: '7X8X0'}) CALL python_api.delete_edge(e);"
    )
    read_query = (
        f"MATCH (n {{id: 'A7422'}})-[e]->() WITH {{node: n, relationship: e}} AS map RETURN python_api.pass_map(map);"
    )
    check_correctness = lambda result: len(result) == 1 and len(result[0]) == 1 and result[0][0] is None

    result = execute_test(cursor, write_query, read_query, "python", check_correctness)
    assert result


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
