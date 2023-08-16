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
import typing

import mgclient
import pytest
from common import execute_and_fetch_all, has_n_result_row, has_one_result_row


@pytest.fixture(scope="function")
def multi_db(request, connection):
    cursor = connection.cursor()
    if request.param:
        execute_and_fetch_all(cursor, "CREATE DATABASE clean")
        execute_and_fetch_all(cursor, "USE DATABASE clean")
        execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n")
    pass
    yield connection


@pytest.mark.parametrize("multi_db", [False, True], indirect=True)
def test_is_write(multi_db):
    is_write = 2
    result_order = "name, signature, is_write"
    cursor = multi_db.cursor()
    for proc in execute_and_fetch_all(
        cursor,
        "CALL mg.procedures() YIELD * WITH name, signature, "
        "is_write WHERE name STARTS WITH 'write' "
        f"RETURN {result_order}",
    ):
        assert proc[is_write] is True

    for proc in execute_and_fetch_all(
        cursor,
        "CALL mg.procedures() YIELD * WITH name, signature, "
        "is_write WHERE NOT name STARTS WITH 'write' "
        f"RETURN {result_order}",
    ):
        assert proc[is_write] is False

    assert cursor.description[0].name == "name"
    assert cursor.description[1].name == "signature"
    assert cursor.description[2].name == "is_write"


@pytest.mark.parametrize("multi_db", [False, True], indirect=True)
def test_single_vertex(multi_db):
    cursor = multi_db.cursor()
    assert has_n_result_row(cursor, "MATCH (n) RETURN n", 0)
    result = execute_and_fetch_all(cursor, "CALL write.create_vertex() YIELD v RETURN v")
    vertex = result[0][0]
    assert isinstance(vertex, mgclient.Node)
    assert has_one_result_row(cursor, "MATCH (n) RETURN n")
    assert vertex.labels == set()
    assert vertex.properties == {}

    def add_label(label: str):
        execute_and_fetch_all(cursor, f"MATCH (n) CALL write.add_label(n, '{label}') " "YIELD * RETURN *")

    def remove_label(label: str):
        execute_and_fetch_all(cursor, f"MATCH (n) CALL write.remove_label(n, '{label}') " "YIELD * RETURN *")

    def get_vertex() -> mgclient.Node:
        return execute_and_fetch_all(cursor, "MATCH (n) RETURN n")[0][0]

    def set_property(property_name: str, property: typing.Any):
        nonlocal cursor
        execute_and_fetch_all(
            cursor,
            f"MATCH (n) CALL write.set_property(n, '{property_name}', " "$property) YIELD * RETURN *",
            {"property": property},
        )

    label_1 = "LABEL1"
    label_2 = "LABEL2"

    add_label(label_1)
    assert get_vertex().labels == {label_1}
    add_label(label_1)
    assert get_vertex().labels == {label_1}
    add_label(label_2)
    assert get_vertex().labels == {label_1, label_2}
    remove_label(label_1)
    assert get_vertex().labels == {label_2}
    property_name = "prop"
    property_value_1 = 1
    property_value_2 = [42, 24]
    set_property(property_name, property_value_1)
    assert get_vertex().properties == {property_name: property_value_1}
    set_property(property_name, property_value_2)
    assert get_vertex().properties == {property_name: property_value_2}
    set_property(property_name, None)
    assert get_vertex().properties == {}

    execute_and_fetch_all(cursor, "MATCH (n) CALL write.delete_vertex(n) YIELD * RETURN 1")
    assert has_n_result_row(cursor, "MATCH (n) RETURN n", 0)


@pytest.mark.parametrize("multi_db", [False, True], indirect=True)
def test_single_edge(multi_db):
    cursor = multi_db.cursor()
    assert has_n_result_row(cursor, "MATCH (n) RETURN n", 0)
    v1_id = execute_and_fetch_all(cursor, "CALL write.create_vertex() YIELD v RETURN v")[0][0].id
    v2_id = execute_and_fetch_all(cursor, "CALL write.create_vertex() YIELD v RETURN v")[0][0].id
    edge_type = "EDGE"
    edge = execute_and_fetch_all(
        cursor,
        f"MATCH (n) WHERE id(n) = {v1_id} "
        f"MATCH (m) WHERE id(m) = {v2_id} "
        f"CALL write.create_edge(n, m, '{edge_type}') "
        "YIELD e RETURN e",
    )[0][0]

    assert edge.type == edge_type
    assert edge.properties == {}
    property_name = "very_looong_prooooperty_naaame"
    property_value_1 = {"a": 1, "b": 3.4, "c": [666]}
    property_value_2 = 64

    def get_edge() -> mgclient.Node:
        return execute_and_fetch_all(cursor, "MATCH ()-[e]->() RETURN e")[0][0]

    def set_property(property_name: str, property: typing.Any):
        nonlocal cursor
        execute_and_fetch_all(
            cursor,
            "MATCH ()-[e]->() " f"CALL write.set_property(e, '{property_name}', " "$property) YIELD * RETURN *",
            {"property": property},
        )

    set_property(property_name, property_value_1)
    assert get_edge().properties == {property_name: property_value_1}
    set_property(property_name, property_value_2)
    assert get_edge().properties == {property_name: property_value_2}
    set_property(property_name, None)
    assert get_edge().properties == {}
    execute_and_fetch_all(cursor, "MATCH ()-[e]->() CALL write.delete_edge(e) YIELD * RETURN 1")
    assert has_n_result_row(cursor, "MATCH ()-[e]->() RETURN e", 0)


@pytest.mark.parametrize("multi_db", [False, True], indirect=True)
def test_detach_delete_vertex(multi_db):
    cursor = multi_db.cursor()
    assert has_n_result_row(cursor, "MATCH (n) RETURN n", 0)
    v1_id = execute_and_fetch_all(cursor, "CALL write.create_vertex() YIELD v RETURN v")[0][0].id
    v2_id = execute_and_fetch_all(cursor, "CALL write.create_vertex() YIELD v RETURN v")[0][0].id
    execute_and_fetch_all(
        cursor,
        f"MATCH (n) WHERE id(n) = {v1_id} "
        f"MATCH (m) WHERE id(m) = {v2_id} "
        f"CALL write.create_edge(n, m, 'EDGE') "
        "YIELD e RETURN e",
    )

    assert has_one_result_row(cursor, "MATCH (n)-[e]->(m) RETURN n, e, m")
    execute_and_fetch_all(
        cursor, f"MATCH (n) WHERE id(n) = {v1_id} " "CALL write.detach_delete_vertex(n) YIELD * RETURN 1"
    )
    assert has_n_result_row(cursor, "MATCH (n)-[e]->(m) RETURN n, e, m", 0)
    assert has_n_result_row(cursor, "MATCH ()-[e]->() RETURN e", 0)
    assert has_one_result_row(cursor, f"MATCH (n) WHERE id(n) = {v2_id} RETURN n")


@pytest.mark.parametrize("multi_db", [False, True], indirect=True)
def test_graph_mutability(multi_db):
    cursor = multi_db.cursor()
    assert has_n_result_row(cursor, "MATCH (n) RETURN n", 0)
    v1_id = execute_and_fetch_all(cursor, "CALL write.create_vertex() YIELD v RETURN v")[0][0].id
    v2_id = execute_and_fetch_all(cursor, "CALL write.create_vertex() YIELD v RETURN v")[0][0].id
    execute_and_fetch_all(
        cursor,
        f"MATCH (n) WHERE id(n) = {v1_id} "
        f"MATCH (m) WHERE id(m) = {v2_id} "
        f"CALL write.create_edge(n, m, 'EDGE') "
        "YIELD e RETURN e",
    )

    def test_mutability(is_write: bool):
        module = "write" if is_write else "read"
        assert (
            execute_and_fetch_all(cursor, f"CALL {module}.graph_is_mutable() " "YIELD mutable RETURN mutable")[0][0]
            is is_write
        )
        assert (
            execute_and_fetch_all(
                cursor, "MATCH (n) " f"CALL {module}.underlying_graph_is_mutable(n) " "YIELD mutable RETURN mutable"
            )[0][0]
            is is_write
        )
        assert (
            execute_and_fetch_all(
                cursor,
                "MATCH (n)-[e]->(m) " f"CALL {module}.underlying_graph_is_mutable(e) " "YIELD mutable RETURN mutable",
            )[0][0]
            is is_write
        )

    test_mutability(True)
    test_mutability(False)


@pytest.mark.parametrize("multi_db", [False, True], indirect=True)
def test_log_message(multi_db):
    cursor = multi_db.cursor()
    success = execute_and_fetch_all(cursor, f"CALL read.log_message('message') YIELD success RETURN success")[0][0]
    assert (success) is True


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
