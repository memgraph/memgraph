import sys

import pytest
from common import connect, execute_and_fetch_results_dict


def test_label():
    expected_results = {
        "name": True,
        "__eq__": True,
        "__ne__": True,
    }

    cursor = connect().cursor()
    results = execute_and_fetch_results_dict(
        cursor, "CALL label.compare_apis() YIELD results_dict RETURN results_dict;"
    )

    assert results == expected_results


def test_properties_on_vertex():
    expected_results = {
        "get": True,
        "get[default]": True,
        "set": True,
        "items": True,
        "keys": True,
        "values": True,
        "__len__": True,
        "__iter__": True,
        "__getitem__": True,
        "__setitem__": True,
        "__contains__": True,
    }

    cursor = connect().cursor()
    results = execute_and_fetch_results_dict(
        cursor, "CALL properties.compare_apis_on_vertex() YIELD results_dict RETURN results_dict;"
    )

    assert results == expected_results


def test_properties_on_edge():
    expected_results = {
        "get": True,
        "get[default]": True,
        "set": True,
        "items": True,
        "keys": True,
        "values": True,
        "__len__": True,
        "__iter__": True,
        "__getitem__": True,
        "__setitem__": True,
        "__contains__": True,
    }

    cursor = connect().cursor()
    results = execute_and_fetch_results_dict(
        cursor, "CALL properties.compare_apis_on_edge() YIELD results_dict RETURN results_dict;"
    )

    assert results == expected_results


def test_edge_type():
    expected_results = {
        "name": True,
        "__eq__": True,
        "__ne__": True,
    }

    cursor = connect().cursor()
    results = execute_and_fetch_results_dict(
        cursor, "CALL edge_type.compare_apis() YIELD results_dict RETURN results_dict;"
    )

    assert results == expected_results


def test_edge():
    expected_results = {
        "is_valid": True,
        "underlying_graph_is_mutable": True,
        "id": True,
        "type": True,
        "from_vertex": True,
        "to_vertex": True,
        "properties": True,
        "__eq__": True,
        "__ne__": True,
    }

    cursor = connect().cursor()
    results = execute_and_fetch_results_dict(cursor, "CALL edge.compare_apis() YIELD results_dict RETURN results_dict;")

    assert results == expected_results


def test_vertex():
    expected_results = {
        "is_valid": True,
        "underlying_graph_is_mutable": True,
        "id": True,
        "labels": True,
        "properties": True,
        "in_edges": True,
        "out_edges": True,
        "__eq__": True,
        "__ne__": True,
    }

    cursor = connect().cursor()
    results = execute_and_fetch_results_dict(
        cursor, "CALL vertex.compare_apis() YIELD results_dict RETURN results_dict;"
    )

    assert results == expected_results


def test_path():
    expected_results = {
        "__copy__": True,
        "is_valid": True,
        "expand": True,
        "pop": True,
        "vertices": True,
        "edges": True,
    }

    cursor = connect().cursor()
    results = execute_and_fetch_results_dict(cursor, "CALL path.compare_apis() YIELD results_dict RETURN results_dict;")

    assert results == expected_results


def test_record():
    expected_results = {
        "fields": True,
    }

    cursor = connect().cursor()
    results = execute_and_fetch_results_dict(
        cursor, "CALL record.compare_apis() YIELD results_dict RETURN results_dict;"
    )

    assert results == expected_results


def test_vertices():
    expected_results = {
        "is_valid": True,
        "__iter__": True,
        "__len__": True,
    }

    cursor = connect().cursor()
    results = execute_and_fetch_results_dict(
        cursor, "CALL vertices.compare_apis() YIELD results_dict RETURN results_dict;"
    )

    assert results == expected_results


def test_graph():
    expected_results = {
        "create_edge": True,
        "create_vertex": True,
        "delete_edge": True,
        "delete_vertex": True,
        "detach_delete_vertex": True,
        "edge_id_assignment": True,
        "get_vertex_by_id": True,
        "is_mutable": True,
        "is_not_mutable": True,
        "is_valid": True,
        "vertices": True,
    }

    cursor = connect().cursor()
    results = execute_and_fetch_results_dict(
        cursor, "CALL graph.compare_apis() YIELD results_dict RETURN results_dict;"
    )
    results.update(
        execute_and_fetch_results_dict(
            cursor, "CALL graph.test_read_proc_mutability() YIELD results_dict RETURN results_dict;"
        )
    )

    assert results == expected_results


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
