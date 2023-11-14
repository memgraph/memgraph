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


def test_assert_creates_label_index_empty_list():
    cursor = connect().cursor()
    results = list(
        execute_and_fetch_all(
            cursor,
            "CALL libschema.assert({Person: []}, {}) YIELD * RETURN *;",
        )
    )
    assert results == [("Created", "", [], "Person", False)]
    show_index_results = list(execute_and_fetch_all(cursor, "SHOW INDEX INFO;"))
    assert show_index_results == [("label", "Person", None, 0)]
    execute_and_fetch_all(cursor, "DROP INDEX ON :Person;")


def test_assert_creates_label_index_empty_string():
    cursor = connect().cursor()
    results = list(
        execute_and_fetch_all(
            cursor,
            "CALL libschema.assert({Person: ['']}, {}) YIELD * RETURN *;",
        )
    )
    assert results == [("Created", "", [], "Person", False)]
    show_index_results = list(execute_and_fetch_all(cursor, "SHOW INDEX INFO;"))
    assert show_index_results == [("label", "Person", None, 0)]
    execute_and_fetch_all(cursor, "DROP INDEX ON :Person;")


def test_assert_wrong_properties_type():
    cursor = connect().cursor()
    try:
        execute_and_fetch_all(
            cursor,
            "CALL libschema.assert({Person: " "}, {}) YIELD * RETURN *;",
        )
    except Exception:
        show_index_results = list(execute_and_fetch_all(cursor, "SHOW INDEX INFO;"))
        assert show_index_results == []
        return
    assert False


def test_assert_property_is_not_a_string():
    cursor = connect().cursor()
    try:
        execute_and_fetch_all(
            cursor,
            "CALL libschema.assert({Person: ['name', 1] {}) YIELD * RETURN *;",
        )
    except Exception:
        show_index_results = list(execute_and_fetch_all(cursor, "SHOW INDEX INFO;"))
        assert show_index_results == []
        return
    assert False


def test_assert_creates_label_index_multiple_empty_strings():
    cursor = connect().cursor()
    results = list(
        execute_and_fetch_all(
            cursor,
            "CALL libschema.assert({Person: ['', '', '', '']}, {}) YIELD * RETURN *;",
        )
    )
    assert results == [("Created", "", [], "Person", False)]
    show_index_results = list(execute_and_fetch_all(cursor, "SHOW INDEX INFO;"))
    assert show_index_results == [("label", "Person", None, 0)]
    execute_and_fetch_all(cursor, "DROP INDEX ON :Person;")


def test_assert_creates_label_property_index():
    cursor = connect().cursor()
    results = list(
        execute_and_fetch_all(
            cursor,
            "CALL libschema.assert({Person: ['name']}, {}) YIELD * RETURN *;",
        )
    )
    assert results == [("Created", "name", ["name"], "Person", False)]
    show_index_results = list(execute_and_fetch_all(cursor, "SHOW INDEX INFO;"))
    assert show_index_results == [("label+property", "Person", "name", 0)]
    execute_and_fetch_all(cursor, "DROP INDEX ON :Person(name);")


def test_assert_creates_multiple_indices():
    cursor = connect().cursor()
    results = list(
        execute_and_fetch_all(
            cursor,
            "CALL libschema.assert({Person: ['', 'id', 'name'], Ball: ['', 'size', 'size', '']}, {}) YIELD * RETURN *;",
        )
    )
    assert len(results) == 5
    assert results[0] == ("Created", "", [], "Ball", False)
    assert results[1] == ("Created", "size", ["size"], "Ball", False)
    assert results[2] == ("Created", "", [], "Person", False)
    assert results[3] == ("Created", "id", ["id"], "Person", False)
    assert results[4] == ("Created", "name", ["name"], "Person", False)
    show_index_results = list(execute_and_fetch_all(cursor, "SHOW INDEX INFO;"))
    assert len(show_index_results) == 5
    assert show_index_results[0] == ("label", "Ball", None, 0)
    assert show_index_results[1] == ("label", "Person", None, 0)
    assert show_index_results[2] == ("label+property", "Ball", "size", 0)
    assert show_index_results[3] == ("label+property", "Person", "id", 0)
    assert show_index_results[4] == ("label+property", "Person", "name", 0)
    execute_and_fetch_all(cursor, "DROP INDEX ON :Person;")
    execute_and_fetch_all(cursor, "DROP INDEX ON :Person(id);")
    execute_and_fetch_all(cursor, "DROP INDEX ON :Person(name);")
    execute_and_fetch_all(cursor, "DROP INDEX ON :Ball;")
    execute_and_fetch_all(cursor, "DROP INDEX ON :Ball(size);")


def test_assert_creates_existence_constraints():
    cursor = connect().cursor()
    results = list(
        execute_and_fetch_all(
            cursor,
            "CALL libschema.assert({}, {}, {Person: ['name', 'surname']}) YIELD * RETURN *;",
        )
    )
    assert results == [
        ("Created", "name", ["name"], "Person", False),
        ("Created", "surname", ["surname"], "Person", False),
    ]
    show_index_results = list(execute_and_fetch_all(cursor, "SHOW INDEX INFO;"))
    assert show_index_results == []
    show_constraint_results = list(execute_and_fetch_all(cursor, "SHOW CONSTRAINT INFO;"))
    assert show_constraint_results == [("exists", "Person", "name"), ("exists", "Person", "surname")]
    execute_and_fetch_all(cursor, "DROP CONSTRAINT ON (n:Person) ASSERT EXISTS (n.name);")
    execute_and_fetch_all(cursor, "DROP CONSTRAINT ON (n:Person) ASSERT EXISTS (n.surname);")


def test_assert_existence_constraint_properties_not_list():
    cursor = connect().cursor()
    try:
        execute_and_fetch_all(cursor, "CALL libschema.assert({}, {}, {Person: 'name'}) YIELD * RETURN *;")
    except Exception:
        show_constraint_results = list(execute_and_fetch_all(cursor, "SHOW CONSTRAINT INFO;"))
        assert show_constraint_results == []
        return
    assert False


def test_assert_existence_constraint_property_not_string():
    cursor = connect().cursor()
    try:
        execute_and_fetch_all(cursor, "CALL libschema.assert({}, {}, {Person: ['name', 1]}) YIELD * RETURN *;")
    except Exception:
        show_constraint_results = list(execute_and_fetch_all(cursor, "SHOW CONSTRAINT INFO;"))
        assert show_constraint_results == []
        return
    assert False


def test_assert_existence_constraint_property_empty_string():
    cursor = connect().cursor()
    try:
        execute_and_fetch_all(cursor, "CALL libschema.assert({}, {}, {Person: ['']}) YIELD * RETURN *;")
    except Exception:
        show_constraint_results = list(execute_and_fetch_all(cursor, "SHOW CONSTRAINT INFO;"))
        assert show_constraint_results == []
        return
    assert False


def test_assert_existence_constraint_property_not_string():
    cursor = connect().cursor()
    try:
        execute_and_fetch_all(cursor, "CALL libschema.assert({}, {}, {Person: ['name', 1]}) YIELD * RETURN *;")
    except Exception:
        show_constraint_results = list(execute_and_fetch_all(cursor, "SHOW CONSTRAINT INFO;"))
        assert show_constraint_results == [("exists", "Person", "name")]
        execute_and_fetch_all(cursor, "DROP CONSTRAINT ON (n:Person) ASSERT EXISTS (n.name);")
        return
    assert False


def test_assert_creates_indices_and_existence_constraints():
    cursor = connect().cursor()
    results = list(
        execute_and_fetch_all(
            cursor,
            "CALL libschema.assert({Person: ['', 'id']}, {}, {Person: ['name', 'surname']}) YIELD * RETURN *;",
        )
    )
    assert len(results) == 4
    assert results[0] == ("Created", "", [], "Person", False)
    assert results[1] == ("Created", "id", ["id"], "Person", False)
    assert results[2] == ("Created", "name", ["name"], "Person", False)
    assert results[3] == ("Created", "surname", ["surname"], "Person", False)
    show_index_results = list(execute_and_fetch_all(cursor, "SHOW INDEX INFO;"))
    assert show_index_results == [("label", "Person", None, 0), ("label+property", "Person", "id", 0)]
    show_constraint_results = list(execute_and_fetch_all(cursor, "SHOW CONSTRAINT INFO;"))
    assert show_constraint_results == [("exists", "Person", "name"), ("exists", "Person", "surname")]
    execute_and_fetch_all(cursor, "DROP INDEX ON :Person;")
    execute_and_fetch_all(cursor, "DROP INDEX ON :Person(id);")
    execute_and_fetch_all(cursor, "DROP CONSTRAINT ON (n:Person) ASSERT EXISTS (n.name);")
    execute_and_fetch_all(cursor, "DROP CONSTRAINT ON (n:Person) ASSERT EXISTS (n.surname);")


def test_node_type_properties1():
    cursor = connect().cursor()
    execute_and_fetch_all(
        cursor,
        "CREATE (d:Dog {name: 'Rex', owner: 'Carl'})-[l:LOVES]->(a:Activity {name: 'Running', location: 'Zadar'})",
    )
    result = list(
        execute_and_fetch_all(
            cursor,
            f"CALL libschema.node_type_properties() YIELD nodeType, nodeLabels, propertyName, propertyTypes , mandatory RETURN nodeType, nodeLabels, propertyName, propertyTypes , mandatory ORDER BY propertyName, nodeLabels[0];",
        )[0]
    )
    assert (result) == [":`Activity`", ["Activity"], "location", ["String"], True]

    result = list(
        execute_and_fetch_all(
            cursor,
            f"CALL libschema.node_type_properties() YIELD nodeType, nodeLabels, propertyName, propertyTypes , mandatory RETURN nodeType, nodeLabels, propertyName, propertyTypes , mandatory ORDER BY propertyName, nodeLabels[0];",
        )[1]
    )
    assert (result) == [":`Activity`", ["Activity"], "name", ["String"], True]

    result = list(
        execute_and_fetch_all(
            cursor,
            f"CALL libschema.node_type_properties() YIELD nodeType, nodeLabels, propertyName, propertyTypes , mandatory RETURN nodeType, nodeLabels, propertyName, propertyTypes , mandatory ORDER BY propertyName, nodeLabels[0];",
        )[2]
    )
    assert (result) == [":`Dog`", ["Dog"], "name", ["String"], True]

    result = list(
        execute_and_fetch_all(
            cursor,
            f"CALL libschema.node_type_properties() YIELD nodeType, nodeLabels, propertyName, propertyTypes , mandatory RETURN nodeType, nodeLabels, propertyName, propertyTypes , mandatory ORDER BY propertyName, nodeLabels[0];",
        )[3]
    )
    assert (result) == [":`Dog`", ["Dog"], "owner", ["String"], True]


def test_rel_type_properties1():
    cursor = connect().cursor()
    execute_and_fetch_all(
        cursor,
        "CREATE (d:Dog {name: 'Rex', owner: 'Carl'})-[l:LOVES]->(a:Activity {name: 'Running', location: 'Zadar'})",
    )
    result = list(
        execute_and_fetch_all(
            cursor,
            f"CALL libschema.rel_type_properties() YIELD relType,propertyName, propertyTypes , mandatory RETURN relType, propertyName, propertyTypes , mandatory;",
        )[0]
    )
    assert (result) == [":`LOVES`", "", "", False]


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
