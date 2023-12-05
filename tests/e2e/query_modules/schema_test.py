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


def test_assert_index_wrong_properties_type():
    cursor = connect().cursor()
    execute_and_fetch_all(
        cursor,
        "CALL libschema.assert({Person: ''}, {}) YIELD * RETURN *;",
    )
    assert list(execute_and_fetch_all(cursor, "SHOW INDEX INFO;")) == []


def test_assert_property_is_not_a_string():
    cursor = connect().cursor()
    execute_and_fetch_all(
        cursor,
        "CALL libschema.assert({Person: ['name', 1]}, {}) YIELD * RETURN *;",
    )
    show_index_results = list(execute_and_fetch_all(cursor, "SHOW INDEX INFO;"))
    assert show_index_results == [("label+property", "Person", "name", 0)]
    execute_and_fetch_all(cursor, "DROP INDEX ON :Person(name);")


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
    assert list(execute_and_fetch_all(cursor, "SHOW INDEX INFO;")) == []
    show_constraint_results = list(execute_and_fetch_all(cursor, "SHOW CONSTRAINT INFO;"))
    assert show_constraint_results == [("exists", "Person", "name"), ("exists", "Person", "surname")]
    execute_and_fetch_all(cursor, "DROP CONSTRAINT ON (n:Person) ASSERT EXISTS (n.name);")
    execute_and_fetch_all(cursor, "DROP CONSTRAINT ON (n:Person) ASSERT EXISTS (n.surname);")


def test_assert_dropping_indices():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "CREATE INDEX ON :Person(name);")
    execute_and_fetch_all(cursor, "CREATE INDEX ON :Person(id);")
    execute_and_fetch_all(cursor, "CREATE INDEX ON :Ball(size);")
    execute_and_fetch_all(cursor, "CREATE INDEX ON :Ball;")
    results = list(execute_and_fetch_all(cursor, "CALL libschema.assert({}, {}) YIELD * RETURN *;"))
    assert len(results) == 4
    assert results[0] == ("Dropped", "", [], "Ball", False)
    assert results[1] == ("Dropped", "size", ["size"], "Ball", False)
    assert results[2] == ("Dropped", "id", ["id"], "Person", False)
    assert results[3] == ("Dropped", "name", ["name"], "Person", False)
    show_index_results = list(execute_and_fetch_all(cursor, "SHOW INDEX INFO;"))
    assert show_index_results == []


def test_assert_existence_constraint_properties_not_list():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "CALL libschema.assert({}, {}, {Person: 'name'}) YIELD * RETURN *;")
    assert list(execute_and_fetch_all(cursor, "SHOW CONSTRAINT INFO;")) == []


def test_assert_existence_constraint_property_not_string():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "CALL libschema.assert({}, {}, {Person: ['name', 1]}) YIELD * RETURN *;")
    show_constraint_results = list(execute_and_fetch_all(cursor, "SHOW CONSTRAINT INFO;"))
    assert show_constraint_results == [("exists", "Person", "name")]
    execute_and_fetch_all(cursor, "DROP CONSTRAINT ON (n:Person) ASSERT EXISTS (n.name);")


def test_assert_existence_constraint_property_empty_string():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "CALL libschema.assert({}, {}, {Person: ['']}) YIELD * RETURN *;")
    assert list(execute_and_fetch_all(cursor, "SHOW CONSTRAINT INFO;")) == []


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


def test_assert_drops_existence_constraints():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "CREATE CONSTRAINT ON (n:Person) ASSERT EXISTS (n.name);")
    execute_and_fetch_all(cursor, "CREATE CONSTRAINT ON (n:Person) ASSERT EXISTS (n.surname);")
    results = list(execute_and_fetch_all(cursor, "CALL libschema.assert({}, {}, {}) YIELD * RETURN *;"))
    assert len(results) == 2
    assert results[0] == ("Dropped", "name", ["name"], "Person", False)
    assert results[1] == ("Dropped", "surname", ["surname"], "Person", False)
    show_constraint_results = list(execute_and_fetch_all(cursor, "SHOW CONSTRAINT INFO;"))
    assert show_constraint_results == []


def test_assert_creates_unique_constraints():
    cursor = connect().cursor()
    results = list(
        execute_and_fetch_all(
            cursor,
            "CALL libschema.assert({}, {Person: [['name', 'surname']]}) YIELD * RETURN *;",
        )
    )
    assert results == [("Created", "[name, surname]", ["name", "surname"], "Person", True)]
    assert list(execute_and_fetch_all(cursor, "SHOW INDEX INFO;")) == []
    show_constraint_results = list(execute_and_fetch_all(cursor, "SHOW CONSTRAINT INFO;"))
    assert show_constraint_results == [("unique", "Person", ["name", "surname"])]
    execute_and_fetch_all(cursor, "DROP CONSTRAINT ON (n:Person) ASSERT n.name, n.surname IS UNIQUE;")


def test_assert_creates_multiple_unique_constraints():
    cursor = connect().cursor()
    results = list(
        execute_and_fetch_all(
            cursor,
            "CALL libschema.assert({}, {Person: [['name', 'surname'], ['id']]}) YIELD * RETURN *;",
        )
    )
    assert results == [
        ("Created", "[name, surname]", ["name", "surname"], "Person", True),
        ("Created", "[id]", ["id"], "Person", True),
    ]
    assert list(execute_and_fetch_all(cursor, "SHOW INDEX INFO;")) == []
    show_constraint_results = list(execute_and_fetch_all(cursor, "SHOW CONSTRAINT INFO;"))
    assert show_constraint_results == [("unique", "Person", ["name", "surname"]), ("unique", "Person", ["id"])]
    execute_and_fetch_all(cursor, "DROP CONSTRAINT ON (n:Person) ASSERT n.name, n.surname IS UNIQUE;")
    execute_and_fetch_all(cursor, "DROP CONSTRAINT ON (n:Person) ASSERT n.id IS UNIQUE;")


def test_assert_creates_unique_constraints_skip_invalid():
    cursor = connect().cursor()
    results = list(
        execute_and_fetch_all(
            cursor,
            "CALL libschema.assert({}, {Person: [['name', 'surname'], 'wrong_type']}) YIELD * RETURN *;",
        )
    )
    assert results == [("Created", "[name, surname]", ["name", "surname"], "Person", True)]
    assert list(execute_and_fetch_all(cursor, "SHOW INDEX INFO;")) == []
    show_constraint_results = list(execute_and_fetch_all(cursor, "SHOW CONSTRAINT INFO;"))
    assert show_constraint_results == [("unique", "Person", ["name", "surname"])]
    execute_and_fetch_all(cursor, "DROP CONSTRAINT ON (n:Person) ASSERT n.name, n.surname IS UNIQUE;")


def test_assert_creates_unique_constraints_skip_invalid_map_type():
    cursor = connect().cursor()
    results = list(
        execute_and_fetch_all(
            cursor,
            "CALL libschema.assert({}, {Person: [['name', 'surname']], Ball: 'wrong_type'}) YIELD * RETURN *;",
        )
    )
    assert results == [("Created", "[name, surname]", ["name", "surname"], "Person", True)]
    assert list(execute_and_fetch_all(cursor, "SHOW INDEX INFO;")) == []
    show_constraint_results = list(execute_and_fetch_all(cursor, "SHOW CONSTRAINT INFO;"))
    assert show_constraint_results == [("unique", "Person", ["name", "surname"])]
    execute_and_fetch_all(cursor, "DROP CONSTRAINT ON (n:Person) ASSERT n.name, n.surname IS UNIQUE;")


def test_assert_creates_constraints_and_indices():
    cursor = connect().cursor()
    results = list(
        execute_and_fetch_all(
            cursor,
            "CALL libschema.assert({Person: ['', 'id']}, {Person: [['name', 'surname'], ['id']]}, {Person: ['name', 'surname']}) YIELD * RETURN *;",
        )
    )
    assert len(results) == 6
    assert results[0] == ("Created", "", [], "Person", False)
    assert results[1] == ("Created", "id", ["id"], "Person", False)
    assert results[2] == ("Created", "name", ["name"], "Person", False)
    assert results[3] == ("Created", "surname", ["surname"], "Person", False)
    assert results[4] == ("Created", "[name, surname]", ["name", "surname"], "Person", True)
    assert results[5] == ("Created", "[id]", ["id"], "Person", True)
    show_index_results = list(execute_and_fetch_all(cursor, "SHOW INDEX INFO;"))
    assert show_index_results == [("label", "Person", None, 0), ("label+property", "Person", "id", 0)]
    show_constraint_results = list(execute_and_fetch_all(cursor, "SHOW CONSTRAINT INFO;"))
    assert show_constraint_results == [
        ("exists", "Person", "name"),
        ("exists", "Person", "surname"),
        ("unique", "Person", ["name", "surname"]),
        ("unique", "Person", ["id"]),
    ]
    execute_and_fetch_all(cursor, "DROP CONSTRAINT ON (n:Person) ASSERT n.name, n.surname IS UNIQUE;")
    execute_and_fetch_all(cursor, "DROP CONSTRAINT ON (n:Person) ASSERT n.id IS UNIQUE;")
    execute_and_fetch_all(cursor, "DROP INDEX ON :Person;")
    execute_and_fetch_all(cursor, "DROP INDEX ON :Person(id);")
    execute_and_fetch_all(cursor, "DROP CONSTRAINT ON (n:Person) ASSERT EXISTS (n.name);")
    execute_and_fetch_all(cursor, "DROP CONSTRAINT ON (n:Person) ASSERT EXISTS (n.surname);")


def test_assert_drops_unique_constraints():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "CREATE CONSTRAINT ON (n:Person) ASSERT n.name, n.surname IS UNIQUE;")
    execute_and_fetch_all(cursor, "CREATE CONSTRAINT ON (n:Person) ASSERT n.id IS UNIQUE;")
    results = list(execute_and_fetch_all(cursor, "CALL libschema.assert({}, {}, {}) YIELD * RETURN *;"))
    assert len(results) == 2
    assert results[0] == ("Dropped", "[id]", ["id"], "Person", True)
    assert results[1] == ("Dropped", "[name, surname]", ["name", "surname"], "Person", True)
    show_constraint_results = list(execute_and_fetch_all(cursor, "SHOW CONSTRAINT INFO;"))
    assert show_constraint_results == []


def test_assert_drops_indices_and_constraints():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "CREATE INDEX ON :Person;")
    execute_and_fetch_all(cursor, "CREATE INDEX ON :Person(id);")
    execute_and_fetch_all(cursor, "CREATE CONSTRAINT ON (n:Person) ASSERT n.name, n.surname IS UNIQUE;")
    execute_and_fetch_all(cursor, "CREATE CONSTRAINT ON (n:Person) ASSERT n.id IS UNIQUE;")
    execute_and_fetch_all(cursor, "CREATE CONSTRAINT ON (n:Person) ASSERT EXISTS (n.name);")
    execute_and_fetch_all(cursor, "CREATE CONSTRAINT ON (n:Person) ASSERT EXISTS (n.surname);")
    results = list(execute_and_fetch_all(cursor, "CALL libschema.assert({}, {}, {}) YIELD * RETURN *;"))
    assert len(results) == 6
    assert results[0] == ("Dropped", "", [], "Person", False)
    assert results[1] == ("Dropped", "id", ["id"], "Person", False)
    assert results[2] == ("Dropped", "name", ["name"], "Person", False)
    assert results[3] == ("Dropped", "surname", ["surname"], "Person", False)
    assert results[4] == ("Dropped", "[id]", ["id"], "Person", True)
    assert results[5] == ("Dropped", "[name, surname]", ["name", "surname"], "Person", True)
    show_index_results = list(execute_and_fetch_all(cursor, "SHOW INDEX INFO;"))
    assert show_index_results == []
    show_constraint_results = list(execute_and_fetch_all(cursor, "SHOW CONSTRAINT INFO;"))
    assert show_constraint_results == []


def test_assert_does_not_drop_indices_and_constraints():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "CREATE INDEX ON :Person;")
    execute_and_fetch_all(cursor, "CREATE INDEX ON :Person(id);")
    execute_and_fetch_all(cursor, "CREATE CONSTRAINT ON (n:Person) ASSERT n.name, n.surname IS UNIQUE;")
    execute_and_fetch_all(cursor, "CREATE CONSTRAINT ON (n:Person) ASSERT n.id IS UNIQUE;")
    execute_and_fetch_all(cursor, "CREATE CONSTRAINT ON (n:Person) ASSERT EXISTS (n.name);")
    execute_and_fetch_all(cursor, "CREATE CONSTRAINT ON (n:Person) ASSERT EXISTS (n.surname);")
    results = list(execute_and_fetch_all(cursor, "CALL libschema.assert({}, {}, {}, false) YIELD * RETURN *;"))
    assert len(results) == 0
    show_index_results = list(execute_and_fetch_all(cursor, "SHOW INDEX INFO;"))
    assert show_index_results == [("label", "Person", None, 0), ("label+property", "Person", "id", 0)]
    show_constraint_results = list(execute_and_fetch_all(cursor, "SHOW CONSTRAINT INFO;"))
    assert show_constraint_results == [
        ("exists", "Person", "name"),
        ("exists", "Person", "surname"),
        ("unique", "Person", ["name", "surname"]),
        ("unique", "Person", ["id"]),
    ]
    execute_and_fetch_all(cursor, "DROP INDEX ON :Person;")
    execute_and_fetch_all(cursor, "DROP INDEX ON :Person(id);")
    execute_and_fetch_all(cursor, "DROP CONSTRAINT ON (n:Person) ASSERT n.name, n.surname IS UNIQUE;")
    execute_and_fetch_all(cursor, "DROP CONSTRAINT ON (n:Person) ASSERT n.id IS UNIQUE;")
    execute_and_fetch_all(cursor, "DROP CONSTRAINT ON (n:Person) ASSERT EXISTS (n.name);")
    execute_and_fetch_all(cursor, "DROP CONSTRAINT ON (n:Person) ASSERT EXISTS (n.surname);")


def test_assert_keeps_existing_indices_and_constraints():
    cursor = connect().cursor()
    assert list(execute_and_fetch_all(cursor, "SHOW INDEX INFO;")) == []
    assert list(execute_and_fetch_all(cursor, "SHOW CONSTRAINT INFO;")) == []
    execute_and_fetch_all(cursor, "CREATE INDEX ON :Person;")
    execute_and_fetch_all(cursor, "CREATE INDEX ON :Person(id);")
    execute_and_fetch_all(cursor, "CREATE CONSTRAINT ON (n:Person) ASSERT n.name, n.surname IS UNIQUE;")
    execute_and_fetch_all(cursor, "CREATE CONSTRAINT ON (n:Person) ASSERT n.id IS UNIQUE;")
    execute_and_fetch_all(cursor, "CREATE CONSTRAINT ON (n:Person) ASSERT EXISTS (n.name);")
    execute_and_fetch_all(cursor, "CREATE CONSTRAINT ON (n:Person) ASSERT EXISTS (n.surname);")
    results = list(
        execute_and_fetch_all(
            cursor,
            "CALL libschema.assert({Person: ['id']}, {Person: [['name', 'surname']]}, {Person: ['name']}) YIELD * RETURN *;",
        )
    )

    print(results)

    assert len(results) == 6

    assert results[0] == ("Kept", "id", ["id"], "Person", False)  # label+property index on Person(id) should be kept
    assert results[1] == ("Dropped", "", [], "Person", False)  # label index on Person should be deleted
    assert results[2] == (
        "Kept",
        "name",
        ["name"],
        "Person",
        False,
    )  # existence constraint on Person(name) should be kept
    assert results[3] == (
        "Dropped",
        "surname",
        ["surname"],
        "Person",
        False,
    )  # existence constraint on surname should be deleted
    assert results[4] == (
        "Kept",
        "[name, surname]",
        ["name", "surname"],
        "Person",
        True,
    )  # unique constraint on Person(name, surname) should be kept
    assert results[5] == (
        "Dropped",
        "[id]",
        ["id"],
        "Person",
        True,
    )  # unique constraint on Person(id) should be deleted


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
    assert (result) == [":`Activity`", ["Activity"], "location", ["String"], False]

    result = list(
        execute_and_fetch_all(
            cursor,
            f"CALL libschema.node_type_properties() YIELD nodeType, nodeLabels, propertyName, propertyTypes , mandatory RETURN nodeType, nodeLabels, propertyName, propertyTypes , mandatory ORDER BY propertyName, nodeLabels[0];",
        )[1]
    )
    assert (result) == [":`Activity`", ["Activity"], "name", ["String"], False]

    result = list(
        execute_and_fetch_all(
            cursor,
            f"CALL libschema.node_type_properties() YIELD nodeType, nodeLabels, propertyName, propertyTypes , mandatory RETURN nodeType, nodeLabels, propertyName, propertyTypes , mandatory ORDER BY propertyName, nodeLabels[0];",
        )[2]
    )
    assert (result) == [":`Dog`", ["Dog"], "name", ["String"], False]

    result = list(
        execute_and_fetch_all(
            cursor,
            f"CALL libschema.node_type_properties() YIELD nodeType, nodeLabels, propertyName, propertyTypes , mandatory RETURN nodeType, nodeLabels, propertyName, propertyTypes , mandatory ORDER BY propertyName, nodeLabels[0];",
        )[3]
    )
    assert (result) == [":`Dog`", ["Dog"], "owner", ["String"], False]


def test_node_type_properties2():
    cursor = connect().cursor()
    execute_and_fetch_all(
        cursor,
        """
        CREATE (d:MyNode)
        CREATE (n:MyNode)
        """,
    )
    result = execute_and_fetch_all(
        cursor,
        f"CALL libschema.node_type_properties() YIELD nodeType, nodeLabels, propertyName, propertyTypes , mandatory RETURN nodeType, nodeLabels, propertyName, propertyTypes , mandatory ORDER BY propertyName, nodeLabels[0];",
    )
    assert (list(result[0])) == [":`MyNode`", ["MyNode"], "", "", False]
    assert (result.__len__()) == 1


def test_node_type_properties3():
    cursor = connect().cursor()
    execute_and_fetch_all(
        cursor,
        """
        CREATE (d:Dog {name: 'Rex', owner: 'Carl'})
        CREATE (n:Dog)
        """,
    )
    result = execute_and_fetch_all(
        cursor,
        f"CALL libschema.node_type_properties() YIELD nodeType, nodeLabels, propertyName, propertyTypes , mandatory RETURN nodeType, nodeLabels, propertyName, propertyTypes , mandatory ORDER BY propertyName, nodeLabels[0];",
    )

    assert (list(result[0])) == [":`Dog`", ["Dog"], "name", ["String"], False]
    assert (list(result[1])) == [":`Dog`", ["Dog"], "owner", ["String"], False]
    assert (result.__len__()) == 2


def test_node_type_properties4():
    cursor = connect().cursor()
    execute_and_fetch_all(
        cursor,
        """
        CREATE (n:Label1:Label2 {property1: 'value1', property2: 'value2'})
        CREATE (m:Label2:Label1 {property3: 'value3'})
        """,
    )
    result = list(
        execute_and_fetch_all(
            cursor,
            f"CALL libschema.node_type_properties() YIELD nodeType, nodeLabels, propertyName, propertyTypes , mandatory RETURN nodeType, nodeLabels, propertyName, propertyTypes , mandatory ORDER BY propertyName, nodeLabels[0];",
        )
    )
    assert (list(result[0])) == [":`Label1`:`Label2`", ["Label1", "Label2"], "property1", ["String"], False]
    assert (list(result[1])) == [":`Label1`:`Label2`", ["Label1", "Label2"], "property2", ["String"], False]
    assert (list(result[2])) == [":`Label1`:`Label2`", ["Label1", "Label2"], "property3", ["String"], False]
    assert (result.__len__()) == 3


def test_node_type_properties5():
    cursor = connect().cursor()
    execute_and_fetch_all(
        cursor,
        """
        CREATE (d:Dog {name: 'Rex'})
        """,
    )
    result = execute_and_fetch_all(
        cursor,
        f"CALL libschema.node_type_properties() YIELD nodeType, nodeLabels, propertyName, propertyTypes , mandatory RETURN nodeType, nodeLabels, propertyName, propertyTypes , mandatory ORDER BY propertyName, nodeLabels[0];",
    )

    assert (list(result[0])) == [":`Dog`", ["Dog"], "name", ["String"], True]
    assert (result.__len__()) == 1


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


def test_rel_type_properties2():
    cursor = connect().cursor()
    execute_and_fetch_all(
        cursor,
        """
        CREATE (d:Dog {name: 'Rex', owner: 'Carl'})-[l:LOVES]->(a:Activity {name: 'Running', location: 'Zadar'})
        CREATE (n:Dog {name: 'Simba', owner: 'Lucy'})-[j:LOVES {duration: 30}]->(b:Activity {name: 'Running', location: 'Zadar'})
        """,
    )
    result = execute_and_fetch_all(
        cursor,
        f"CALL libschema.rel_type_properties() YIELD relType,propertyName, propertyTypes , mandatory RETURN relType, propertyName, propertyTypes , mandatory;",
    )
    assert (list(result[0])) == [":`LOVES`", "duration", ["Int"], False]
    assert (result.__len__()) == 1


def test_rel_type_properties3():
    cursor = connect().cursor()
    execute_and_fetch_all(
        cursor,
        """
        CREATE (n:Dog {name: 'Simba', owner: 'Lucy'})-[j:LOVES {duration: 30}]->(b:Activity {name: 'Running', location: 'Zadar'})
        """,
    )
    result = execute_and_fetch_all(
        cursor,
        f"CALL libschema.rel_type_properties() YIELD relType,propertyName, propertyTypes , mandatory RETURN relType, propertyName, propertyTypes , mandatory;",
    )
    assert (list(result[0])) == [":`LOVES`", "duration", ["Int"], True]
    assert (result.__len__()) == 1


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
