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


def test_assert_fails_in_explicit_txn():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "BEGIN")
    with pytest.raises(Exception) as e:
        execute_and_fetch_all(cursor, "CALL schema.assert({}, {}, {}, true) YIELD * RETURN *;")
    assert str(e.value).startswith("Schema-related procedures call is not allowed in multicommand transactions.")


def test_assert_creates_label_index_empty_list():
    cursor = connect().cursor()
    results = list(
        execute_and_fetch_all(
            cursor,
            "CALL schema.assert({Person: []}, {}) YIELD * RETURN *;",
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
            "CALL schema.assert({Person: ['']}, {}) YIELD * RETURN *;",
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
        "CALL schema.assert({Person: ''}, {}) YIELD * RETURN *;",
    )
    assert list(execute_and_fetch_all(cursor, "SHOW INDEX INFO;")) == []


def test_assert_property_is_not_a_string():
    cursor = connect().cursor()
    execute_and_fetch_all(
        cursor,
        "CALL schema.assert({Person: ['name', 1]}, {}) YIELD * RETURN *;",
    )
    show_index_results = list(execute_and_fetch_all(cursor, "SHOW INDEX INFO;"))
    assert show_index_results == [("label+property", "Person", ["name"], 0)]
    execute_and_fetch_all(cursor, "DROP INDEX ON :Person(name);")


def test_assert_creates_label_index_multiple_empty_strings():
    cursor = connect().cursor()
    results = list(
        execute_and_fetch_all(
            cursor,
            "CALL schema.assert({Person: ['', '', '', '']}, {}) YIELD * RETURN *;",
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
            "CALL schema.assert({Person: ['name']}, {}) YIELD * RETURN *;",
        )
    )
    assert results == [("Created", "name", ["name"], "Person", False)]
    show_index_results = list(execute_and_fetch_all(cursor, "SHOW INDEX INFO;"))
    assert show_index_results == [("label+property", "Person", ["name"], 0)]
    execute_and_fetch_all(cursor, "DROP INDEX ON :Person(name);")


def test_assert_creates_multiple_indices():
    cursor = connect().cursor()
    results = list(
        execute_and_fetch_all(
            cursor,
            "CALL schema.assert({Person: ['', 'id', 'name'], Ball: ['', 'size', 'size', '']}, {}) YIELD * RETURN *;",
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
    assert show_index_results[2] == ("label+property", "Ball", ["size"], 0)
    assert show_index_results[3] == ("label+property", "Person", ["id"], 0)
    assert show_index_results[4] == ("label+property", "Person", ["name"], 0)
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
            "CALL schema.assert({}, {}, {Person: ['name', 'surname']}) YIELD * RETURN *;",
        )
    )
    assert results == [
        ("Created", "name", ["name"], "Person", False),
        ("Created", "surname", ["surname"], "Person", False),
    ]
    assert list(execute_and_fetch_all(cursor, "SHOW INDEX INFO;")) == []
    show_constraint_results = list(execute_and_fetch_all(cursor, "SHOW CONSTRAINT INFO;"))
    assert show_constraint_results == [("exists", "Person", "name", ""), ("exists", "Person", "surname", "")]
    execute_and_fetch_all(cursor, "DROP CONSTRAINT ON (n:Person) ASSERT EXISTS (n.name);")
    execute_and_fetch_all(cursor, "DROP CONSTRAINT ON (n:Person) ASSERT EXISTS (n.surname);")


def test_assert_dropping_indices():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "CREATE INDEX ON :Person(name);")
    execute_and_fetch_all(cursor, "CREATE INDEX ON :Person(id);")
    execute_and_fetch_all(cursor, "CREATE INDEX ON :Ball(size);")
    execute_and_fetch_all(cursor, "CREATE INDEX ON :Ball;")
    results = list(execute_and_fetch_all(cursor, "CALL schema.assert({}, {}) YIELD * RETURN *;"))
    assert len(results) == 4
    assert results[0] == ("Dropped", "", [], "Ball", False)
    assert results[1] == ("Dropped", "size", ["size"], "Ball", False)
    assert results[2] == ("Dropped", "id", ["id"], "Person", False)
    assert results[3] == ("Dropped", "name", ["name"], "Person", False)
    show_index_results = list(execute_and_fetch_all(cursor, "SHOW INDEX INFO;"))
    assert show_index_results == []


def test_assert_existence_constraint_properties_not_list():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "CALL schema.assert({}, {}, {Person: 'name'}) YIELD * RETURN *;")
    assert list(execute_and_fetch_all(cursor, "SHOW CONSTRAINT INFO;")) == []


def test_assert_existence_constraint_property_not_string():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "CALL schema.assert({}, {}, {Person: ['name', 1]}) YIELD * RETURN *;")
    show_constraint_results = list(execute_and_fetch_all(cursor, "SHOW CONSTRAINT INFO;"))
    assert show_constraint_results == [("exists", "Person", "name", "")]
    execute_and_fetch_all(cursor, "DROP CONSTRAINT ON (n:Person) ASSERT EXISTS (n.name);")


def test_assert_existence_constraint_property_empty_string():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "CALL schema.assert({}, {}, {Person: ['']}) YIELD * RETURN *;")
    assert list(execute_and_fetch_all(cursor, "SHOW CONSTRAINT INFO;")) == []


def test_assert_creates_indices_and_existence_constraints():
    cursor = connect().cursor()
    results = list(
        execute_and_fetch_all(
            cursor,
            "CALL schema.assert({Person: ['', 'id']}, {}, {Person: ['name', 'surname']}) YIELD * RETURN *;",
        )
    )
    assert len(results) == 4
    assert results[0] == ("Created", "", [], "Person", False)
    assert results[1] == ("Created", "id", ["id"], "Person", False)
    assert results[2] == ("Created", "name", ["name"], "Person", False)
    assert results[3] == ("Created", "surname", ["surname"], "Person", False)
    show_index_results = list(execute_and_fetch_all(cursor, "SHOW INDEX INFO;"))
    assert show_index_results == [("label", "Person", None, 0), ("label+property", "Person", ["id"], 0)]
    show_constraint_results = list(execute_and_fetch_all(cursor, "SHOW CONSTRAINT INFO;"))
    assert show_constraint_results == [("exists", "Person", "name", ""), ("exists", "Person", "surname", "")]
    execute_and_fetch_all(cursor, "DROP INDEX ON :Person;")
    execute_and_fetch_all(cursor, "DROP INDEX ON :Person(id);")
    execute_and_fetch_all(cursor, "DROP CONSTRAINT ON (n:Person) ASSERT EXISTS (n.name);")
    execute_and_fetch_all(cursor, "DROP CONSTRAINT ON (n:Person) ASSERT EXISTS (n.surname);")


def test_assert_drops_existence_constraints():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "CREATE CONSTRAINT ON (n:Person) ASSERT EXISTS (n.name);")
    execute_and_fetch_all(cursor, "CREATE CONSTRAINT ON (n:Person) ASSERT EXISTS (n.surname);")
    results = list(execute_and_fetch_all(cursor, "CALL schema.assert({}, {}, {}) YIELD * RETURN *;"))
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
            "CALL schema.assert({}, {Person: [['name', 'surname']]}) YIELD * RETURN *;",
        )
    )
    assert results == [("Created", "[name, surname]", ["name", "surname"], "Person", True)]
    assert list(execute_and_fetch_all(cursor, "SHOW INDEX INFO;")) == []
    show_constraint_results = list(execute_and_fetch_all(cursor, "SHOW CONSTRAINT INFO;"))
    assert show_constraint_results == [("unique", "Person", ["name", "surname"], "")]
    execute_and_fetch_all(cursor, "DROP CONSTRAINT ON (n:Person) ASSERT n.name, n.surname IS UNIQUE;")


def test_assert_creates_multiple_unique_constraints():
    cursor = connect().cursor()
    results = list(
        execute_and_fetch_all(
            cursor,
            "CALL schema.assert({}, {Person: [['name', 'surname'], ['id']]}) YIELD * RETURN *;",
        )
    )
    assert results == [
        ("Created", "[name, surname]", ["name", "surname"], "Person", True),
        ("Created", "[id]", ["id"], "Person", True),
    ]
    assert list(execute_and_fetch_all(cursor, "SHOW INDEX INFO;")) == []
    show_constraint_results = list(execute_and_fetch_all(cursor, "SHOW CONSTRAINT INFO;"))
    assert show_constraint_results == [("unique", "Person", ["name", "surname"], ""), ("unique", "Person", ["id"], "")]
    execute_and_fetch_all(cursor, "DROP CONSTRAINT ON (n:Person) ASSERT n.name, n.surname IS UNIQUE;")
    execute_and_fetch_all(cursor, "DROP CONSTRAINT ON (n:Person) ASSERT n.id IS UNIQUE;")


def test_assert_creates_unique_constraints_skip_invalid():
    cursor = connect().cursor()
    results = list(
        execute_and_fetch_all(
            cursor,
            "CALL schema.assert({}, {Person: [['name', 'surname'], 'wrong_type']}) YIELD * RETURN *;",
        )
    )
    assert results == [("Created", "[name, surname]", ["name", "surname"], "Person", True)]
    assert list(execute_and_fetch_all(cursor, "SHOW INDEX INFO;")) == []
    show_constraint_results = list(execute_and_fetch_all(cursor, "SHOW CONSTRAINT INFO;"))
    assert show_constraint_results == [("unique", "Person", ["name", "surname"], "")]
    execute_and_fetch_all(cursor, "DROP CONSTRAINT ON (n:Person) ASSERT n.name, n.surname IS UNIQUE;")


def test_assert_creates_unique_constraints_skip_invalid_map_type():
    cursor = connect().cursor()
    results = list(
        execute_and_fetch_all(
            cursor,
            "CALL schema.assert({}, {Person: [['name', 'surname']], Ball: 'wrong_type'}) YIELD * RETURN *;",
        )
    )
    assert results == [("Created", "[name, surname]", ["name", "surname"], "Person", True)]
    assert list(execute_and_fetch_all(cursor, "SHOW INDEX INFO;")) == []
    show_constraint_results = list(execute_and_fetch_all(cursor, "SHOW CONSTRAINT INFO;"))
    assert show_constraint_results == [("unique", "Person", ["name", "surname"], "")]
    execute_and_fetch_all(cursor, "DROP CONSTRAINT ON (n:Person) ASSERT n.name, n.surname IS UNIQUE;")


def test_assert_creates_constraints_and_indices():
    cursor = connect().cursor()
    results = list(
        execute_and_fetch_all(
            cursor,
            "CALL schema.assert({Person: ['', 'id']}, {Person: [['name', 'surname'], ['id']]}, {Person: ['name', 'surname']}) YIELD * RETURN *;",
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
    assert show_index_results == [("label", "Person", None, 0), ("label+property", "Person", ["id"], 0)]
    show_constraint_results = list(execute_and_fetch_all(cursor, "SHOW CONSTRAINT INFO;"))
    assert show_constraint_results == [
        ("exists", "Person", "name", ""),
        ("exists", "Person", "surname", ""),
        ("unique", "Person", ["name", "surname"], ""),
        ("unique", "Person", ["id"], ""),
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
    results = list(execute_and_fetch_all(cursor, "CALL schema.assert({}, {}, {}) YIELD * RETURN *;"))
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
    results = list(execute_and_fetch_all(cursor, "CALL schema.assert({}, {}, {}) YIELD * RETURN *;"))
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
    results = list(execute_and_fetch_all(cursor, "CALL schema.assert({}, {}, {}, false) YIELD * RETURN *;"))
    assert len(results) == 0
    show_index_results = list(execute_and_fetch_all(cursor, "SHOW INDEX INFO;"))
    assert show_index_results == [("label", "Person", None, 0), ("label+property", "Person", ["id"], 0)]
    show_constraint_results = list(execute_and_fetch_all(cursor, "SHOW CONSTRAINT INFO;"))
    assert show_constraint_results == [
        ("exists", "Person", "name", ""),
        ("exists", "Person", "surname", ""),
        ("unique", "Person", ["name", "surname"], ""),
        ("unique", "Person", ["id"], ""),
    ]
    execute_and_fetch_all(cursor, "DROP INDEX ON :Person;")
    execute_and_fetch_all(cursor, "DROP INDEX ON :Person(id);")
    execute_and_fetch_all(cursor, "DROP CONSTRAINT ON (n:Person) ASSERT n.name, n.surname IS UNIQUE;")
    execute_and_fetch_all(cursor, "DROP CONSTRAINT ON (n:Person) ASSERT n.id IS UNIQUE;")
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
            f"CALL schema.node_type_properties() YIELD nodeType, nodeLabels, propertyName, propertyTypes , mandatory RETURN nodeType, nodeLabels, propertyName, propertyTypes , mandatory ORDER BY propertyName, nodeLabels[0];",
        )[0]
    )
    assert (result) == [":`Activity`", ["Activity"], "location", ["String"], False]

    result = list(
        execute_and_fetch_all(
            cursor,
            f"CALL schema.node_type_properties() YIELD nodeType, nodeLabels, propertyName, propertyTypes , mandatory RETURN nodeType, nodeLabels, propertyName, propertyTypes , mandatory ORDER BY propertyName, nodeLabels[0];",
        )[1]
    )
    assert (result) == [":`Activity`", ["Activity"], "name", ["String"], False]

    result = list(
        execute_and_fetch_all(
            cursor,
            f"CALL schema.node_type_properties() YIELD nodeType, nodeLabels, propertyName, propertyTypes , mandatory RETURN nodeType, nodeLabels, propertyName, propertyTypes , mandatory ORDER BY propertyName, nodeLabels[0];",
        )[2]
    )
    assert (result) == [":`Dog`", ["Dog"], "name", ["String"], False]

    result = list(
        execute_and_fetch_all(
            cursor,
            f"CALL schema.node_type_properties() YIELD nodeType, nodeLabels, propertyName, propertyTypes , mandatory RETURN nodeType, nodeLabels, propertyName, propertyTypes , mandatory ORDER BY propertyName, nodeLabels[0];",
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
        f"CALL schema.node_type_properties() YIELD nodeType, nodeLabels, propertyName, propertyTypes , mandatory RETURN nodeType, nodeLabels, propertyName, propertyTypes , mandatory ORDER BY propertyName, nodeLabels[0];",
    )

    assert (list(result[0])) == [":`MyNode`", ["MyNode"], "", [], False]
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
        f"CALL schema.node_type_properties() YIELD nodeType, nodeLabels, propertyName, propertyTypes , mandatory RETURN nodeType, nodeLabels, propertyName, propertyTypes , mandatory ORDER BY propertyName, nodeLabels[0];",
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
            f"CALL schema.node_type_properties() YIELD nodeType, nodeLabels, propertyName, propertyTypes , mandatory RETURN nodeType, nodeLabels, propertyName, propertyTypes , mandatory ORDER BY propertyName, nodeLabels[0];",
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
        f"CALL schema.node_type_properties() YIELD nodeType, nodeLabels, propertyName, propertyTypes , mandatory RETURN nodeType, nodeLabels, propertyName, propertyTypes , mandatory ORDER BY propertyName, nodeLabels[0];",
    )

    assert (list(result[0])) == [":`Dog`", ["Dog"], "name", ["String"], False]
    assert (result.__len__()) == 1


def test_node_type_properties6():
    cursor = connect().cursor()
    execute_and_fetch_all(
        cursor,
        """
        CREATE (d:Dog {name: 'Rex'})
        CREATE (n:Dog {name: 'Simba', owner: 'Lucy'})
        """,
    )
    result = execute_and_fetch_all(
        cursor,
        f"CALL schema.node_type_properties() YIELD nodeType, nodeLabels, propertyName, propertyTypes , mandatory RETURN nodeType, nodeLabels, propertyName, propertyTypes , mandatory ORDER BY propertyName, nodeLabels[0];",
    )

    assert (list(result[0])) == [":`Dog`", ["Dog"], "name", ["String"], False]
    assert (list(result[1])) == [":`Dog`", ["Dog"], "owner", ["String"], False]
    assert (result.__len__()) == 2


def test_node_type_properties_multiple_property_types():
    cursor = connect().cursor()
    execute_and_fetch_all(
        cursor,
        """
        CREATE (n:Node {prop1: 1})
        CREATE (m:Node {prop1: '1'})
        """,
    )
    result = execute_and_fetch_all(
        cursor,
        f"CALL schema.node_type_properties() YIELD nodeType, nodeLabels, propertyName, propertyTypes , mandatory RETURN nodeType, nodeLabels, propertyName, propertyTypes , mandatory ORDER BY propertyName, nodeLabels[0];",
    )
    assert (list(result[0])) == [":`Node`", ["Node"], "prop1", ["Int", "String"], False] or (list(result[0])) == [
        ":`Node`",
        ["Node"],
        "prop1",
        ["String", "Int"],
        False,
    ]
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
            f"CALL schema.rel_type_properties() YIELD relType,propertyName, propertyTypes , mandatory RETURN relType, propertyName, propertyTypes , mandatory;",
        )[0]
    )
    assert (result) == [":`LOVES`", "", [], False]


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
        f"CALL schema.rel_type_properties() YIELD relType,propertyName, propertyTypes , mandatory RETURN relType, propertyName, propertyTypes , mandatory;",
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
        f"CALL schema.rel_type_properties() YIELD relType,propertyName, propertyTypes , mandatory RETURN relType, propertyName, propertyTypes , mandatory;",
    )
    assert (list(result[0])) == [":`LOVES`", "duration", ["Int"], False]
    assert (result.__len__()) == 1


def test_rel_type_properties4():
    cursor = connect().cursor()
    execute_and_fetch_all(
        cursor,
        """
        CREATE (n:Dog {name: 'Simba', owner: 'Lucy'})-[j:LOVES {duration: 30}]->(a:Activity {name: 'Running', location: 'Zadar'})
        CREATE (m:Dog {name: 'Rex', owner: 'Lucy'})-[r:LOVES {duration: 30, weather: 'sunny'}]->(b:Activity {name: 'Running', location: 'Zadar'})
        """,
    )
    result = execute_and_fetch_all(
        cursor,
        f"CALL schema.rel_type_properties() YIELD relType,propertyName, propertyTypes , mandatory RETURN relType, propertyName, propertyTypes , mandatory;",
    )
    assert (list(result[0])) == [":`LOVES`", "weather", ["String"], False]
    assert (list(result[1])) == [":`LOVES`", "duration", ["Int"], False]
    assert (result.__len__()) == 2


def test_rel_type_properties_multiple_property_types():
    cursor = connect().cursor()
    execute_and_fetch_all(
        cursor,
        """
        CREATE (n:Dog {name: 'Simba', owner: 'Lucy'})-[j:LOVES {duration: 30}]->(a:Activity {name: 'Running', location: 'Zadar'})
        CREATE (m:Dog {name: 'Rex', owner: 'Lucy'})-[r:LOVES {duration: "30"}]->(b:Activity {name: 'Running', location: 'Zadar'})
        """,
    )
    result = execute_and_fetch_all(
        cursor,
        f"CALL schema.rel_type_properties() YIELD relType,propertyName, propertyTypes , mandatory RETURN relType, propertyName, propertyTypes , mandatory;",
    )
    assert (list(result[0])) == [":`LOVES`", "duration", ["Int", "String"], False] or (list(result[0])) == [
        ":`LOVES`",
        "duration",
        ["String", "Int"],
        False,
    ]
    assert (result.__len__()) == 1


def test_node_type_properties_zoned_datetime():
    cursor = connect().cursor()
    execute_and_fetch_all(
        cursor,
        "CREATE (n:Event {name: 'Meeting', scheduled: datetime('2024-01-15T10:30:00+01:00')})",
    )
    result = execute_and_fetch_all(
        cursor,
        "CALL schema.node_type_properties() YIELD nodeType, nodeLabels, propertyName, propertyTypes, mandatory "
        "RETURN nodeType, nodeLabels, propertyName, propertyTypes, mandatory ORDER BY propertyName;",
    )
    assert len(result) == 2
    assert list(result[0]) == [":`Event`", ["Event"], "name", ["String"], False]
    assert list(result[1]) == [":`Event`", ["Event"], "scheduled", ["DateTime"], False]


def test_node_type_properties_point2d():
    cursor = connect().cursor()
    execute_and_fetch_all(
        cursor,
        "CREATE (n:Place {name: 'Zagreb', location: point({x: 15.9819, y: 45.8150, srid: 4326})})",
    )
    result = execute_and_fetch_all(
        cursor,
        "CALL schema.node_type_properties() YIELD nodeType, nodeLabels, propertyName, propertyTypes, mandatory "
        "RETURN nodeType, nodeLabels, propertyName, propertyTypes, mandatory ORDER BY propertyName;",
    )
    assert len(result) == 2
    assert list(result[0]) == [":`Place`", ["Place"], "location", ["Point"], False]
    assert list(result[1]) == [":`Place`", ["Place"], "name", ["String"], False]


def test_node_type_properties_point3d():
    cursor = connect().cursor()
    execute_and_fetch_all(
        cursor,
        "CREATE (n:Place {name: 'Zagreb', location: point({x: 15.9819, y: 45.8150, z: 150.0, srid: 4979})})",
    )
    result = execute_and_fetch_all(
        cursor,
        "CALL schema.node_type_properties() YIELD nodeType, nodeLabels, propertyName, propertyTypes, mandatory "
        "RETURN nodeType, nodeLabels, propertyName, propertyTypes, mandatory ORDER BY propertyName;",
    )
    assert len(result) == 2
    assert list(result[0]) == [":`Place`", ["Place"], "location", ["Point"], False]
    assert list(result[1]) == [":`Place`", ["Place"], "name", ["String"], False]


def test_node_type_properties_enum():
    cursor = connect().cursor()
    # No DROP ENUM cleanup needed — e2e runner starts a fresh memgraph instance
    execute_and_fetch_all(cursor, "CREATE ENUM Status VALUES { Good, Okay, Bad };")
    execute_and_fetch_all(
        cursor,
        "CREATE (n:Item {name: 'Widget', status: Status::Good})",
    )
    result = execute_and_fetch_all(
        cursor,
        "CALL schema.node_type_properties() YIELD nodeType, nodeLabels, propertyName, propertyTypes, mandatory "
        "RETURN nodeType, nodeLabels, propertyName, propertyTypes, mandatory ORDER BY propertyName;",
    )
    assert len(result) == 2
    assert list(result[0]) == [":`Item`", ["Item"], "name", ["String"], False]
    assert list(result[1]) == [":`Item`", ["Item"], "status", ["Enum"], False]


def test_node_type_properties_boolean():
    cursor = connect().cursor()
    execute_and_fetch_all(
        cursor,
        "CREATE (n:Flag {name: 'feature', enabled: true})",
    )
    result = execute_and_fetch_all(
        cursor,
        "CALL schema.node_type_properties() YIELD nodeType, nodeLabels, propertyName, propertyTypes, mandatory "
        "RETURN nodeType, nodeLabels, propertyName, propertyTypes, mandatory ORDER BY propertyName;",
    )
    assert len(result) == 2
    assert list(result[0]) == [":`Flag`", ["Flag"], "enabled", ["Boolean"], False]
    assert list(result[1]) == [":`Flag`", ["Flag"], "name", ["String"], False]


def test_node_type_properties_float():
    cursor = connect().cursor()
    execute_and_fetch_all(
        cursor,
        "CREATE (n:Measurement {name: 'temperature', value: 21.5})",
    )
    result = execute_and_fetch_all(
        cursor,
        "CALL schema.node_type_properties() YIELD nodeType, nodeLabels, propertyName, propertyTypes, mandatory "
        "RETURN nodeType, nodeLabels, propertyName, propertyTypes, mandatory ORDER BY propertyName;",
    )
    assert len(result) == 2
    assert list(result[0]) == [":`Measurement`", ["Measurement"], "name", ["String"], False]
    assert list(result[1]) == [":`Measurement`", ["Measurement"], "value", ["Float"], False]


def test_rel_type_properties_point2d():
    cursor = connect().cursor()
    execute_and_fetch_all(
        cursor,
        "CREATE (a:Person {name: 'Alice'})-[r:VISITED {location: point({x: 15.9819, y: 45.8150, srid: 4326})}]->(b:Place {name: 'Zagreb'})",
    )
    result = execute_and_fetch_all(
        cursor,
        "CALL schema.rel_type_properties() YIELD relType, propertyName, propertyTypes, mandatory "
        "RETURN relType, propertyName, propertyTypes, mandatory;",
    )
    assert len(result) == 1
    assert list(result[0]) == [":`VISITED`", "location", ["Point"], False]


def test_node_type_properties_config_include_labels():
    cursor = connect().cursor()
    execute_and_fetch_all(
        cursor,
        """
        CREATE (d:Dog {name: 'Rex'})-[:LOVES]->(a:Activity {name: 'Running'})
        CREATE (c:Cat {name: 'Whiskers'})
        """,
    )
    result = execute_and_fetch_all(
        cursor,
        "CALL schema.node_type_properties({includeLabels: ['Dog']}) "
        "YIELD nodeType, nodeLabels, propertyName, propertyTypes, mandatory "
        "RETURN nodeType, nodeLabels, propertyName, propertyTypes, mandatory ORDER BY propertyName;",
    )
    assert len(result) == 1
    assert list(result[0]) == [":`Dog`", ["Dog"], "name", ["String"], False]


def test_node_type_properties_config_exclude_labels():
    cursor = connect().cursor()
    execute_and_fetch_all(
        cursor,
        """
        CREATE (d:Dog {name: 'Rex'})-[:LOVES]->(a:Activity {name: 'Running'})
        CREATE (c:Cat {name: 'Whiskers'})
        """,
    )
    result = execute_and_fetch_all(
        cursor,
        "CALL schema.node_type_properties({excludeLabels: ['Dog', 'Activity']}) "
        "YIELD nodeType, nodeLabels, propertyName, propertyTypes, mandatory "
        "RETURN nodeType, nodeLabels, propertyName, propertyTypes, mandatory ORDER BY propertyName;",
    )
    assert len(result) == 1
    assert list(result[0]) == [":`Cat`", ["Cat"], "name", ["String"], False]


def test_node_type_properties_sample_does_not_drive_mandatory():
    cursor = connect().cursor()
    # Create 3 Dog nodes, 2 with owner property, 1 without
    execute_and_fetch_all(
        cursor,
        """
        CREATE (:Dog {name: 'Rex', owner: 'Carl'})
        CREATE (:Dog {name: 'Simba', owner: 'Lucy'})
        CREATE (:Dog {name: 'Buddy'})
        """,
    )
    # Sampling caps the scan; mandatory comes from existence constraints, not sample coverage.
    result = execute_and_fetch_all(
        cursor,
        "CALL schema.node_type_properties({sample: 2}) "
        "YIELD nodeType, nodeLabels, propertyName, propertyTypes, mandatory "
        "RETURN nodeType, nodeLabels, propertyName, propertyTypes, mandatory ORDER BY propertyName;",
    )
    assert len(result) == 2
    mandatory = {list(r)[2]: list(r)[4] for r in result}
    assert mandatory["name"] is False
    assert mandatory["owner"] is False


def test_node_type_properties_config_include_and_exclude_labels():
    cursor = connect().cursor()
    execute_and_fetch_all(
        cursor,
        """
        CREATE (:Dog {name: 'Rex'})
        CREATE (:Cat {name: 'Whiskers'})
        CREATE (:Bird {name: 'Tweety'})
        """,
    )
    # Include Dog and Cat, but exclude Cat -> only Dog
    result = execute_and_fetch_all(
        cursor,
        "CALL schema.node_type_properties({includeLabels: ['Dog', 'Cat'], excludeLabels: ['Cat']}) "
        "YIELD nodeType, nodeLabels, propertyName, propertyTypes, mandatory "
        "RETURN nodeType, nodeLabels, propertyName, propertyTypes, mandatory ORDER BY propertyName;",
    )
    assert len(result) == 1
    assert list(result[0]) == [":`Dog`", ["Dog"], "name", ["String"], False]


def test_node_type_properties_config_empty_map():
    cursor = connect().cursor()
    execute_and_fetch_all(
        cursor,
        "CREATE (:Dog {name: 'Rex'})",
    )
    # Empty config map should behave same as no config
    result = execute_and_fetch_all(
        cursor,
        "CALL schema.node_type_properties({}) "
        "YIELD nodeType, nodeLabels, propertyName, propertyTypes, mandatory "
        "RETURN nodeType, nodeLabels, propertyName, propertyTypes, mandatory;",
    )
    assert len(result) == 1
    assert list(result[0]) == [":`Dog`", ["Dog"], "name", ["String"], False]


def test_node_type_properties_config_include_rels():
    cursor = connect().cursor()
    execute_and_fetch_all(
        cursor,
        """
        CREATE (d:Dog {name: 'Rex'})-[:LOVES]->(a:Activity {name: 'Running'})
        CREATE (c:Cat {name: 'Whiskers'})-[:HATES]->(b:Activity {name: 'Sleeping'})
        """,
    )
    # Only include nodes that have an outgoing LOVES relationship
    result = execute_and_fetch_all(
        cursor,
        "CALL schema.node_type_properties({includeRels: ['LOVES']}) "
        "YIELD nodeType, nodeLabels, propertyName, propertyTypes, mandatory "
        "RETURN nodeType, nodeLabels, propertyName, propertyTypes, mandatory ORDER BY nodeLabels[0], propertyName;",
    )
    assert len(result) == 1
    assert list(result[0]) == [":`Dog`", ["Dog"], "name", ["String"], False]


def test_node_type_properties_config_exclude_rels():
    cursor = connect().cursor()
    execute_and_fetch_all(
        cursor,
        """
        CREATE (d:Dog {name: 'Rex'})-[:LOVES]->(a:Activity {name: 'Running'})
        CREATE (c:Cat {name: 'Whiskers'})-[:HATES]->(b:Activity {name: 'Sleeping'})
        """,
    )
    # Exclude nodes that have an outgoing HATES relationship
    result = execute_and_fetch_all(
        cursor,
        "CALL schema.node_type_properties({excludeRels: ['HATES']}) "
        "YIELD nodeType, nodeLabels, propertyName, propertyTypes, mandatory "
        "RETURN nodeType, nodeLabels, propertyName, propertyTypes, mandatory ORDER BY nodeLabels[0], propertyName;",
    )
    # Dog and Activity nodes remain (Activity has no outgoing HATES)
    assert len(result) == 2
    assert list(result[0]) == [":`Activity`", ["Activity"], "name", ["String"], False]
    assert list(result[1]) == [":`Dog`", ["Dog"], "name", ["String"], False]


def test_node_type_properties_config_include_and_exclude_rels():
    cursor = connect().cursor()
    execute_and_fetch_all(
        cursor,
        """
        CREATE (d:Dog {name: 'Rex'})-[:LOVES]->(a:Activity {name: 'Running'})
        CREATE (d2:Dog {name: 'Simba'})-[:LOVES]->(b:Activity {name: 'Walking'})
        CREATE (d2)-[:HATES]->(c:Activity {name: 'Bathing'})
        """,
    )
    # Include nodes with LOVES, but exclude those that also have HATES
    result = execute_and_fetch_all(
        cursor,
        "CALL schema.node_type_properties({includeRels: ['LOVES'], excludeRels: ['HATES']}) "
        "YIELD nodeType, nodeLabels, propertyName, propertyTypes, mandatory "
        "RETURN nodeType, nodeLabels, propertyName, propertyTypes, mandatory ORDER BY propertyName;",
    )
    # Only Rex (Dog) has LOVES without HATES
    assert len(result) == 1
    assert list(result[0]) == [":`Dog`", ["Dog"], "name", ["String"], False]


def test_rel_type_properties_config_include_rels():
    cursor = connect().cursor()
    execute_and_fetch_all(
        cursor,
        """
        CREATE (d:Dog {name: 'Rex'})-[:LOVES {duration: 30}]->(a:Activity {name: 'Running'})
        CREATE (d2:Dog {name: 'Simba'})-[:HATES {reason: 'loud'}]->(b:Activity {name: 'Thunder'})
        """,
    )
    result = execute_and_fetch_all(
        cursor,
        "CALL schema.rel_type_properties({includeRels: ['LOVES']}) "
        "YIELD relType, propertyName, propertyTypes, mandatory "
        "RETURN relType, propertyName, propertyTypes, mandatory;",
    )
    assert len(result) == 1
    assert list(result[0]) == [":`LOVES`", "duration", ["Int"], False]


def test_rel_type_properties_config_exclude_rels():
    cursor = connect().cursor()
    execute_and_fetch_all(
        cursor,
        """
        CREATE (d:Dog {name: 'Rex'})-[:LOVES {duration: 30}]->(a:Activity {name: 'Running'})
        CREATE (d2:Dog {name: 'Simba'})-[:HATES {reason: 'loud'}]->(b:Activity {name: 'Thunder'})
        """,
    )
    result = execute_and_fetch_all(
        cursor,
        "CALL schema.rel_type_properties({excludeRels: ['HATES']}) "
        "YIELD relType, propertyName, propertyTypes, mandatory "
        "RETURN relType, propertyName, propertyTypes, mandatory;",
    )
    assert len(result) == 1
    assert list(result[0]) == [":`LOVES`", "duration", ["Int"], False]


def test_rel_type_properties_config_include_labels():
    cursor = connect().cursor()
    execute_and_fetch_all(
        cursor,
        """
        CREATE (d:Dog {name: 'Rex'})-[:LOVES {duration: 30}]->(a:Activity {name: 'Running'})
        CREATE (c:Cat {name: 'Whiskers'})-[:LOVES {duration: 10}]->(b:Activity {name: 'Sleeping'})
        """,
    )
    # Only scan outgoing rels from Dog nodes
    result = execute_and_fetch_all(
        cursor,
        "CALL schema.rel_type_properties({includeLabels: ['Dog']}) "
        "YIELD relType, propertyName, propertyTypes, mandatory "
        "RETURN relType, propertyName, propertyTypes, mandatory;",
    )
    assert len(result) == 1
    assert list(result[0]) == [":`LOVES`", "duration", ["Int"], False]


def test_rel_type_properties_config_exclude_labels():
    cursor = connect().cursor()
    execute_and_fetch_all(
        cursor,
        """
        CREATE (d:Dog {name: 'Rex'})-[:LOVES {duration: 30}]->(a:Activity {name: 'Running'})
        CREATE (c:Cat {name: 'Whiskers'})-[:HATES {reason: 'wet'}]->(b:Activity {name: 'Bathing'})
        """,
    )
    # Exclude Cat source nodes -> only Dog's relationships
    result = execute_and_fetch_all(
        cursor,
        "CALL schema.rel_type_properties({excludeLabels: ['Cat']}) "
        "YIELD relType, propertyName, propertyTypes, mandatory "
        "RETURN relType, propertyName, propertyTypes, mandatory;",
    )
    assert len(result) == 1
    assert list(result[0]) == [":`LOVES`", "duration", ["Int"], False]


def test_rel_type_properties_config_empty_map():
    cursor = connect().cursor()
    execute_and_fetch_all(
        cursor,
        "CREATE (d:Dog {name: 'Rex'})-[:LOVES {duration: 30}]->(a:Activity {name: 'Running'})",
    )
    result = execute_and_fetch_all(
        cursor,
        "CALL schema.rel_type_properties({}) "
        "YIELD relType, propertyName, propertyTypes, mandatory "
        "RETURN relType, propertyName, propertyTypes, mandatory;",
    )
    assert len(result) == 1
    assert list(result[0]) == [":`LOVES`", "duration", ["Int"], False]


def test_rel_type_properties_config_include_and_exclude_rels():
    cursor = connect().cursor()
    execute_and_fetch_all(
        cursor,
        """
        CREATE (d:Dog)-[:LOVES]->(a:Activity)
        CREATE (d2:Dog)-[:HATES]->(b:Activity)
        CREATE (d3:Dog)-[:LIKES]->(c:Activity)
        """,
    )
    # Include LOVES and LIKES, but exclude LIKES -> only LOVES
    result = execute_and_fetch_all(
        cursor,
        "CALL schema.rel_type_properties({includeRels: ['LOVES', 'LIKES'], excludeRels: ['LIKES']}) "
        "YIELD relType, propertyName, propertyTypes, mandatory "
        "RETURN relType, propertyName, propertyTypes, mandatory;",
    )
    assert len(result) == 1
    assert list(result[0]) == [":`LOVES`", "", [], False]


def test_node_type_properties_apoc_meta_mapping():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "CREATE (:Dog {name: 'Rex'})")
    result = execute_and_fetch_all(
        cursor,
        "CALL apoc.meta.nodeTypeProperties() "
        "YIELD nodeType, nodeLabels, propertyName, propertyTypes, mandatory "
        "RETURN nodeType, nodeLabels, propertyName, propertyTypes, mandatory;",
    )
    assert len(result) == 1
    assert list(result[0]) == [":`Dog`", ["Dog"], "name", ["String"], False]


def test_node_type_properties_db_schema_mapping():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "CREATE (:Dog {name: 'Rex'})")
    result = execute_and_fetch_all(
        cursor,
        "CALL db.schema.nodeTypeProperties() "
        "YIELD nodeType, nodeLabels, propertyName, propertyTypes, mandatory "
        "RETURN nodeType, nodeLabels, propertyName, propertyTypes, mandatory;",
    )
    assert len(result) == 1
    assert list(result[0]) == [":`Dog`", ["Dog"], "name", ["String"], False]


def test_rel_type_properties_apoc_meta_mapping():
    cursor = connect().cursor()
    execute_and_fetch_all(
        cursor,
        "CREATE (:Dog {name: 'Rex'})-[:LOVES {duration: 30}]->(:Activity {name: 'Running'})",
    )
    result = execute_and_fetch_all(
        cursor,
        "CALL apoc.meta.relTypeProperties() "
        "YIELD relType, propertyName, propertyTypes, mandatory "
        "RETURN relType, propertyName, propertyTypes, mandatory;",
    )
    assert len(result) == 1
    assert list(result[0]) == [":`LOVES`", "duration", ["Int"], False]


def test_rel_type_properties_db_schema_mapping():
    cursor = connect().cursor()
    execute_and_fetch_all(
        cursor,
        "CREATE (:Dog {name: 'Rex'})-[:LOVES {duration: 30}]->(:Activity {name: 'Running'})",
    )
    result = execute_and_fetch_all(
        cursor,
        "CALL db.schema.relTypeProperties() "
        "YIELD relType, propertyName, propertyTypes, mandatory "
        "RETURN relType, propertyName, propertyTypes, mandatory;",
    )
    assert len(result) == 1
    assert list(result[0]) == [":`LOVES`", "duration", ["Int"], False]


def test_node_type_properties_observations_counts():
    cursor = connect().cursor()
    execute_and_fetch_all(
        cursor,
        """
        CREATE (:Dog {name: 'Rex', owner: 'Carl'})
        CREATE (:Dog {name: 'Simba'})
        CREATE (:Dog {name: 'Buddy'})
        """,
    )
    result = execute_and_fetch_all(
        cursor,
        "CALL schema.node_type_properties() "
        "YIELD nodeType, propertyName, mandatory, propertyObservations, totalObservations "
        "RETURN nodeType, propertyName, mandatory, propertyObservations, totalObservations "
        "ORDER BY propertyName;",
    )
    counts = {list(r)[1]: (list(r)[2], list(r)[3], list(r)[4]) for r in result}
    assert counts["name"] == (False, 3, 3)
    assert counts["owner"] == (False, 1, 3)


def test_rel_type_properties_source_and_target_labels():
    cursor = connect().cursor()
    execute_and_fetch_all(
        cursor,
        """
        CREATE (:Dog {name: 'Rex'})-[:LOVES {duration: 30}]->(:Activity {name: 'Running'})
        CREATE (:Cat {name: 'Whiskers'})-[:LOVES {duration: 10}]->(:Activity {name: 'Sleeping'})
        """,
    )
    result = execute_and_fetch_all(
        cursor,
        "CALL schema.rel_type_properties() "
        "YIELD relType, sourceNodeLabels, targetNodeLabels, propertyName, propertyTypes, mandatory, "
        "propertyObservations, totalObservations "
        "RETURN relType, sourceNodeLabels, targetNodeLabels, propertyName, propertyTypes, mandatory, "
        "propertyObservations, totalObservations "
        "ORDER BY sourceNodeLabels[0];",
    )
    assert len(result) == 2
    assert list(result[0]) == [":`LOVES`", ["Cat"], ["Activity"], "duration", ["Int"], False, 1, 1]
    assert list(result[1]) == [":`LOVES`", ["Dog"], ["Activity"], "duration", ["Int"], False, 1, 1]


def test_rel_type_properties_partitions_by_endpoint_labels():
    cursor = connect().cursor()
    execute_and_fetch_all(
        cursor,
        """
        CREATE (:Dog)-[:LOVES {duration: 30}]->(:Activity)
        CREATE (:Dog)-[:LOVES]->(:Activity)
        CREATE (:Cat)-[:LOVES {weather: 'sunny'}]->(:Place)
        """,
    )
    result = execute_and_fetch_all(
        cursor,
        "CALL schema.rel_type_properties() "
        "YIELD relType, sourceNodeLabels, targetNodeLabels, propertyName, mandatory, "
        "propertyObservations, totalObservations "
        "RETURN relType, sourceNodeLabels, targetNodeLabels, propertyName, mandatory, "
        "propertyObservations, totalObservations "
        "ORDER BY sourceNodeLabels[0], propertyName;",
    )
    rows = {(list(r)[1][0], list(r)[2][0], list(r)[3]): (list(r)[4], list(r)[5], list(r)[6]) for r in result}
    assert rows[("Cat", "Place", "weather")] == (False, 1, 1)
    assert rows[("Dog", "Activity", "duration")] == (False, 1, 2)


def test_rel_type_properties_unlabeled_endpoints():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "CREATE ()-[:KNOWS]->()")
    result = execute_and_fetch_all(
        cursor,
        "CALL schema.rel_type_properties() "
        "YIELD relType, sourceNodeLabels, targetNodeLabels, propertyName, "
        "propertyObservations, totalObservations "
        "RETURN relType, sourceNodeLabels, targetNodeLabels, propertyName, "
        "propertyObservations, totalObservations;",
    )
    assert len(result) == 1
    assert list(result[0]) == [":`KNOWS`", [], [], "", 0, 1]


def test_rel_type_properties_multi_label_endpoints():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "CREATE (:Dog:Pet {name: 'Rex'})-[:OWNS]->(:Toy:Object {name: 'Ball'})")
    result = execute_and_fetch_all(
        cursor,
        "CALL schema.rel_type_properties() "
        "YIELD relType, sourceNodeLabels, targetNodeLabels "
        "RETURN relType, sourceNodeLabels, targetNodeLabels;",
    )
    assert len(result) == 1
    rel_type, source_labels, target_labels = result[0]
    assert (rel_type, sorted(source_labels), sorted(target_labels)) == (":`OWNS`", ["Dog", "Pet"], ["Object", "Toy"])


def test_node_type_properties_empty_properties_row():
    cursor = connect().cursor()
    execute_and_fetch_all(
        cursor,
        """
        CREATE (:Empty)
        CREATE (:Empty)
        CREATE (:Empty)
        """,
    )
    result = execute_and_fetch_all(
        cursor,
        "CALL schema.node_type_properties() "
        "YIELD nodeType, propertyName, propertyObservations, totalObservations "
        "WHERE nodeType = ':`Empty`' "
        "RETURN nodeType, propertyName, propertyObservations, totalObservations;",
    )
    assert len(result) == 1
    assert list(result[0]) == [":`Empty`", "", 0, 3]


def test_node_type_properties_list_is_opaque():
    # Lists are reported as List[Any] regardless of element type. The Simba BI connector
    # maps any list to VARCHAR (no array SQL type), so element-type detection adds no value.
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "CREATE (:Item {strs: ['a', 'b'], mixed: [1, 'a', true]})")
    result = execute_and_fetch_all(
        cursor,
        "CALL schema.node_type_properties() YIELD propertyName, propertyTypes "
        "RETURN propertyName, propertyTypes ORDER BY propertyName;",
    )
    rows = {list(r)[0]: list(r)[1] for r in result}
    assert rows["strs"] == ["List[Any]"]
    assert rows["mixed"] == ["List[Any]"]


def test_mandatory_true_when_existence_constraint_present():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "CREATE CONSTRAINT ON (n:Person) ASSERT EXISTS (n.name);")
    execute_and_fetch_all(cursor, "CREATE (:Person {name: 'Alice'})")
    result = execute_and_fetch_all(
        cursor,
        "CALL schema.node_type_properties() YIELD propertyName, mandatory RETURN propertyName, mandatory;",
    )
    rows = {list(r)[0]: list(r)[1] for r in result}
    assert rows["name"] is True
    execute_and_fetch_all(cursor, "DROP CONSTRAINT ON (n:Person) ASSERT EXISTS (n.name);")


def test_mandatory_false_when_no_constraint_even_if_always_present():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "CREATE (:Person {name: 'Alice'}) CREATE (:Person {name: 'Bob'})")
    result = execute_and_fetch_all(
        cursor,
        "CALL schema.node_type_properties() YIELD propertyName, mandatory RETURN propertyName, mandatory;",
    )
    rows = {list(r)[0]: list(r)[1] for r in result}
    assert rows["name"] is False


def test_mandatory_constraint_label_specific_on_multi_label_node():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "CREATE CONSTRAINT ON (n:Person) ASSERT EXISTS (n.name);")
    execute_and_fetch_all(cursor, "CREATE (:Person:Employee {name: 'Alice'})")
    result = execute_and_fetch_all(
        cursor,
        "CALL schema.node_type_properties() YIELD propertyName, mandatory RETURN propertyName, mandatory;",
    )
    rows = {list(r)[0]: list(r)[1] for r in result}
    assert rows["name"] is True
    execute_and_fetch_all(cursor, "DROP CONSTRAINT ON (n:Person) ASSERT EXISTS (n.name);")


def test_mandatory_constraint_unrelated_label_not_picked_up():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "CREATE CONSTRAINT ON (n:Person) ASSERT EXISTS (n.email);")
    execute_and_fetch_all(cursor, "CREATE (:Company {email: 'info@x.com'})")
    result = execute_and_fetch_all(
        cursor,
        "CALL schema.node_type_properties() YIELD nodeLabels, propertyName, mandatory "
        "RETURN nodeLabels, propertyName, mandatory;",
    )
    rows = {tuple(list(r)[0]) + (list(r)[1],): list(r)[2] for r in result}
    assert rows[("Company", "email")] is False
    execute_and_fetch_all(cursor, "DROP CONSTRAINT ON (n:Person) ASSERT EXISTS (n.email);")


def test_rel_mandatory_always_false():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "CREATE (:A)-[:R {p: 1}]->(:B)")
    result = execute_and_fetch_all(
        cursor,
        "CALL schema.rel_type_properties() YIELD propertyName, mandatory RETURN propertyName, mandatory;",
    )
    rows = {list(r)[0]: list(r)[1] for r in result}
    assert rows["p"] is False


def test_node_type_properties_on_virtual_graph_does_not_crash():
    # ListAllExistenceConstraints throws on derive() graphs; the procedure must swallow it.
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "CREATE CONSTRAINT ON (n:Person) ASSERT EXISTS (n.name);")
    execute_and_fetch_all(cursor, "CREATE (:Person {name: 'Alice'})-[:KNOWS]->(:Person {name: 'Bob'})")
    execute_and_fetch_all(
        cursor,
        "MATCH p=(:Person)-[:KNOWS]->(:Person) "
        "WITH derive(p, {virtualEdgeType: 'KNOWS'}) AS g "
        "CALL schema.node_type_properties(g) YIELD propertyName, mandatory "
        "RETURN propertyName, mandatory;",
    )
    execute_and_fetch_all(cursor, "DROP CONSTRAINT ON (n:Person) ASSERT EXISTS (n.name);")


def test_mandatory_constraint_only_other_label_present():
    # Sibling of test_mandatory_constraint_label_specific_on_multi_label_node:
    # constraint on Person.name, node has only :Employee → not mandatory for the Employee-only row.
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "CREATE CONSTRAINT ON (n:Person) ASSERT EXISTS (n.name);")
    execute_and_fetch_all(cursor, "CREATE (:Employee {name: 'Bob'})")
    result = execute_and_fetch_all(
        cursor,
        "CALL schema.node_type_properties() YIELD nodeLabels, propertyName, mandatory "
        "RETURN nodeLabels, propertyName, mandatory;",
    )
    rows = {tuple(list(r)[0]) + (list(r)[1],): list(r)[2] for r in result}
    assert rows[("Employee", "name")] is False
    execute_and_fetch_all(cursor, "DROP CONSTRAINT ON (n:Person) ASSERT EXISTS (n.name);")


def test_mandatory_constraint_with_no_matching_nodes():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "CREATE CONSTRAINT ON (n:Ghost) ASSERT EXISTS (n.id);")
    execute_and_fetch_all(cursor, "CREATE (:Person {name: 'Alice'})")
    result = execute_and_fetch_all(
        cursor,
        "CALL schema.node_type_properties() YIELD nodeLabels RETURN nodeLabels;",
    )
    labels = {tuple(list(r)[0]) for r in result}
    assert ("Ghost",) not in labels
    execute_and_fetch_all(cursor, "DROP CONSTRAINT ON (n:Ghost) ASSERT EXISTS (n.id);")


def test_mandatory_true_with_sample_smaller_than_population():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "CREATE CONSTRAINT ON (n:Person) ASSERT EXISTS (n.name);")
    execute_and_fetch_all(
        cursor,
        "UNWIND range(1, 10) AS i CREATE (:Person {name: toString(i)})",
    )
    result = execute_and_fetch_all(
        cursor,
        "CALL schema.node_type_properties({sample: 3}) "
        "YIELD propertyName, mandatory, totalObservations "
        "RETURN propertyName, mandatory, totalObservations;",
    )
    rows = {list(r)[0]: (list(r)[1], list(r)[2]) for r in result}
    assert rows["name"] == (True, 3)
    execute_and_fetch_all(cursor, "DROP CONSTRAINT ON (n:Person) ASSERT EXISTS (n.name);")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
