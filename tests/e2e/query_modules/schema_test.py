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

    assert (list(result[0])) == [":`Dog`", ["Dog"], "name", ["String"], True]
    assert (list(result[1])) == [":`Dog`", ["Dog"], "owner", ["String"], True]
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
    assert (list(result[0])) == [":`Label1`:`Label2`", ["Label1", "Label2"], "property1", ["String"], True]
    assert (list(result[1])) == [":`Label1`:`Label2`", ["Label1", "Label2"], "property2", ["String"], True]
    assert (list(result[2])) == [":`Label1`:`Label2`", ["Label1", "Label2"], "property3", ["String"], True]
    assert (result.__len__()) == 3


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
    assert (list(result[0])) == [":`LOVES`", "duration", ["Int"], True]
    assert (result.__len__()) == 1


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
