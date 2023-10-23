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


def test_convert_list1():
    cursor = connect().cursor()
    result = execute_and_fetch_all(cursor, f"RETURN convert_QM.str2object('[2, 4, 8, [2]]') AS result;")[0][0]
    assert (result) == [2, 4, 8, [2]]


def test_convert_list_wrong():
    cursor = connect().cursor()
    result = execute_and_fetch_all(cursor, f"RETURN convert_QM.str2object('[2, 4, 8, [2]]') AS result;")[0][0]
    assert (result) != [3, 4, 8, [2]]


def test_mgps1():
    cursor = connect().cursor()
    result = list(
        execute_and_fetch_all(
            cursor, f"CALL mgps_QM.components() YIELD edition, name, versions RETURN edition, name, versions;"
        )[0]
    )
    assert (result) == ["community", "Memgraph", ["5.9.0"]]


def test_node_type_properties1():
    cursor = connect().cursor()
    execute_and_fetch_all(
        cursor,
        "CREATE (d:Dog {name: 'Rex', owner: 'Carl'})-[l:LOVES]->(a:Activity {name: 'Running', location: 'Zadar'})",
    )
    result = list(
        execute_and_fetch_all(
            cursor,
            f"CALL schema_QM.node_type_properties() YIELD nodeType, nodeLabels, propertyName, propertyTypes , mandatory RETURN nodeType, nodeLabels, propertyName, propertyTypes , mandatory ORDER BY propertyName, nodeLabels[0];",
        )[0]
    )
    assert (result) == [":`Activity`", ["Activity"], "location", ["String"], True]

    result = list(
        execute_and_fetch_all(
            cursor,
            f"CALL schema_QM.node_type_properties() YIELD nodeType, nodeLabels, propertyName, propertyTypes , mandatory RETURN nodeType, nodeLabels, propertyName, propertyTypes , mandatory ORDER BY propertyName, nodeLabels[0];",
        )[1]
    )
    assert (result) == [":`Activity`", ["Activity"], "name", ["String"], True]

    result = list(
        execute_and_fetch_all(
            cursor,
            f"CALL schema_QM.node_type_properties() YIELD nodeType, nodeLabels, propertyName, propertyTypes , mandatory RETURN nodeType, nodeLabels, propertyName, propertyTypes , mandatory ORDER BY propertyName, nodeLabels[0];",
        )[2]
    )
    assert (result) == [":`Dog`", ["Dog"], "name", ["String"], True]

    result = list(
        execute_and_fetch_all(
            cursor,
            f"CALL schema_QM.node_type_properties() YIELD nodeType, nodeLabels, propertyName, propertyTypes , mandatory RETURN nodeType, nodeLabels, propertyName, propertyTypes , mandatory ORDER BY propertyName, nodeLabels[0];",
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
            f"CALL schema_QM.rel_type_properties() YIELD relType,propertyName, propertyTypes , mandatory RETURN relType, propertyName, propertyTypes , mandatory;",
        )[0]
    )
    assert (result) == [":`LOVES`", "", "", False]


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
