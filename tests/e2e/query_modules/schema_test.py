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


def test_assert_creates():
    cursor = connect().cursor()
    results = list(
        execute_and_fetch_all(
            cursor,
            "CALL libschema.assert({'Person': 'name', 'Person': 'surname', 'Person': ''}, {'Person': ['oib', 'jmbg']}, {'Person': 'name', 'Person': 'surname'}) YIELD * RETURN *;",
        )
    )
    assert len(results) == 7
    assert results[0] == ["Person", "name", "[name]", False, "CREATED"]
    assert results[1] == ["Person", "surname", "[surname]", False, "CREATED"]
    assert results[2] == ["Person", "", "[]", False, "CREATED"]
    assert results[3] == ["Person", "oib", "[oib]", True, "CREATED"]
    assert results[4] == ["Person", "jmbg", "[jmbg]", True, "CREATED"]
    assert results[5] == ["Person", "name", "[name]", False, "CREATED"]
    assert results[6] == ["Person", "surname", "[surname]", False, "CREATED"]


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
