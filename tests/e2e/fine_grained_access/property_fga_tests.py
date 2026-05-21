# Copyright 2026 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import common
import pytest


def admin_cursor():
    conn = common.connect(username="admin", password="test")
    return conn.cursor()


def user_cursor():
    conn = common.connect(username="user", password="test")
    return conn.cursor()


def test_return_denied_property_is_null():
    result = common.execute_and_fetch_all(user_cursor(), "MATCH (n:Employee) RETURN n.ssn AS ssn;")
    assert len(result) == 1
    assert result[0][0] is None


def test_return_allowed_property_is_visible():
    result = common.execute_and_fetch_all(user_cursor(), "MATCH (n:Employee) RETURN n.name AS name;")
    assert len(result) == 1
    assert result[0][0] == "Alice"


def test_return_denied_property_bracket_access_is_null():
    result = common.execute_and_fetch_all(user_cursor(), 'MATCH (n:Employee) RETURN n["ssn"] AS ssn;')
    assert len(result) == 1
    assert result[0][0] is None


def test_properties_includes_denied_key_with_null_value():
    result = common.execute_and_fetch_all(user_cursor(), "MATCH (n:Employee) RETURN properties(n) AS props;")
    assert len(result) == 1
    props = result[0][0]
    assert "ssn" in props
    assert props["ssn"] is None
    assert props["name"] == "Alice"


def test_keys_omits_denied_key():
    result = common.execute_and_fetch_all(user_cursor(), "MATCH (n:Employee) RETURN keys(n) AS k;")
    assert len(result) == 1
    keys = result[0][0]
    assert "name" in keys
    assert "ssn" not in keys


def test_return_whole_node_redacts_denied_property():
    result = common.execute_and_fetch_all(user_cursor(), "MATCH (n:Employee) RETURN n;")
    assert len(result) == 1
    node = result[0][0]
    assert node.properties["name"] == "Alice"
    assert "ssn" in node.properties
    assert node.properties["ssn"] is None


def test_where_on_denied_property_returns_no_rows():
    result = common.execute_and_fetch_all(user_cursor(), "MATCH (n:Employee) WHERE n.ssn = '123-45-6789' RETURN n;")
    assert len(result) == 0


def test_where_is_not_null_on_denied_property_returns_no_rows():
    result = common.execute_and_fetch_all(user_cursor(), "MATCH (n:Employee) WHERE n.ssn IS NOT NULL RETURN n;")
    assert len(result) == 0


def test_collect_denied_property_returns_nulls():
    result = common.execute_and_fetch_all(user_cursor(), "MATCH (n:Employee) RETURN collect(n.ssn) AS collected;")
    assert len(result) == 1
    assert result[0][0] == [None]


def test_count_denied_property_returns_zero():
    result = common.execute_and_fetch_all(user_cursor(), "MATCH (n:Employee) RETURN count(n.ssn) AS cnt;")
    assert len(result) == 1
    assert result[0][0] == 0


def test_dump_database_omits_denied_property():
    rows = common.execute_and_fetch_all(user_cursor(), "DUMP DATABASE;")
    cypher = "\n".join(row[0] for row in rows)
    assert "Alice" in cypher
    assert "123-45-6789" not in cypher


def test_edge_denied_property_is_null():
    result = common.execute_and_fetch_all(
        user_cursor(),
        "MATCH (:Employee)-[r:WORKS_AT]->(:Company) RETURN r.start_date AS sd, r.secret_code AS sc;",
    )
    assert len(result) == 1
    assert result[0][0] == "2020-01-01"
    assert result[0][1] is None


def test_path_traversal_redacts_denied_property():
    result = common.execute_and_fetch_all(
        user_cursor(),
        "MATCH p = (:Person {name: 'Start'})-[*]->(:Person {name: 'End'}) RETURN nodes(p) AS ns;",
    )
    assert len(result) == 1
    nodes = result[0][0]
    # The path goes Start -> Employee(Alice) -> End
    # Middle node (Employee) should have ssn redacted
    for node in nodes:
        if "Employee" in [l.name for l in node.labels]:
            assert "ssn" in node.properties
            assert node.properties["ssn"] is None
            assert node.properties["name"] == "Alice"


def test_path_filter_on_denied_property_returns_no_rows():
    result = common.execute_and_fetch_all(
        user_cursor(),
        "MATCH p = (:Person)-[*]->(n:Employee) WHERE n.ssn = '123-45-6789' RETURN p;",
    )
    assert len(result) == 0


def test_values_nulls_denied_property():
    result = common.execute_and_fetch_all(user_cursor(), "MATCH (n:Employee) RETURN values(n) AS v;")
    assert len(result) == 1
    vals = result[0][0]
    assert None in vals
    assert "Alice" in vals


def test_admin_sees_all_properties():
    result = common.execute_and_fetch_all(admin_cursor(), "MATCH (n:Employee) RETURN n.ssn AS ssn, n.name AS name;")
    assert len(result) == 1
    assert result[0][0] == "123-45-6789"
    assert result[0][1] == "Alice"
