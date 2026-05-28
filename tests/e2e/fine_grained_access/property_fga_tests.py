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

import json
import sys

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


def test_values_omits_denied_key():
    result = common.execute_and_fetch_all(user_cursor(), "MATCH (n:Employee) RETURN keys(n) AS k, values(n) AS v;")
    assert len(result) == 1
    keys = result[0][0]
    vals = result[0][1]
    assert len(keys) == len(vals)
    assert "name" in keys
    assert "ssn" not in keys
    assert "Alice" in vals
    assert None not in vals


def test_return_whole_node_redacts_denied_property():
    result = common.execute_and_fetch_all(user_cursor(), "MATCH (n:Employee) RETURN n;")
    assert len(result) == 1
    node = result[0][0]
    assert node.properties["name"] == "Alice"
    assert "ssn" not in node.properties


def test_where_on_denied_property_returns_no_rows():
    result = common.execute_and_fetch_all(user_cursor(), "MATCH (n:Employee) WHERE n.ssn = '123-45-6789' RETURN n;")
    assert len(result) == 0


def test_where_is_not_null_on_denied_property_returns_no_rows():
    result = common.execute_and_fetch_all(user_cursor(), "MATCH (n:Employee) WHERE n.ssn IS NOT NULL RETURN n;")
    assert len(result) == 0


def test_collect_denied_property_returns_empty():
    result = common.execute_and_fetch_all(user_cursor(), "MATCH (n:Employee) RETURN collect(n.ssn) AS collected;")
    assert len(result) == 1
    assert result[0][0] == []


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
        if "Employee" in node.labels:
            assert "ssn" not in node.properties
            assert node.properties["name"] == "Alice"


def test_path_filter_on_denied_property_returns_no_rows():
    result = common.execute_and_fetch_all(
        user_cursor(),
        "MATCH p = (:Person)-[*]->(n:Employee) WHERE n.ssn = '123-45-6789' RETURN p;",
    )
    assert len(result) == 0


def test_show_schema_info_omits_denied_properties():
    schema = json.loads(common.execute_and_fetch_all(user_cursor(), "SHOW SCHEMA INFO;")[0][0])
    employee = next(n for n in schema["nodes"] if "Employee" in n["labels"])
    prop_keys = [p["key"] for p in employee["properties"]]
    assert "name" in prop_keys
    assert "ssn" not in prop_keys


def test_show_schema_info_admin_sees_all_properties():
    schema = json.loads(common.execute_and_fetch_all(admin_cursor(), "SHOW SCHEMA INFO;")[0][0])
    employee = next(n for n in schema["nodes"] if "Employee" in n["labels"])
    prop_keys = [p["key"] for p in employee["properties"]]
    assert "name" in prop_keys
    assert "ssn" in prop_keys


def test_explain_redacts_denied_property():
    rows = common.execute_and_fetch_all(user_cursor(), "EXPLAIN MATCH (n:Employee) WHERE n.ssn = 'X' RETURN n.ssn;")
    non_produce = [row[0] for row in rows if "Produce" not in row[0]]
    plan_text = "\n".join(non_produce)
    assert "ssn" not in plan_text
    assert "<redacted>" in plan_text


def test_explain_shows_allowed_property():
    rows = common.execute_and_fetch_all(user_cursor(), "EXPLAIN MATCH (n:Employee) WHERE n.name = 'X' RETURN n.name;")
    plan_text = "\n".join(row[0] for row in rows)
    assert "name" in plan_text
    assert "<redacted>" not in plan_text


def test_profile_redacts_denied_property():
    rows = common.execute_and_fetch_all(user_cursor(), "PROFILE MATCH (n:Employee) WHERE n.ssn = 'X' RETURN n.ssn;")
    non_produce = [row[0] for row in rows if "Produce" not in row[0]]
    operators = "\n".join(non_produce)
    assert "ssn" not in operators
    assert "<redacted>" in operators


def test_admin_sees_all_properties():
    result = common.execute_and_fetch_all(admin_cursor(), "MATCH (n:Employee) RETURN n.ssn AS ssn, n.name AS name;")
    assert len(result) == 1
    assert result[0][0] == "123-45-6789"
    assert result[0][1] == "Alice"


# --- SET PROPERTY (write) permission tests ---


def show_privileges_for(cursor, role):
    return common.execute_and_fetch_all(cursor, f"SHOW PRIVILEGES FOR {role};")


def find_property_privilege(rows, substring):
    """Find rows whose privilege column (index 0) contains the given substring."""
    return [r for r in rows if substring in r[0]]


def test_grant_set_property_shows_in_privileges():
    admin = admin_cursor()
    common.execute_and_fetch_all(admin, "GRANT SET PROPERTY {salary} ON NODES Employee TO user;")
    rows = show_privileges_for(admin, "user")
    matches = find_property_privilege(rows, "SET PROPERTY")
    assert len(matches) > 0
    salary_rows = [r for r in matches if "salary" in r[0] and "Employee" in r[0]]
    assert len(salary_rows) == 1
    assert salary_rows[0][1] == "GRANT"
    # Clean up
    common.execute_and_fetch_all(admin, "REVOKE SET PROPERTY {salary} ON NODES Employee FROM user;")


def test_deny_set_property_shows_in_privileges():
    admin = admin_cursor()
    common.execute_and_fetch_all(admin, "DENY SET PROPERTY {name} ON NODES Employee TO user;")
    rows = show_privileges_for(admin, "user")
    matches = find_property_privilege(rows, "SET PROPERTY")
    name_rows = [r for r in matches if "name" in r[0] and "Employee" in r[0]]
    assert len(name_rows) == 1
    assert name_rows[0][1] == "DENY"
    # Clean up
    common.execute_and_fetch_all(admin, "REVOKE SET PROPERTY {name} ON NODES Employee FROM user;")


def test_revoke_set_property_removes_from_privileges():
    admin = admin_cursor()
    common.execute_and_fetch_all(admin, "GRANT SET PROPERTY {salary} ON NODES Employee TO user;")
    common.execute_and_fetch_all(admin, "REVOKE SET PROPERTY {salary} ON NODES Employee FROM user;")
    rows = show_privileges_for(admin, "user")
    matches = find_property_privilege(rows, "SET PROPERTY")
    salary_rows = [r for r in matches if "salary" in r[0] and "Employee" in r[0]]
    assert len(salary_rows) == 0


def test_grant_set_property_independent_of_read():
    admin = admin_cursor()
    common.execute_and_fetch_all(admin, "GRANT SET PROPERTY {salary} ON NODES Employee TO user;")
    rows = show_privileges_for(admin, "user")
    # READ {*} grant on Employee should still be present
    read_rows = find_property_privilege(rows, "READ")
    employee_read = [r for r in read_rows if "Employee" in r[0]]
    assert len(employee_read) > 0
    # SET PROPERTY should also be present
    write_rows = find_property_privilege(rows, "SET PROPERTY")
    assert len(write_rows) > 0
    # Clean up
    common.execute_and_fetch_all(admin, "REVOKE SET PROPERTY {salary} ON NODES Employee FROM user;")


def test_grant_set_property_on_edge_type():
    admin = admin_cursor()
    common.execute_and_fetch_all(admin, "GRANT SET PROPERTY {start_date} ON RELATIONSHIPS WORKS_AT TO user;")
    rows = show_privileges_for(admin, "user")
    matches = find_property_privilege(rows, "SET PROPERTY")
    edge_rows = [r for r in matches if "start_date" in r[0] and "WORKS_AT" in r[0]]
    assert len(edge_rows) == 1
    assert edge_rows[0][1] == "GRANT"
    # Clean up
    common.execute_and_fetch_all(admin, "REVOKE SET PROPERTY {start_date} ON RELATIONSHIPS WORKS_AT FROM user;")


# --- Write-path enforcement tests ---


def test_set_property_denied_throws():
    """SET n.prop fails when user lacks SET PROPERTY permission on that property."""
    admin = admin_cursor()
    common.execute_and_fetch_all(admin, "GRANT SET PROPERTY {*} ON NODES Employee TO user;")
    common.execute_and_fetch_all(admin, "DENY SET PROPERTY {ssn} ON NODES Employee TO user;")
    try:
        with pytest.raises(Exception, match="(?i)property"):
            common.execute_and_fetch_all(user_cursor(), "MATCH (n:Employee) SET n.ssn = 'new-ssn';")
    finally:
        common.execute_and_fetch_all(admin, "REVOKE SET PROPERTY {*} ON NODES Employee FROM user;")
        common.execute_and_fetch_all(admin, "REVOKE SET PROPERTY {ssn} ON NODES Employee FROM user;")


def test_set_property_allowed_succeeds():
    """SET n.prop succeeds when user has SET PROPERTY permission."""
    admin = admin_cursor()
    common.execute_and_fetch_all(admin, "GRANT SET PROPERTY {name} ON NODES Employee TO user;")
    try:
        common.execute_and_fetch_all(user_cursor(), "MATCH (n:Employee) SET n.name = 'Bob';")
        result = common.execute_and_fetch_all(admin_cursor(), "MATCH (n:Employee) RETURN n.name;")
        assert result[0][0] == "Bob"
    finally:
        # Restore original value
        common.execute_and_fetch_all(admin, "MATCH (n:Employee) SET n.name = 'Alice';")
        common.execute_and_fetch_all(admin, "REVOKE SET PROPERTY {name} ON NODES Employee FROM user;")


def test_set_property_no_write_rules_allows():
    """SET n.prop succeeds when no SET PROPERTY rules exist (no rules = no restriction)."""
    admin = admin_cursor()
    try:
        common.execute_and_fetch_all(user_cursor(), "MATCH (n:Employee) SET n.name = 'Bob';")
        result = common.execute_and_fetch_all(admin, "MATCH (n:Employee) RETURN n.name;")
        assert result[0][0] == "Bob"
    finally:
        common.execute_and_fetch_all(admin, "MATCH (n:Employee) SET n.name = 'Alice';")


def test_remove_property_denied_throws():
    """REMOVE n.prop fails when user lacks SET PROPERTY permission."""
    admin = admin_cursor()
    common.execute_and_fetch_all(admin, "GRANT SET PROPERTY {*} ON NODES Employee TO user;")
    common.execute_and_fetch_all(admin, "DENY SET PROPERTY {ssn} ON NODES Employee TO user;")
    try:
        with pytest.raises(Exception, match="(?i)property"):
            common.execute_and_fetch_all(user_cursor(), "MATCH (n:Employee) REMOVE n.ssn;")
    finally:
        common.execute_and_fetch_all(admin, "REVOKE SET PROPERTY {*} ON NODES Employee FROM user;")
        common.execute_and_fetch_all(admin, "REVOKE SET PROPERTY {ssn} ON NODES Employee FROM user;")


def test_set_properties_update_denied_property_throws():
    """SET n += {prop: val} fails if any property in the map is denied."""
    admin = admin_cursor()
    common.execute_and_fetch_all(admin, "GRANT SET PROPERTY {*} ON NODES Employee TO user;")
    common.execute_and_fetch_all(admin, "DENY SET PROPERTY {ssn} ON NODES Employee TO user;")
    try:
        with pytest.raises(Exception, match="(?i)property"):
            common.execute_and_fetch_all(user_cursor(), "MATCH (n:Employee) SET n += {ssn: '000', name: 'Bob'};")
    finally:
        common.execute_and_fetch_all(admin, "REVOKE SET PROPERTY {*} ON NODES Employee FROM user;")
        common.execute_and_fetch_all(admin, "REVOKE SET PROPERTY {ssn} ON NODES Employee FROM user;")


def test_set_properties_replace_denied_property_throws():
    """SET n = {prop: val} fails if any property being set or removed is denied."""
    admin = admin_cursor()
    common.execute_and_fetch_all(admin, "GRANT SET PROPERTY {*} ON NODES Employee TO user;")
    common.execute_and_fetch_all(admin, "DENY SET PROPERTY {ssn} ON NODES Employee TO user;")
    try:
        # This replaces all properties. Even though ssn isn't in the new map,
        # it would be removed, which requires write permission.
        with pytest.raises(Exception, match="(?i)property"):
            common.execute_and_fetch_all(user_cursor(), "MATCH (n:Employee) SET n = {name: 'Bob'};")
    finally:
        common.execute_and_fetch_all(admin, "REVOKE SET PROPERTY {*} ON NODES Employee FROM user;")
        common.execute_and_fetch_all(admin, "REVOKE SET PROPERTY {ssn} ON NODES Employee FROM user;")


def test_create_node_denied_property_throws():
    """CREATE with a denied property fails."""
    admin = admin_cursor()
    common.execute_and_fetch_all(admin, "GRANT SET PROPERTY {*} ON NODES Employee TO user;")
    common.execute_and_fetch_all(admin, "DENY SET PROPERTY {ssn} ON NODES Employee TO user;")
    try:
        with pytest.raises(Exception, match="(?i)property"):
            common.execute_and_fetch_all(user_cursor(), "CREATE (:Employee {name: 'Eve', ssn: '999'});")
    finally:
        # Clean up in case it did get created
        common.execute_and_fetch_all(admin, "MATCH (n:Employee {name: 'Eve'}) DETACH DELETE n;")
        common.execute_and_fetch_all(admin, "REVOKE SET PROPERTY {*} ON NODES Employee FROM user;")
        common.execute_and_fetch_all(admin, "REVOKE SET PROPERTY {ssn} ON NODES Employee FROM user;")


def test_create_node_allowed_succeeds():
    """CREATE with only allowed properties succeeds."""
    admin = admin_cursor()
    common.execute_and_fetch_all(admin, "GRANT SET PROPERTY {name} ON NODES Employee TO user;")
    try:
        common.execute_and_fetch_all(user_cursor(), "CREATE (:Employee {name: 'Eve'});")
        result = common.execute_and_fetch_all(admin_cursor(), "MATCH (n:Employee {name: 'Eve'}) RETURN n.name;")
        assert len(result) == 1
        assert result[0][0] == "Eve"
    finally:
        common.execute_and_fetch_all(admin, "MATCH (n:Employee {name: 'Eve'}) DETACH DELETE n;")
        common.execute_and_fetch_all(admin, "REVOKE SET PROPERTY {name} ON NODES Employee FROM user;")


def test_create_edge_denied_property_throws():
    """CREATE edge with a denied property fails."""
    admin = admin_cursor()
    common.execute_and_fetch_all(admin, "GRANT SET PROPERTY {*} ON RELATIONSHIPS WORKS_AT TO user;")
    common.execute_and_fetch_all(admin, "DENY SET PROPERTY {secret_code} ON RELATIONSHIPS WORKS_AT TO user;")
    try:
        with pytest.raises(Exception, match="(?i)property"):
            common.execute_and_fetch_all(
                user_cursor(),
                "MATCH (e:Employee {name: 'Alice'}), (c:Company {name: 'Acme'}) "
                "CREATE (e)-[:WORKS_AT {secret_code: 'Y99'}]->(c);",
            )
    finally:
        common.execute_and_fetch_all(admin, "REVOKE SET PROPERTY {*} ON RELATIONSHIPS WORKS_AT FROM user;")
        common.execute_and_fetch_all(admin, "REVOKE SET PROPERTY {secret_code} ON RELATIONSHIPS WORKS_AT FROM user;")


def test_set_edge_property_denied_throws():
    """SET on edge property fails when denied."""
    admin = admin_cursor()
    common.execute_and_fetch_all(admin, "GRANT SET PROPERTY {*} ON RELATIONSHIPS WORKS_AT TO user;")
    common.execute_and_fetch_all(admin, "DENY SET PROPERTY {secret_code} ON RELATIONSHIPS WORKS_AT TO user;")
    try:
        with pytest.raises(Exception, match="(?i)property"):
            common.execute_and_fetch_all(user_cursor(), "MATCH ()-[r:WORKS_AT]->() SET r.secret_code = 'hacked';")
    finally:
        common.execute_and_fetch_all(admin, "REVOKE SET PROPERTY {*} ON RELATIONSHIPS WORKS_AT FROM user;")
        common.execute_and_fetch_all(admin, "REVOKE SET PROPERTY {secret_code} ON RELATIONSHIPS WORKS_AT FROM user;")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
