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


def test_properties_function_includes_denied_key_as_null():
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


def test_bolt_serialized_node_omits_denied_property():
    result = common.execute_and_fetch_all(user_cursor(), "MATCH (n:Employee) RETURN n;")
    assert len(result) == 1
    node = result[0][0]
    assert node.properties["name"] == "Alice"
    assert "ssn" not in node.properties


def test_where_on_denied_property_returns_no_rows():
    result = common.execute_and_fetch_all(user_cursor(), "MATCH (n:Employee) WHERE n.ssn = '123-45-6789' RETURN n;")
    assert len(result) == 0


def test_collect_denied_property_returns_empty():
    result = common.execute_and_fetch_all(user_cursor(), "MATCH (n:Employee) RETURN collect(n.ssn) AS collected;")
    assert len(result) == 1
    assert result[0][0] == []


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


def test_derive_strips_denied_property():
    result = common.execute_and_fetch_all(
        user_cursor(),
        """
        MATCH p = (:Person {name: 'Start'})-[:KNOWS]->(:Employee)
        WITH derive(p, {virtualEdgeType: 'V'}) AS g
        UNWIND g.nodes AS n
        WITH n WHERE 'Employee' IN labels(n)
        RETURN keys(n) AS k, n.name AS name, n.ssn AS ssn
        """,
    )
    assert len(result) == 1
    keys = result[0][0]
    assert "name" in keys
    assert "ssn" not in keys
    assert result[0][1] == "Alice"
    assert result[0][2] is None


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


def test_admin_sees_all_properties():
    result = common.execute_and_fetch_all(admin_cursor(), "MATCH (n:Employee) RETURN n.ssn AS ssn, n.name AS name;")
    assert len(result) == 1
    assert result[0][0] == "123-45-6789"
    assert result[0][1] == "Alice"


# --- SET PROPERTY (write) permission tests ---


# --- Write-path enforcement tests ---


def test_set_property_denied_throws():
    """SET n.prop fails when user lacks SET PROPERTY permission on that property."""
    admin = admin_cursor()
    common.execute_and_fetch_all(admin, "GRANT SET PROPERTY {*} ON NODES CONTAINING LABELS :Employee TO user;")
    common.execute_and_fetch_all(admin, "DENY SET PROPERTY {ssn} ON NODES CONTAINING LABELS :Employee TO user;")
    try:
        with pytest.raises(Exception, match="(?i)property"):
            common.execute_and_fetch_all(user_cursor(), "MATCH (n:Employee) SET n.ssn = 'new-ssn';")
    finally:
        common.execute_and_fetch_all(admin, "REVOKE SET PROPERTY {*} ON NODES CONTAINING LABELS :Employee FROM user;")
        common.execute_and_fetch_all(admin, "REVOKE SET PROPERTY {ssn} ON NODES CONTAINING LABELS :Employee FROM user;")


def test_set_property_allowed_succeeds():
    """SET n.prop succeeds when user has SET PROPERTY permission."""
    admin = admin_cursor()
    common.execute_and_fetch_all(admin, "GRANT SET PROPERTY {name} ON NODES CONTAINING LABELS :Employee TO user;")
    try:
        common.execute_and_fetch_all(user_cursor(), "MATCH (n:Employee) SET n.name = 'Bob';")
        result = common.execute_and_fetch_all(admin_cursor(), "MATCH (n:Employee) RETURN n.name;")
        assert result[0][0] == "Bob"
    finally:
        # Restore original value
        common.execute_and_fetch_all(admin, "MATCH (n:Employee) SET n.name = 'Alice';")
        common.execute_and_fetch_all(
            admin, "REVOKE SET PROPERTY {name} ON NODES CONTAINING LABELS :Employee FROM user;"
        )


def test_set_property_no_write_rules_denied():
    """SET n.prop fails when no SET PROPERTY rules exist (no rules = no access)."""
    with pytest.raises(Exception, match="(?i)property"):
        common.execute_and_fetch_all(user_cursor(), "MATCH (n:Employee) SET n.name = 'Bob';")


def test_remove_property_denied_throws():
    """REMOVE n.prop fails when user lacks SET PROPERTY permission."""
    admin = admin_cursor()
    common.execute_and_fetch_all(admin, "GRANT SET PROPERTY {*} ON NODES CONTAINING LABELS :Employee TO user;")
    common.execute_and_fetch_all(admin, "DENY SET PROPERTY {ssn} ON NODES CONTAINING LABELS :Employee TO user;")
    try:
        with pytest.raises(Exception, match="(?i)property"):
            common.execute_and_fetch_all(user_cursor(), "MATCH (n:Employee) REMOVE n.ssn;")
    finally:
        common.execute_and_fetch_all(admin, "REVOKE SET PROPERTY {*} ON NODES CONTAINING LABELS :Employee FROM user;")
        common.execute_and_fetch_all(admin, "REVOKE SET PROPERTY {ssn} ON NODES CONTAINING LABELS :Employee FROM user;")


def test_set_properties_update_denied_property_throws():
    """SET n += {prop: val} fails if any property in the map is denied."""
    admin = admin_cursor()
    common.execute_and_fetch_all(admin, "GRANT SET PROPERTY {*} ON NODES CONTAINING LABELS :Employee TO user;")
    common.execute_and_fetch_all(admin, "DENY SET PROPERTY {ssn} ON NODES CONTAINING LABELS :Employee TO user;")
    try:
        with pytest.raises(Exception, match="(?i)property"):
            common.execute_and_fetch_all(user_cursor(), "MATCH (n:Employee) SET n += {ssn: '000', name: 'Bob'};")
    finally:
        common.execute_and_fetch_all(admin, "REVOKE SET PROPERTY {*} ON NODES CONTAINING LABELS :Employee FROM user;")
        common.execute_and_fetch_all(admin, "REVOKE SET PROPERTY {ssn} ON NODES CONTAINING LABELS :Employee FROM user;")


def test_set_properties_replace_denied_property_throws():
    """SET n = {prop: val} fails if any property being set or removed is denied."""
    admin = admin_cursor()
    common.execute_and_fetch_all(admin, "GRANT SET PROPERTY {*} ON NODES CONTAINING LABELS :Employee TO user;")
    common.execute_and_fetch_all(admin, "DENY SET PROPERTY {ssn} ON NODES CONTAINING LABELS :Employee TO user;")
    try:
        # This replaces all properties. Even though ssn isn't in the new map,
        # it would be removed, which requires write permission.
        with pytest.raises(Exception, match="(?i)property"):
            common.execute_and_fetch_all(user_cursor(), "MATCH (n:Employee) SET n = {name: 'Bob'};")
    finally:
        common.execute_and_fetch_all(admin, "REVOKE SET PROPERTY {*} ON NODES CONTAINING LABELS :Employee FROM user;")
        common.execute_and_fetch_all(admin, "REVOKE SET PROPERTY {ssn} ON NODES CONTAINING LABELS :Employee FROM user;")


def test_create_node_denied_property_throws():
    """CREATE with a denied property fails."""
    admin = admin_cursor()
    common.execute_and_fetch_all(admin, "GRANT SET PROPERTY {*} ON NODES CONTAINING LABELS :Employee TO user;")
    common.execute_and_fetch_all(admin, "DENY SET PROPERTY {ssn} ON NODES CONTAINING LABELS :Employee TO user;")
    try:
        with pytest.raises(Exception, match="(?i)property"):
            common.execute_and_fetch_all(user_cursor(), "CREATE (:Employee {name: 'Eve', ssn: '999'});")
    finally:
        # Clean up in case it did get created
        common.execute_and_fetch_all(admin, "MATCH (n:Employee {name: 'Eve'}) DETACH DELETE n;")
        common.execute_and_fetch_all(admin, "REVOKE SET PROPERTY {*} ON NODES CONTAINING LABELS :Employee FROM user;")
        common.execute_and_fetch_all(admin, "REVOKE SET PROPERTY {ssn} ON NODES CONTAINING LABELS :Employee FROM user;")


def test_create_node_allowed_succeeds():
    """CREATE with only allowed properties succeeds."""
    admin = admin_cursor()
    common.execute_and_fetch_all(admin, "GRANT SET PROPERTY {name} ON NODES CONTAINING LABELS :Employee TO user;")
    try:
        common.execute_and_fetch_all(user_cursor(), "CREATE (:Employee {name: 'Eve'});")
        result = common.execute_and_fetch_all(admin_cursor(), "MATCH (n:Employee {name: 'Eve'}) RETURN n.name;")
        assert len(result) == 1
        assert result[0][0] == "Eve"
    finally:
        common.execute_and_fetch_all(admin, "MATCH (n:Employee {name: 'Eve'}) DETACH DELETE n;")
        common.execute_and_fetch_all(
            admin, "REVOKE SET PROPERTY {name} ON NODES CONTAINING LABELS :Employee FROM user;"
        )


def test_create_edge_denied_property_throws():
    """CREATE edge with a denied property fails."""
    admin = admin_cursor()
    common.execute_and_fetch_all(admin, "GRANT SET PROPERTY {*} ON EDGES OF TYPE :WORKS_AT TO user;")
    common.execute_and_fetch_all(admin, "DENY SET PROPERTY {secret_code} ON EDGES OF TYPE :WORKS_AT TO user;")
    try:
        with pytest.raises(Exception, match="(?i)property"):
            common.execute_and_fetch_all(
                user_cursor(),
                "MATCH (e:Employee {name: 'Alice'}), (c:Company {name: 'Acme'}) "
                "CREATE (e)-[:WORKS_AT {secret_code: 'Y99'}]->(c);",
            )
    finally:
        common.execute_and_fetch_all(admin, "REVOKE SET PROPERTY {*} ON EDGES OF TYPE :WORKS_AT FROM user;")
        common.execute_and_fetch_all(admin, "REVOKE SET PROPERTY {secret_code} ON EDGES OF TYPE :WORKS_AT FROM user;")


def test_set_edge_property_denied_throws():
    """SET on edge property fails when denied."""
    admin = admin_cursor()
    common.execute_and_fetch_all(admin, "GRANT SET PROPERTY {*} ON EDGES OF TYPE :WORKS_AT TO user;")
    common.execute_and_fetch_all(admin, "DENY SET PROPERTY {secret_code} ON EDGES OF TYPE :WORKS_AT TO user;")
    try:
        with pytest.raises(Exception, match="(?i)property"):
            common.execute_and_fetch_all(user_cursor(), "MATCH ()-[r:WORKS_AT]->() SET r.secret_code = 'hacked';")
    finally:
        common.execute_and_fetch_all(admin, "REVOKE SET PROPERTY {*} ON EDGES OF TYPE :WORKS_AT FROM user;")
        common.execute_and_fetch_all(admin, "REVOKE SET PROPERTY {secret_code} ON EDGES OF TYPE :WORKS_AT FROM user;")


def test_set_edge_property_allowed_succeeds():
    """SET on edge property succeeds when user has SET PROPERTY permission."""
    admin = admin_cursor()
    common.execute_and_fetch_all(admin, "GRANT SET PROPERTY {start_date} ON EDGES OF TYPE :WORKS_AT TO user;")
    try:
        common.execute_and_fetch_all(user_cursor(), "MATCH ()-[r:WORKS_AT]->() SET r.start_date = '2025-01-01';")
        result = common.execute_and_fetch_all(admin_cursor(), "MATCH ()-[r:WORKS_AT]->() RETURN r.start_date;")
        assert result[0][0] == "2025-01-01"
    finally:
        common.execute_and_fetch_all(admin, "MATCH ()-[r:WORKS_AT]->() SET r.start_date = '2020-01-01';")
        common.execute_and_fetch_all(admin, "REVOKE SET PROPERTY {start_date} ON EDGES OF TYPE :WORKS_AT FROM user;")


def test_merge_node_denied_property_throws():
    """MERGE with a denied property fails."""
    admin = admin_cursor()
    common.execute_and_fetch_all(admin, "GRANT SET PROPERTY {*} ON NODES CONTAINING LABELS :Employee TO user;")
    common.execute_and_fetch_all(admin, "DENY SET PROPERTY {ssn} ON NODES CONTAINING LABELS :Employee TO user;")
    try:
        with pytest.raises(Exception, match="(?i)property"):
            common.execute_and_fetch_all(user_cursor(), "MERGE (n:Employee {name: 'Eve', ssn: '999'});")
    finally:
        common.execute_and_fetch_all(admin, "MATCH (n:Employee {name: 'Eve'}) DETACH DELETE n;")
        common.execute_and_fetch_all(admin, "REVOKE SET PROPERTY {*} ON NODES CONTAINING LABELS :Employee FROM user;")
        common.execute_and_fetch_all(admin, "REVOKE SET PROPERTY {ssn} ON NODES CONTAINING LABELS :Employee FROM user;")


def test_role_merge_deny_wins():
    """User PBAC + two roles: deny from one role overrides grant from the other."""
    admin = admin_cursor()
    try:
        # Role A grants READ {salary} on :Employee
        common.execute_and_fetch_all(admin, "CREATE ROLE role_a;")
        common.execute_and_fetch_all(admin, "GRANT READ {salary} ON NODES CONTAINING LABELS :Employee TO ROLE role_a;")
        # Role B denies READ {salary} on :Employee
        common.execute_and_fetch_all(admin, "CREATE ROLE role_b;")
        common.execute_and_fetch_all(admin, "DENY READ {salary} ON NODES CONTAINING LABELS :Employee TO ROLE role_b;")
        # Assign both roles
        common.execute_and_fetch_all(admin, "GRANT ROLE role_a, role_b TO user;")

        result = common.execute_and_fetch_all(
            user_cursor(), "MATCH (n:Employee) RETURN n.name AS name, n.salary AS salary;"
        )
        assert len(result) == 1
        assert result[0][0] == "Alice"  # user's own global GRANT READ {*} covers name
        assert result[0][1] is None  # role_b's per-entity DENY wins over role_a's GRANT
    finally:
        common.execute_and_fetch_all(admin, "REVOKE ROLE role_a, role_b FROM user;")
        common.execute_and_fetch_all(admin, "DROP ROLE role_a;")
        common.execute_and_fetch_all(admin, "DROP ROLE role_b;")


def test_create_after_a_label_write_masks_a_property_the_label_denies():
    # The CREATE reads a after the SET made it :Employee, whose ssn the user may not read.
    admin = admin_cursor()
    common.execute_and_fetch_all(admin, "GRANT SET PROPERTY {v} ON NODES CONTAINING LABELS :WvLeakCopy TO user;")
    common.execute_and_fetch_all(admin, "CREATE (:WvLeakSrc {ssn: 'S1'});")
    try:
        result = common.execute_and_fetch_all(
            user_cursor(),
            "MATCH (a:WvLeakSrc) SET a:Employee CREATE (x:WvLeakCopy {v: a{.*}.ssn}) RETURN x.v AS v;",
        )
        assert result == [(None,)]
    finally:
        common.execute_and_fetch_all(admin, "MATCH (n) WHERE n:WvLeakSrc OR n:WvLeakCopy DETACH DELETE n;")
        common.execute_and_fetch_all(admin, "REVOKE SET PROPERTY {v} ON NODES CONTAINING LABELS :WvLeakCopy FROM user;")


def test_a_union_leg_after_a_label_write_masks_a_property_the_label_denies():
    # The second leg runs after the first made a :Employee, whose ssn the user may not read.
    admin = admin_cursor()
    common.execute_and_fetch_all(admin, "GRANT SET PROPERTY {v} ON NODES CONTAINING LABELS :WvLeakCopy TO user;")
    common.execute_and_fetch_all(admin, "CREATE (:WvLeakSrc {ssn: 'S1'});")
    try:
        common.execute_and_fetch_all(
            user_cursor(),
            "MATCH (a:WvLeakSrc) SET a:Employee UNION ALL MATCH (a:WvLeakSrc) CREATE (:WvLeakCopy {v: a.ssn});",
        )
        result = common.execute_and_fetch_all(admin, "MATCH (x:WvLeakCopy) RETURN x.v AS v;")
        assert result == [(None,)]
    finally:
        common.execute_and_fetch_all(admin, "MATCH (n) WHERE n:WvLeakSrc OR n:WvLeakCopy DETACH DELETE n;")
        common.execute_and_fetch_all(admin, "REVOKE SET PROPERTY {v} ON NODES CONTAINING LABELS :WvLeakCopy FROM user;")


def test_create_edge_after_a_label_write_checks_the_new_label():
    # Clause order makes node 1 :WvSecretE (row a=3) before row a=1 creates an edge from it, which the user may not.
    admin = admin_cursor()
    for query in [
        "CREATE USER wv_edge IDENTIFIED BY 'test';",
        "GRANT ALL PRIVILEGES TO wv_edge;",
        "GRANT DATABASE * TO wv_edge;",
        "GRANT CREATE, READ, UPDATE, DELETE ON NODES CONTAINING LABELS * TO wv_edge;",
        "GRANT CREATE, READ, UPDATE, DELETE ON EDGES OF TYPE * TO wv_edge;",
        "GRANT READ, SET PROPERTY {*} ON NODES CONTAINING LABELS * TO wv_edge;",
        "GRANT CREATE, READ, UPDATE ON NODES CONTAINING LABELS :WvSecretE TO wv_edge;",
        "DENY CREATE EDGE ON NODES CONTAINING LABELS :WvSecretE TO wv_edge;",
        "UNWIND [1, 2, 3] AS i CREATE (:WvE {id: i});",
    ]:
        common.execute_and_fetch_all(admin, query)
    try:
        cursor = common.connect(username="wv_edge", password="test").cursor()
        with pytest.raises(Exception, match="CREATE EDGE"):
            common.execute_and_fetch_all(
                cursor,
                "MATCH (a:WvE) WHERE a.id IN [1, 3] WITH a ORDER BY a.id "
                "MATCH (b:WvE {id: CASE a.id WHEN 1 THEN 2 ELSE 1 END}) SET b:WvSecretE CREATE (a)-[:WvR]->(:WvEx);",
            )
    finally:
        common.execute_and_fetch_all(admin, "MATCH (n) WHERE n:WvE OR n:WvEx DETACH DELETE n;")
        common.execute_and_fetch_all(admin, "DROP USER wv_edge;")


def _wv_delete_user(name, *grants):
    admin = admin_cursor()
    for query in [
        f"CREATE USER {name} IDENTIFIED BY 'test';",
        f"GRANT ALL PRIVILEGES TO {name};",
        f"GRANT DATABASE * TO {name};",
        f"GRANT CREATE, READ, UPDATE, DELETE ON NODES CONTAINING LABELS * TO {name};",
        f"GRANT CREATE, READ, UPDATE, DELETE ON EDGES OF TYPE * TO {name};",
        f"GRANT READ, SET PROPERTY {{*}} ON NODES CONTAINING LABELS * TO {name};",
        *(grant.format(name=name) for grant in grants),
    ]:
        common.execute_and_fetch_all(admin, query)
    return common.connect(username=name, password="test").cursor()


WV_PAIRS = "MATCH (a:WvDel), (b:WvDel) WHERE a.id <> b.id "


# Each drain above the DELETE checks or reads before the later write; the deletes still land at the end of the part.
@pytest.mark.parametrize(
    "query",
    [
        WV_PAIRS + "DELETE b SET a:WvDelZ",
        WV_PAIRS + "DETACH DELETE b SET a:WvDelZ",
        WV_PAIRS + "SET a.k = 1 DELETE b SET a:WvDelZ",
        WV_PAIRS + "DELETE b SET a.k = 1 SET a:WvDelZ",
        WV_PAIRS + "DELETE b SET a:WvDelY SET a.k = 1",
        WV_PAIRS + "DELETE b SET a:WvDelY SET a.k = 1 SET a:WvDelZ",
        WV_PAIRS + "SET a:WvDelQ DELETE b SET a:WvDelZ",
        WV_PAIRS + "DELETE b SET a:WvDelZ RETURN count(*) AS c",
        "MATCH p = (a:WvDel)-[:WvDelR]->(), q = (b:WvDel)-[:WvDelR]->() WHERE a.id <> b.id DETACH DELETE q SET a:WvDelZ",
    ],
)
def test_a_drain_above_a_delete_does_not_apply_it_early(query):
    admin = admin_cursor()
    if "WvDelR" in query:
        common.execute_and_fetch_all(admin, "CREATE (a:WvDel {id: 1})-[:WvDelR]->(b:WvDel {id: 2})-[:WvDelR]->(a);")
    else:
        common.execute_and_fetch_all(admin, "CREATE (:WvDel {id: 1}), (:WvDel {id: 2});")
    try:
        cursor = _wv_delete_user("wv_del", "DENY DELETE ON NODES CONTAINING LABELS :WvDelUnrelated TO {name};")
        common.execute_and_fetch_all(cursor, query)
        assert common.execute_and_fetch_all(admin, "MATCH (n:WvDel) RETURN count(n);") == [(0,)]
    finally:
        common.execute_and_fetch_all(admin, "MATCH (n:WvDel) DETACH DELETE n;")
        common.execute_and_fetch_all(admin, "DROP USER wv_del;")


def test_a_delete_checks_its_permission_before_a_later_label_removal():
    # Row a=2 removes :WvDelSecret from node 2 before row a=1 deletes it; the check must see the label.
    admin = admin_cursor()
    common.execute_and_fetch_all(admin, "CREATE (:WvDel {id: 1}), (:WvDel:WvDelSecret {id: 2});")
    try:
        cursor = _wv_delete_user(
            "wv_del",
            "GRANT CREATE, READ, UPDATE ON NODES CONTAINING LABELS :WvDel, :WvDelSecret MATCHING EXACTLY TO {name};",
            "DENY DELETE ON NODES CONTAINING LABELS :WvDel, :WvDelSecret MATCHING EXACTLY TO {name};",
        )
        with pytest.raises(Exception, match="DELETE permission"):
            common.execute_and_fetch_all(
                cursor, WV_PAIRS + "WITH a, b ORDER BY a.id DESC DELETE b REMOVE a:WvDelSecret"
            )
        assert common.execute_and_fetch_all(admin, "MATCH (n:WvDel) RETURN count(n);") == [(2,)]
    finally:
        common.execute_and_fetch_all(admin, "MATCH (n:WvDel) DETACH DELETE n;")
        common.execute_and_fetch_all(admin, "DROP USER wv_del;")


def test_a_delete_checks_its_permission_on_the_labels_its_clause_sees():
    # Each node has only :WvDel when its DELETE runs; the label a later clause adds does not count.
    admin = admin_cursor()
    common.execute_and_fetch_all(admin, "CREATE (:WvDel {id: 1}), (:WvDel {id: 2});")
    try:
        cursor = _wv_delete_user(
            "wv_del",
            "GRANT CREATE, READ, UPDATE ON NODES CONTAINING LABELS :WvDel, :WvDelSecret MATCHING EXACTLY TO {name};",
            "DENY DELETE ON NODES CONTAINING LABELS :WvDel, :WvDelSecret MATCHING EXACTLY TO {name};",
        )
        common.execute_and_fetch_all(cursor, WV_PAIRS + "DELETE b SET a:WvDelSecret")
        assert common.execute_and_fetch_all(admin, "MATCH (n:WvDel) RETURN count(n);") == [(0,)]
    finally:
        common.execute_and_fetch_all(admin, "MATCH (n:WvDel) DETACH DELETE n;")
        common.execute_and_fetch_all(admin, "DROP USER wv_del;")


def test_a_property_read_of_a_deleted_node_raises_for_a_user_with_property_rules():
    # Row a=2 reads node 2 after row a=1 deleted it: the read raises, as it does for admin, and nothing commits.
    admin = admin_cursor()
    common.execute_and_fetch_all(admin, "CREATE (:WvDel {id: 1}), (:WvDel {id: 2});")
    try:
        cursor = _wv_delete_user(
            "wv_del", "DENY SET PROPERTY {{x}} ON NODES CONTAINING LABELS :WvDelUnrelated TO {name};"
        )
        with pytest.raises(Exception, match="deleted"):
            common.execute_and_fetch_all(cursor, WV_PAIRS + "DETACH DELETE b CREATE (:WvDelOut {v: a.id})")
        assert common.execute_and_fetch_all(admin, "MATCH (n:WvDel) RETURN count(n);") == [(2,)]
        assert common.execute_and_fetch_all(admin, "MATCH (n:WvDelOut) RETURN count(n);") == [(0,)]
    finally:
        common.execute_and_fetch_all(admin, "MATCH (n) WHERE n:WvDel OR n:WvDelOut DETACH DELETE n;")
        common.execute_and_fetch_all(admin, "DROP USER wv_del;")


def test_a_with_ends_what_a_later_drain_may_defer():
    # The WITH applies the deletes, so the later label write meets a deleted node.
    admin = admin_cursor()
    common.execute_and_fetch_all(admin, "CREATE (:WvDel {id: 1}), (:WvDel {id: 2});")
    try:
        cursor = _wv_delete_user("wv_del", "DENY DELETE ON NODES CONTAINING LABELS :WvDelUnrelated TO {name};")
        with pytest.raises(Exception, match="deleted"):
            common.execute_and_fetch_all(cursor, WV_PAIRS + "DELETE b WITH a SET a.k = 1 SET a:WvDelZ")
        assert common.execute_and_fetch_all(admin, "MATCH (n:WvDel) RETURN count(n);") == [(2,)]
    finally:
        common.execute_and_fetch_all(admin, "MATCH (n:WvDel) DETACH DELETE n;")
        common.execute_and_fetch_all(admin, "DROP USER wv_del;")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
