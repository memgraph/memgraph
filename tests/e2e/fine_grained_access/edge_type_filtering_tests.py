import sys

import common
import pytest


@pytest.mark.parametrize("switch", [False, True])
def test_all_edge_types_all_labels_granted(switch):
    admin_connection = common.connect(username="admin", password="test")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON LABELS * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON EDGE_TYPES * TO user;")
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    results = common.execute_and_fetch_all(user_connection.cursor(), "MATCH (n)-[r]->(m) RETURN n,r,m;")

    assert len(results) == 3


@pytest.mark.parametrize("switch", [False, True])
def test_deny_all_edge_types_and_all_labels(switch):
    admin_connection = common.connect(username="admin", password="test")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT NOTHING ON LABELS * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT NOTHING ON EDGE_TYPES * TO user;")
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    results = common.execute_and_fetch_all(user_connection.cursor(), "MATCH (n)-[r]->(m) RETURN n,r,m;")

    assert len(results) == 0


@pytest.mark.parametrize("switch", [False, True])
def test_revoke_all_edge_types_and_all_labels(switch):
    admin_connection = common.connect(username="admin", password="test")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE LABELS * FROM user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE EDGE_TYPES * FROM user;")
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    results = common.execute_and_fetch_all(user_connection.cursor(), "MATCH (n)-[r]->(m) RETURN n,r,m;")

    assert len(results) == 0


@pytest.mark.parametrize("switch", [False, True])
def test_deny_edge_type(switch):
    admin_connection = common.connect(username="admin", password="test")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON LABELS :label1, :label2, :label3 TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON EDGE_TYPES :edgeType2 TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT NOTHING ON EDGE_TYPES :edgeType1 TO user;")
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    results = common.execute_and_fetch_all(user_connection.cursor(), "MATCH (n)-[r]->(m) RETURN n,r,m;")

    assert len(results) == 2


@pytest.mark.parametrize("switch", [False, True])
def test_denied_node_label(switch):
    admin_connection = common.connect(username="admin", password="test")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON LABELS :label1,:label3 TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON EDGE_TYPES :edgeType1, :edgeType2 TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT NOTHING ON LABELS :label2 TO user;")
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    results = common.execute_and_fetch_all(user_connection.cursor(), "MATCH (n)-[r]->(m) RETURN n,r,m;")

    assert len(results) == 2


@pytest.mark.parametrize("switch", [False, True])
def test_denied_one_of_node_label(switch):
    admin_connection = common.connect(username="admin", password="test")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON LABELS :label1,:label2 TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON EDGE_TYPES :edgeType1, :edgeType2 TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT NOTHING ON LABELS :label3 TO user;")
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    results = common.execute_and_fetch_all(user_connection.cursor(), "MATCH (n)-[r]->(m) RETURN n,r,m;")

    assert len(results) == 1


@pytest.mark.parametrize("switch", [False, True])
def test_revoke_all_labels(switch):
    admin_connection = common.connect(username="admin", password="test")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE LABELS * FROM user;")
    user_connection = common.connect(username="user", password="test")
    if switch:
        common.switch_db(user_connection.cursor())
    results = common.execute_and_fetch_all(user_connection.cursor(), "MATCH (n)-[r]->(m) RETURN n,r,m;")

    assert len(results) == 0


@pytest.mark.parametrize("switch", [False, True])
def test_revoke_all_edge_types(switch):
    admin_connection = common.connect(username="admin", password="test")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE EDGE_TYPES * FROM user;")
    user_connection = common.connect(username="user", password="test")
    if switch:
        common.switch_db(user_connection.cursor())
    results = common.execute_and_fetch_all(user_connection.cursor(), "MATCH (n)-[r]->(m) RETURN n,r,m;")

    assert len(results) == 0


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
