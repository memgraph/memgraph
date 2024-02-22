import sys

import common
import pytest


@pytest.mark.parametrize("switch", [False, True])
def test_weighted_shortest_path_all_edge_types_all_labels_granted(switch):
    admin_connection = common.connect(username="admin", password="test")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE LABELS * FROM user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE EDGE_TYPES * FROM user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON LABELS * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON EDGE_TYPES * TO user;")
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    total_paths_results = common.execute_and_fetch_all(
        user_connection.cursor(),
        "MATCH p=(n)-[r *wShortest (r, n | r.weight)]->(m) RETURN extract( node in nodes(p) | node.id);",
    )
    path_result = common.execute_and_fetch_all(
        user_connection.cursor(),
        "MATCH p=(n:label0)-[r *wShortest (r, n | r.weight) path_length]->(m:label4) RETURN path_length,nodes(p);",
    )

    expected_path = [0, 1, 3, 4, 5]
    expected_all_paths = [
        [0, 1],
        [0, 1, 2],
        [0, 1, 3],
        [0, 1, 3, 4],
        [0, 1, 3, 4, 5],
        [1, 2],
        [1, 3],
        [1, 3, 4],
        [1, 3, 4, 5],
        [2, 1],
        [2, 3],
        [2, 3, 4],
        [2, 3, 4, 5],
        [3, 4],
        [3, 4, 5],
        [4, 3],
        [4, 5],
    ]

    assert len(total_paths_results) == 16
    assert all(path[0] in expected_all_paths for path in total_paths_results)
    assert path_result[0][0] == 20
    assert all(node.id in expected_path for node in path_result[0][1])


@pytest.mark.parametrize("switch", [False, True])
def test_weighted_shortest_path_all_edge_types_all_labels_denied(switch):
    admin_connection = common.connect(username="admin", password="test")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE LABELS * FROM user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE EDGE_TYPES * FROM user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT NOTHING ON LABELS * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT NOTHING ON EDGE_TYPES * TO user;")
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    results = common.execute_and_fetch_all(
        user_connection.cursor(), "MATCH p=(n)-[r *wShortest (r, n | r.weight)]->(m) RETURN p;"
    )

    assert len(results) == 0


@pytest.mark.parametrize("switch", [False, True])
def test_weighted_shortest_path_denied_start(switch):
    admin_connection = common.connect(username="admin", password="test")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE LABELS * FROM user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE EDGE_TYPES * FROM user;")
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT READ ON LABELS :label1, :label2, :label3, :label4 TO user;"
    )
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON EDGE_TYPES * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT NOTHING ON LABELS :label0 TO user;")
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    path_length_result = common.execute_and_fetch_all(
        user_connection.cursor(),
        "MATCH p=(n:label0)-[r *wShortest (r, n | r.weight) path_length]->(m:label4) RETURN path_length;",
    )

    assert len(path_length_result) == 0


@pytest.mark.parametrize("switch", [False, True])
def test_weighted_shortest_path_denied_destination(switch):
    admin_connection = common.connect(username="admin", password="test")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE LABELS * FROM user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE EDGE_TYPES * FROM user;")
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT READ ON LABELS :label0, :label1, :label2, :label3 TO user;"
    )
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON EDGE_TYPES * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT NOTHING ON LABELS :label4 TO user;")
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    path_length_result = common.execute_and_fetch_all(
        user_connection.cursor(),
        "MATCH p=(n:label0)-[r *wShortest (r, n | r.weight) path_length]->(m:label4) RETURN path_length;",
    )

    assert len(path_length_result) == 0


@pytest.mark.parametrize("switch", [False, True])
def test_weighted_shortest_path_denied_label_1(switch):
    admin_connection = common.connect(username="admin", password="test")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE LABELS * FROM user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE EDGE_TYPES * FROM user;")
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT READ ON LABELS :label0, :label2, :label3, :label4 TO user;"
    )
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT NOTHING ON LABELS :label1 TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON EDGE_TYPES * TO user;")
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    total_paths_results = common.execute_and_fetch_all(
        user_connection.cursor(),
        "MATCH p=(n)-[r *wShortest (r, n | r.weight)]->(m) RETURN extract( node in nodes(p) | node.id);",
    )

    path_result = common.execute_and_fetch_all(
        user_connection.cursor(),
        "MATCH p=(n:label0)-[r *wShortest (r, n | r.weight) path_length]->(m:label4) RETURN path_length, nodes(p);",
    )

    expected_path = [0, 2, 3, 4, 5]

    expected_all_paths = [
        [0, 2],
        [0, 2, 3],
        [0, 2, 3, 4],
        [0, 2, 3, 4, 5],
        [2, 3],
        [2, 3, 4],
        [2, 3, 4, 5],
        [3, 4],
        [3, 4, 5],
        [4, 3],
        [4, 5],
    ]

    assert len(total_paths_results) == 11
    assert all(path[0] in expected_all_paths for path in total_paths_results)
    assert path_result[0][0] == 30
    assert all(node.id in expected_path for node in path_result[0][1])


@pytest.mark.parametrize("switch", [False, True])
def test_weighted_shortest_path_denied_edge_type_3(switch):
    admin_connection = common.connect(username="admin", password="test")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE LABELS * FROM user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE EDGE_TYPES * FROM user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON LABELS * TO user;")
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT READ ON EDGE_TYPES :edge_type_1, :edge_type_2, :edge_type_4 TO user;"
    )
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT NOTHING ON EDGE_TYPES :edge_type_3 TO user;")
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    path_result = common.execute_and_fetch_all(
        user_connection.cursor(),
        "MATCH p=(n:label0)-[r *wShortest (r, n | r.weight) path_length]->(m:label4) RETURN path_length, nodes(p);",
    )

    total_paths_results = common.execute_and_fetch_all(
        user_connection.cursor(),
        "MATCH p=(n)-[r *wShortest (r, n | r.weight)]->(m) RETURN extract( node in nodes(p) | node.id);",
    )

    expected_path = [0, 1, 2, 3, 5]
    expected_all_paths = [
        [0, 1],
        [0, 1, 2],
        [0, 1, 2, 4],
        [0, 1, 2, 4, 3],
        [0, 1, 2, 4, 5],
        [1, 2, 4, 3],
        [1, 2],
        [1, 2, 4],
        [1, 2, 4, 5],
        [2, 1],
        [2, 4, 3],
        [2, 4],
        [2, 4, 5],
        [3, 4],
        [3, 4, 5],
        [4, 3],
        [4, 5],
    ]

    assert len(total_paths_results) == 16
    assert all(path[0] in expected_all_paths for path in total_paths_results)
    assert path_result[0][0] == 25
    assert all(node.id in expected_path for node in path_result[0][1])


@pytest.mark.parametrize("switch", [False, True])
def test_dfs_all_edge_types_all_labels_granted(switch):
    admin_connection = common.connect(username="admin", password="test")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE LABELS * FROM user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE EDGE_TYPES * FROM user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON LABELS * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON EDGE_TYPES * TO user;")
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    source_destination_paths = common.execute_and_fetch_all(
        user_connection.cursor(),
        "MATCH path=(n:label0)-[* 1..3]->(m:label4) RETURN extract( node in nodes(path) | node.id);",
    )

    expected_paths = [[0, 1, 3, 5], [0, 2, 3, 5], [0, 2, 4, 5]]

    assert len(source_destination_paths) == 3
    assert all(path[0] in expected_paths for path in source_destination_paths)


@pytest.mark.parametrize("switch", [False, True])
def test_dfs_all_edge_types_all_labels_denied(switch):
    admin_connection = common.connect(username="admin", password="test")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE LABELS * FROM user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE EDGE_TYPES * FROM user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT NOTHING ON LABELS * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT NOTHING ON EDGE_TYPES * TO user;")
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    total_paths_results = common.execute_and_fetch_all(user_connection.cursor(), "MATCH p=(n)-[*]->(m) RETURN p;")

    assert len(total_paths_results) == 0


@pytest.mark.parametrize("switch", [False, True])
def test_dfs_denied_start(switch):
    admin_connection = common.connect(username="admin", password="test")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE LABELS * FROM user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE EDGE_TYPES * FROM user;")
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT READ ON LABELS :label1, :label2, :label3, :label4 TO user;"
    )
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON EDGE_TYPES * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT NOTHING ON LABELS :label0 TO user;")
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    source_destination_path = common.execute_and_fetch_all(
        user_connection.cursor(), "MATCH p=(n:label0)-[*]->(m:label4) RETURN p;"
    )

    assert len(source_destination_path) == 0


@pytest.mark.parametrize("switch", [False, True])
def test_dfs_denied_destination(switch):
    admin_connection = common.connect(username="admin", password="test")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE LABELS * FROM user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE EDGE_TYPES * FROM user;")
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT READ ON LABELS :label0, :label1, :label2, :label3 TO user;"
    )
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON EDGE_TYPES * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT NOTHING ON LABELS :label4 TO user;")
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    source_destination_path = common.execute_and_fetch_all(
        user_connection.cursor(), "MATCH p=(n:label0)-[*]->(m:label4) RETURN p;"
    )

    assert len(source_destination_path) == 0


@pytest.mark.parametrize("switch", [False, True])
def test_dfs_denied_label_1(switch):
    admin_connection = common.connect(username="admin", password="test")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE LABELS * FROM user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE EDGE_TYPES * FROM user;")
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT READ ON LABELS :label0, :label2, :label3, :label4 TO user;"
    )
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT NOTHING ON LABELS :label1 TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON EDGE_TYPES * TO user;")
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    source_destination_paths = common.execute_and_fetch_all(
        user_connection.cursor(),
        "MATCH p=(n:label0)-[* 1..3]->(m:label4) RETURN extract( node in nodes(p) | node.id);",
    )

    expected_paths = [[0, 2, 3, 5], [0, 2, 4, 5]]

    assert len(source_destination_paths) == 2
    assert all(path[0] in expected_paths for path in source_destination_paths)


@pytest.mark.parametrize("switch", [False, True])
def test_dfs_denied_edge_type_3(switch):
    admin_connection = common.connect(username="admin", password="test")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE LABELS * FROM user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE EDGE_TYPES * FROM user;")

    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON LABELS * TO user;")
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT READ ON EDGE_TYPES :edge_type_1, :edge_type_2, :edge_type_4 TO user;"
    )
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT NOTHING ON EDGE_TYPES :edge_type_3 TO user;")
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    source_destination_path = common.execute_and_fetch_all(
        user_connection.cursor(),
        "MATCH p=(n:label0)-[r * 1..3]->(m:label4) RETURN extract( node in nodes(p) | node.id);",
    )

    expected_path = [0, 2, 4, 5]

    assert len(source_destination_path) == 1
    assert source_destination_path[0][0] == expected_path


@pytest.mark.parametrize("switch", [False, True])
def test_bfs_sts_all_edge_types_all_labels_granted(switch):
    admin_connection = common.connect(username="admin", password="test")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE LABELS * FROM user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE EDGE_TYPES * FROM user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON LABELS * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON EDGE_TYPES * TO user;")
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    source_destination_path = common.execute_and_fetch_all(
        user_connection.cursor(),
        "MATCH (n), (m) WITH n, m MATCH p=(n:label0)-[r *BFS]->(m:label4) RETURN extract( node in nodes(p) | node.id);",
    )

    expected_path = [0, 1, 3, 5]

    assert len(source_destination_path) == 1
    assert source_destination_path[0][0] == expected_path


@pytest.mark.parametrize("switch", [False, True])
def test_bfs_sts_all_edge_types_all_labels_denied(switch):
    admin_connection = common.connect(username="admin", password="test")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE LABELS * FROM user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE EDGE_TYPES * FROM user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT NOTHING ON LABELS * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT NOTHING ON EDGE_TYPES * TO user;")
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    total_paths_results = common.execute_and_fetch_all(
        user_connection.cursor(), "MATCH (n), (m) WITH n, m MATCH p=(n)-[r *BFS]->(m) RETURN p;"
    )

    assert len(total_paths_results) == 0


@pytest.mark.parametrize("switch", [False, True])
def test_bfs_sts_denied_start(switch):
    admin_connection = common.connect(username="admin", password="test")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE LABELS * FROM user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE EDGE_TYPES * FROM user;")
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT READ ON LABELS :label1, :label2, :label3, :label4 TO user;"
    )
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON EDGE_TYPES * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT NOTHING ON LABELS :label0 TO user;")
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    source_destination_path = common.execute_and_fetch_all(
        user_connection.cursor(), "MATCH (n), (m) WITH n, m MATCH p=(n:label0)-[r *BFS]->(m:label4) RETURN p;"
    )

    assert len(source_destination_path) == 0


@pytest.mark.parametrize("switch", [False, True])
def test_bfs_sts_denied_destination(switch):
    admin_connection = common.connect(username="admin", password="test")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE LABELS * FROM user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE EDGE_TYPES * FROM user;")
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT READ ON LABELS :label0, :label1, :label2, :label3 TO user;"
    )
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON EDGE_TYPES * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT NOTHING ON LABELS :label4 TO user;")
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    source_destination_path = common.execute_and_fetch_all(
        user_connection.cursor(), "MATCH (n), (m) WITH n, m MATCH p=(n:label0)-[r *BFS]->(m:label4) RETURN p;"
    )

    assert len(source_destination_path) == 0


@pytest.mark.parametrize("switch", [False, True])
def test_bfs_sts_denied_label_1(switch):
    admin_connection = common.connect(username="admin", password="test")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE LABELS * FROM user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE EDGE_TYPES * FROM user;")
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT READ ON LABELS :label0, :label2, :label3, :label4 TO user;"
    )
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT NOTHING ON LABELS :label1 TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON EDGE_TYPES * TO user;")
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    source_destination_path = common.execute_and_fetch_all(
        user_connection.cursor(),
        "MATCH (n), (m) WITH n, m MATCH p=(n:label0)-[r *BFS]->(m:label4) RETURN extract( node in nodes(p) | node.id);",
    )
    expected_path = [0, 2, 4, 5]

    assert len(source_destination_path) == 1
    assert source_destination_path[0][0] == expected_path


@pytest.mark.parametrize("switch", [False, True])
def test_bfs_sts_denied_edge_type_3(switch):
    admin_connection = common.connect(username="admin", password="test")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE LABELS * FROM user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE EDGE_TYPES * FROM user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON LABELS * TO user;")
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT READ ON EDGE_TYPES :edge_type_1, :edge_type_2, :edge_type_4 TO user;"
    )
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT NOTHING ON EDGE_TYPES :edge_type_3 TO user;")
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    source_destination_path = common.execute_and_fetch_all(
        user_connection.cursor(),
        "MATCH (n), (m) WITH n, m MATCH p=(n:label0)-[r *BFS]->(m:label4) RETURN extract( node in nodes(p) | node.id);",
    )
    expected_path = [0, 2, 4, 5]

    assert len(source_destination_path) == 1
    assert source_destination_path[0][0] == expected_path


@pytest.mark.parametrize("switch", [False, True])
def test_bfs_single_source_all_edge_types_all_labels_granted(switch):
    admin_connection = common.connect(username="admin", password="test")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE LABELS * FROM user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE EDGE_TYPES * FROM user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON LABELS * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON EDGE_TYPES * TO user;")
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    source_destination_path = common.execute_and_fetch_all(
        user_connection.cursor(),
        "MATCH p=(n:label0)-[r *BFS]->(m:label4) RETURN extract( node in nodes(p) | node.id);",
    )

    expected_path = [0, 2, 3, 5]

    assert len(source_destination_path) == 1
    assert source_destination_path[0][0] == expected_path


@pytest.mark.parametrize("switch", [False, True])
def test_bfs_single_source_all_edge_types_all_labels_denied(switch):
    admin_connection = common.connect(username="admin", password="test")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE LABELS * FROM user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE EDGE_TYPES * FROM user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT NOTHING ON LABELS * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT NOTHING ON EDGE_TYPES * TO user;")
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    total_paths_results = common.execute_and_fetch_all(user_connection.cursor(), "MATCH p=(n)-[r *BFS]->(m) RETURN p;")

    assert len(total_paths_results) == 0


@pytest.mark.parametrize("switch", [False, True])
def test_bfs_single_source_denied_start(switch):
    admin_connection = common.connect(username="admin", password="test")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE LABELS * FROM user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE EDGE_TYPES * FROM user;")
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT READ ON LABELS :label1, :label2, :label3, :label4 TO user;"
    )
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON EDGE_TYPES * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT NOTHING ON LABELS :label0 TO user;")
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    source_destination_path = common.execute_and_fetch_all(
        user_connection.cursor(), "MATCH p=(n:label0)-[r *BFS]->(m:label4) RETURN p;"
    )

    assert len(source_destination_path) == 0


@pytest.mark.parametrize("switch", [False, True])
def test_bfs_single_source_denied_destination(switch):
    admin_connection = common.connect(username="admin", password="test")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE LABELS * FROM user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE EDGE_TYPES * FROM user;")
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT READ ON LABELS :label0, :label1, :label2, :label3 TO user;"
    )
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON EDGE_TYPES * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT NOTHING ON LABELS :label4 TO user;")
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    source_destination_path = common.execute_and_fetch_all(
        user_connection.cursor(), "MATCH p=(n:label0)-[r *BFS]->(m:label4) RETURN p;"
    )

    assert len(source_destination_path) == 0


@pytest.mark.parametrize("switch", [False, True])
def test_bfs_single_source_denied_label_1(switch):
    admin_connection = common.connect(username="admin", password="test")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE LABELS * FROM user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE EDGE_TYPES * FROM user;")
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT READ ON LABELS :label0, :label2, :label3, :label4 TO user;"
    )
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT NOTHING ON LABELS :label1 TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON EDGE_TYPES * TO user;")
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    source_destination_path = common.execute_and_fetch_all(
        user_connection.cursor(),
        "MATCH p=(n:label0)-[r *BFS]->(m:label4) RETURN extract( node in nodes(p) | node.id);",
    )

    expected_path = [0, 2, 3, 5]

    assert len(source_destination_path) == 1
    assert source_destination_path[0][0] == expected_path


@pytest.mark.parametrize("switch", [False, True])
def test_bfs_single_source_denied_edge_type_3(switch):
    admin_connection = common.connect(username="admin", password="test")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE LABELS * FROM user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE EDGE_TYPES * FROM user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON LABELS * TO user;")
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT READ ON EDGE_TYPES :edge_type_1, :edge_type_2, :edge_type_4 TO user;"
    )
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT NOTHING ON EDGE_TYPES :edge_type_3 TO user;")
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    source_destination_path = common.execute_and_fetch_all(
        user_connection.cursor(),
        "MATCH p=(n:label0)-[r *BFS]->(m:label4) RETURN extract( node in nodes(p) | node.id);",
    )

    expected_path = [0, 2, 4, 5]

    assert len(source_destination_path) == 1
    assert source_destination_path[0][0] == expected_path


@pytest.mark.parametrize("switch", [False, True])
def test_all_shortest_paths_when_all_edge_types_all_labels_granted(switch):
    admin_connection = common.connect(username="admin", password="test")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE LABELS * FROM user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE EDGE_TYPES * FROM user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON LABELS * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON EDGE_TYPES * TO user;")
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    total_paths_results = common.execute_and_fetch_all(
        user_connection.cursor(),
        "MATCH p=(n)-[r *allShortest (r, n | r.weight)]->(m) RETURN extract( node in nodes(p) | node.id);",
    )
    path_result = common.execute_and_fetch_all(
        user_connection.cursor(),
        "MATCH p=(n:label0)-[r *allShortest (r, n | r.weight) path_length]->(m:label4) RETURN path_length,nodes(p);",
    )

    expected_path = [0, 1, 3, 4, 5]
    expected_all_paths = [
        [0, 1],
        [0, 1, 2],
        [0, 1, 3],
        [0, 1, 3, 4],
        [0, 1, 3, 4, 5],
        [1, 2],
        [1, 3],
        [1, 3, 4],
        [1, 3, 4, 5],
        [2, 1],
        [2, 3],
        [2, 3, 4],
        [2, 3, 4, 5],
        [3, 4],
        [3, 4, 5],
        [4, 3],
        [4, 5],
    ]

    assert len(total_paths_results) == 16
    assert all(path[0] in expected_all_paths for path in total_paths_results)
    assert path_result[0][0] == 20
    assert all(node.id in expected_path for node in path_result[0][1])


@pytest.mark.parametrize("switch", [False, True])
def test_all_shortest_paths_when_all_edge_types_all_labels_denied(switch):
    admin_connection = common.connect(username="admin", password="test")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE LABELS * FROM user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE EDGE_TYPES * FROM user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT NOTHING ON LABELS * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT NOTHING ON EDGE_TYPES * TO user;")
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    results = common.execute_and_fetch_all(
        user_connection.cursor(), "MATCH p=(n)-[r *allShortest (r, n | r.weight)]->(m) RETURN p;"
    )

    assert len(results) == 0


@pytest.mark.parametrize("switch", [False, True])
def test_all_shortest_paths_when_denied_start(switch):
    admin_connection = common.connect(username="admin", password="test")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE LABELS * FROM user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE EDGE_TYPES * FROM user;")
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT READ ON LABELS :label1, :label2, :label3, :label4 TO user;"
    )
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON EDGE_TYPES * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT NOTHING ON LABELS :label0 TO user;")
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    path_length_result = common.execute_and_fetch_all(
        user_connection.cursor(),
        "MATCH p=(n:label0)-[r *allShortest (r, n | r.weight) path_length]->(m:label4) RETURN path_length;",
    )

    assert len(path_length_result) == 0


@pytest.mark.parametrize("switch", [False, True])
def test_all_shortest_paths_when_denied_destination(switch):
    admin_connection = common.connect(username="admin", password="test")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE LABELS * FROM user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE EDGE_TYPES * FROM user;")
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT READ ON LABELS :label0, :label1, :label2, :label3 TO user;"
    )
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON EDGE_TYPES * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT NOTHING ON LABELS :label4 TO user;")
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    path_length_result = common.execute_and_fetch_all(
        user_connection.cursor(),
        "MATCH p=(n:label0)-[r *allShortest (r, n | r.weight) path_length]->(m:label4) RETURN path_length;",
    )

    assert len(path_length_result) == 0


@pytest.mark.parametrize("switch", [False, True])
def test_all_shortest_paths_when_denied_label_1(switch):
    admin_connection = common.connect(username="admin", password="test")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE LABELS * FROM user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE EDGE_TYPES * FROM user;")
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT READ ON LABELS :label0, :label2, :label3, :label4 TO user;"
    )
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT NOTHING ON LABELS :label1 TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON EDGE_TYPES * TO user;")
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    total_paths_results = common.execute_and_fetch_all(
        user_connection.cursor(),
        "MATCH p=(n)-[r *allShortest (r, n | r.weight)]->(m) RETURN extract( node in nodes(p) | node.id);",
    )

    path_result = common.execute_and_fetch_all(
        user_connection.cursor(),
        "MATCH p=(n:label0)-[r *allShortest (r, n | r.weight) path_length]->(m:label4) RETURN path_length, nodes(p);",
    )

    expected_path = [0, 2, 3, 4, 5]

    expected_all_paths = [
        [0, 2],
        [0, 2, 3],
        [0, 2, 3, 4],
        [0, 2, 3, 4, 5],
        [2, 3],
        [2, 3, 4],
        [2, 3, 4, 5],
        [3, 4],
        [3, 4, 5],
        [4, 3],
        [4, 5],
    ]

    assert len(total_paths_results) == 11
    assert all(path[0] in expected_all_paths for path in total_paths_results)
    assert path_result[0][0] == 30
    assert all(node.id in expected_path for node in path_result[0][1])


@pytest.mark.parametrize("switch", [False, True])
def test_all_shortest_paths_when_denied_edge_type_3(switch):
    admin_connection = common.connect(username="admin", password="test")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE LABELS * FROM user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "REVOKE EDGE_TYPES * FROM user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON LABELS * TO user;")
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT READ ON EDGE_TYPES :edge_type_1, :edge_type_2, :edge_type_4 TO user;"
    )
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT NOTHING ON EDGE_TYPES :edge_type_3 TO user;")
    user_connection = common.connect(username="user", password="test")

    if switch:
        common.switch_db(user_connection.cursor())
    path_result = common.execute_and_fetch_all(
        user_connection.cursor(),
        "MATCH p=(n:label0)-[r *allShortest (r, n | r.weight) path_length]->(m:label4) RETURN path_length, nodes(p);",
    )

    total_paths_results = common.execute_and_fetch_all(
        user_connection.cursor(),
        "MATCH p=(n)-[r *allShortest (r, n | r.weight)]->(m) RETURN extract( node in nodes(p) | node.id);",
    )

    expected_path = [0, 1, 2, 3, 5]
    expected_all_paths = [
        [0, 1],
        [0, 1, 2],
        [0, 1, 2, 4],
        [0, 1, 2, 4, 3],
        [0, 1, 2, 4, 5],
        [1, 2, 4, 3],
        [1, 2],
        [1, 2, 4],
        [1, 2, 4, 5],
        [2, 1],
        [2, 4, 3],
        [2, 4],
        [2, 4, 5],
        [3, 4],
        [3, 4, 5],
        [4, 3],
        [4, 5],
    ]

    assert len(total_paths_results) == 16
    assert all(path[0] in expected_all_paths for path in total_paths_results)
    assert path_result[0][0] == 25
    assert all(node.id in expected_path for node in path_result[0][1])


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
