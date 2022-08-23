import common
import sys
import pytest


def test_weighted_shortest_path_all_edge_types_all_labels_granted():
    admin_connection = common.connect(username="admin", password="test")
    user_connnection = common.connect(username="user", password="test")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON READ ON LABELS * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON READ ON EDGE_TYPES * TO user;")

    total_paths_results = common.execute_and_fetch_all(
        user_connnection.cursor(), "MATCH p=(n)-[r *wShortest (r, n | r.weight)]->(m) RETURN p;"
    )
    path_length_result = common.execute_and_fetch_all(
        user_connnection.cursor(),
        "MATCH p=(n:label0)-[r *wShortest (r, n | r.weight) path_length]->(m:label4) RETURN path_length;",
    )

    assert len(total_paths_results) == 16
    assert path_length_result[0][0] == 20


def test_weighted_shortest_path_all_edge_types_all_labels_denied():
    admin_connection = common.connect(username="admin", password="test")
    user_connnection = common.connect(username="user", password="test")
    common.execute_and_fetch_all(admin_connection.cursor(), "DENY READ ON LABELS * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "DENY READ ON EDGE_TYPES * TO user;")

    results = common.execute_and_fetch_all(
        user_connnection.cursor(), "MATCH p=(n)-[r *wShortest (r, n | r.weight)]->(m) RETURN p;"
    )

    assert len(results) == 0


def test_weighted_shortest_path_denied_start():
    admin_connection = common.connect(username="admin", password="test")
    user_connnection = common.connect(username="user", password="test")
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT READ ON LABELS :label1, :label2, :label3, :label4 TO user;"
    )
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON EDGE_TYPES * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "DENY READ ON LABELS :label0 TO user;")

    path_length_result = common.execute_and_fetch_all(
        user_connnection.cursor(),
        "MATCH p=(n:label0)-[r *wShortest (r, n | r.weight) path_length]->(m:label4) RETURN path_length;",
    )

    assert len(path_length_result) == 0


def test_weighted_shortest_path_denied_destination():
    admin_connection = common.connect(username="admin", password="test")
    user_connnection = common.connect(username="user", password="test")
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT READ ON LABELS :label0, :label1, :label2, :label3 TO user;"
    )
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON EDGE_TYPES * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "DENY READ ON LABELS :label4 TO user;")

    path_length_result = common.execute_and_fetch_all(
        user_connnection.cursor(),
        "MATCH p=(n:label0)-[r *wShortest (r, n | r.weight) path_length]->(m:label4) RETURN path_length;",
    )

    assert len(path_length_result) == 0


def test_weighted_shortest_path_denied_label_1():
    admin_connection = common.connect(username="admin", password="test")
    user_connnection = common.connect(username="user", password="test")
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT READ ON LABELS :label0, :label2, :label3, :label4 TO user;"
    )
    common.execute_and_fetch_all(admin_connection.cursor(), "DENY READ ON READ ON LABELS :label1 TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON EDGE_TYPES * TO user;")

    total_paths_results = common.execute_and_fetch_all(
        user_connnection.cursor(), "MATCH p=(n)-[r *wShortest (r, n | r.weight)]->(m) RETURN p;"
    )
    path_length_result = common.execute_and_fetch_all(
        user_connnection.cursor(),
        "MATCH p=(n:label0)-[r *wShortest (r, n | r.weight) path_length]->(m:label4) RETURN path_length;",
    )

    assert len(total_paths_results) == 11
    assert path_length_result[0][0] == 30


def test_weighted_shortest_path_denied_edge_type_3():
    admin_connection = common.connect(username="admin", password="test")
    user_connnection = common.connect(username="user", password="test")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON LABELS * TO user;")
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT READ ON EDGE_TYPES :edge_type_1, :edge_type_2, :edge_type_4 TO user;"
    )
    common.execute_and_fetch_all(admin_connection.cursor(), "DENY READ ON EDGE_TYPES :edge_type_3 TO user;")

    total_paths_results = common.execute_and_fetch_all(
        user_connnection.cursor(), "MATCH p=(n)-[r *wShortest (r, n | r.weight)]->(m) RETURN p;"
    )
    path_length_result = common.execute_and_fetch_all(
        user_connnection.cursor(),
        "MATCH p=(n:label0)-[r *wShortest (r, n | r.weight) path_length]->(m:label4) RETURN path_length;",
    )

    assert len(total_paths_results) == 16
    assert path_length_result[0][0] == 25


def test_dfs_all_edge_types_all_labels_granted():
    admin_connection = common.connect(username="admin", password="test")
    user_connnection = common.connect(username="user", password="test")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON LABELS * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON EDGE_TYPES * TO user;")

    source_destination_path = common.execute_and_fetch_all(
        user_connnection.cursor(), "MATCH p=(n:label0)-[*]->(m:label4) RETURN p;"
    )

    assert len(source_destination_path) == 15


def test_dfs_all_edge_types_all_labels_denied():
    admin_connection = common.connect(username="admin", password="test")
    user_connnection = common.connect(username="user", password="test")
    common.execute_and_fetch_all(admin_connection.cursor(), "DENY READ ON LABELS * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "DENY READ ON EDGE_TYPES * TO user;")

    total_paths_results = common.execute_and_fetch_all(user_connnection.cursor(), "MATCH p=(n)-[*]->(m) RETURN p;")

    assert len(total_paths_results) == 0


def test_dfs_denied_start():
    admin_connection = common.connect(username="admin", password="test")
    user_connnection = common.connect(username="user", password="test")
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT READ ON LABELS :label1, :label2, :label3, :label4 TO user;"
    )
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON EDGE_TYPES * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "DENY READ ON LABELS :label0 TO user;")

    source_destination_path = common.execute_and_fetch_all(
        user_connnection.cursor(), "MATCH p=(n:label0)-[*]->(m:label4) RETURN p;"
    )

    assert len(source_destination_path) == 0


def test_dfs_denied_destination():
    admin_connection = common.connect(username="admin", password="test")
    user_connnection = common.connect(username="user", password="test")
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT READ ON LABELS :label0, :label1, :label2, :label3 TO user;"
    )
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON EDGE_TYPES * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "DENY READ ON LABELS :label4 TO user;")

    source_destination_path = common.execute_and_fetch_all(
        user_connnection.cursor(), "MATCH p=(n:label0)-[*]->(m:label4) RETURN p;"
    )

    assert len(source_destination_path) == 0


def test_dfs_denied_label_1():
    admin_connection = common.connect(username="admin", password="test")
    user_connnection = common.connect(username="user", password="test")
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT READ ON LABELS :label0, :label2, :label3, :label4 TO user;"
    )
    common.execute_and_fetch_all(admin_connection.cursor(), "DENY READ ON READ ON LABELS :label1 TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON EDGE_TYPES * TO user;")
    source_destination_paths = common.execute_and_fetch_all(
        user_connnection.cursor(), "MATCH p=(n:label0)-[*]->(m:label4) RETURN nodes(p);"
    )

    assert len(source_destination_paths) == 6
    assert not any("label1" in node.labels for path in source_destination_paths for node in path[0])


def test_dfs_denied_edge_type_3():
    admin_connection = common.connect(username="admin", password="test")
    user_connnection = common.connect(username="user", password="test")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON LABELS * TO user;")
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT READ ON EDGE_TYPES :edge_type_1, :edge_type_2, :edge_type_4 TO user;"
    )
    common.execute_and_fetch_all(admin_connection.cursor(), "DENY READ ON EDGE_TYPES :edge_type_3 TO user;")

    source_destination_paths = common.execute_and_fetch_all(
        user_connnection.cursor(), "MATCH p=(n:label0)-[r *]->(m:label4) RETURN r;"
    )

    assert len(source_destination_paths) == 6
    assert not any("edge_type_3" == edge.type for path in source_destination_paths for edge in path[0])


def test_bfs_sts_all_edge_types_all_labels_granted():
    admin_connection = common.connect(username="admin", password="test")
    user_connnection = common.connect(username="user", password="test")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON LABELS * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON EDGE_TYPES * TO user;")

    source_destination_path = common.execute_and_fetch_all(
        user_connnection.cursor(), "MATCH (n), (m) WITH n, m MATCH p=(n:label0)-[r *BFS]->(m:label4) RETURN p;"
    )

    assert len(source_destination_path) == 1


def test_bfs_sts_all_edge_types_all_labels_denied():
    admin_connection = common.connect(username="admin", password="test")
    user_connnection = common.connect(username="user", password="test")
    common.execute_and_fetch_all(admin_connection.cursor(), "DENY READ ON LABELS * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "DENY READ ON EDGE_TYPES * TO user;")

    total_paths_results = common.execute_and_fetch_all(
        user_connnection.cursor(), "MATCH (n), (m) WITH n, m MATCH p=(n)-[r *BFS]->(m) RETURN p;"
    )

    assert len(total_paths_results) == 0


def test_bfs_sts_denied_start():
    admin_connection = common.connect(username="admin", password="test")
    user_connnection = common.connect(username="user", password="test")
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT READ ON LABELS :label1, :label2, :label3, :label4 TO user;"
    )
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON EDGE_TYPES * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "DENY READ ON LABELS :label0 TO user;")

    source_destination_path = common.execute_and_fetch_all(
        user_connnection.cursor(), "MATCH (n), (m) WITH n, m MATCH p=(n:label0)-[r *BFS]->(m:label4) RETURN p;"
    )

    assert len(source_destination_path) == 0


def test_bfs_sts_denied_destination():
    admin_connection = common.connect(username="admin", password="test")
    user_connnection = common.connect(username="user", password="test")
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT READ ON LABELS :label0, :label1, :label2, :label3 TO user;"
    )
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON EDGE_TYPES * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "DENY READ ON LABELS :label4 TO user;")

    source_destination_path = common.execute_and_fetch_all(
        user_connnection.cursor(), "MATCH (n), (m) WITH n, m MATCH p=(n:label0)-[r *BFS]->(m:label4) RETURN p;"
    )

    assert len(source_destination_path) == 0


def test_bfs_sts_denied_label_1():
    admin_connection = common.connect(username="admin", password="test")
    user_connnection = common.connect(username="user", password="test")
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT READ ON LABELS :label0, :label2, :label3, :label4 TO user;"
    )
    common.execute_and_fetch_all(admin_connection.cursor(), "DENY READ ON READ ON LABELS :label1 TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON EDGE_TYPES * TO user;")
    source_destination_path = common.execute_and_fetch_all(
        user_connnection.cursor(), "MATCH (n), (m) WITH n, m MATCH p=(n:label0)-[r *BFS]->(m:label4) RETURN nodes(p);"
    )

    assert len(source_destination_path) == 1
    assert not any("label1" in node.labels for node in source_destination_path[0][0])


def test_bfs_sts_denied_edge_type_3():
    admin_connection = common.connect(username="admin", password="test")
    user_connnection = common.connect(username="user", password="test")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON LABELS * TO user;")
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT READ ON EDGE_TYPES :edge_type_1, :edge_type_2, :edge_type_4 TO user;"
    )
    common.execute_and_fetch_all(admin_connection.cursor(), "DENY READ ON EDGE_TYPES :edge_type_3 TO user;")

    source_destination_path = common.execute_and_fetch_all(
        user_connnection.cursor(), "MATCH (n), (m) WITH n, m MATCH p=(n:label0)-[r *BFS]->(m:label4) RETURN r;"
    )

    assert len(source_destination_path) == 1
    assert not any("edge_type_3" == edge.type for edge in source_destination_path[0][0])


def test_bfs_singe_source_all_edge_types_all_labels_granted():
    admin_connection = common.connect(username="admin", password="test")
    user_connnection = common.connect(username="user", password="test")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON LABELS * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON EDGE_TYPES * TO user;")

    source_destination_path = common.execute_and_fetch_all(
        user_connnection.cursor(), "MATCH p=(n:label0)-[r *BFS]->(m:label4) RETURN p;"
    )

    assert len(source_destination_path) == 1


def test_bfs_singe_source_all_edge_types_all_labels_denied():
    admin_connection = common.connect(username="admin", password="test")
    user_connnection = common.connect(username="user", password="test")
    common.execute_and_fetch_all(admin_connection.cursor(), "DENY READ ON LABELS * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "DENY READ ON EDGE_TYPES * TO user;")

    total_paths_results = common.execute_and_fetch_all(user_connnection.cursor(), "MATCH p=(n)-[r *BFS]->(m) RETURN p;")

    assert len(total_paths_results) == 0


def test_bfs_singe_source_denied_start():
    admin_connection = common.connect(username="admin", password="test")
    user_connnection = common.connect(username="user", password="test")
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT READ ON LABELS :label1, :label2, :label3, :label4 TO user;"
    )
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON EDGE_TYPES * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "DENY READ ON LABELS :label0 TO user;")

    source_destination_path = common.execute_and_fetch_all(
        user_connnection.cursor(), "MATCH p=(n:label0)-[r *BFS]->(m:label4) RETURN p;"
    )

    assert len(source_destination_path) == 0


def test_bfs_singe_source_denied_destination():
    admin_connection = common.connect(username="admin", password="test")
    user_connnection = common.connect(username="user", password="test")
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT READ ON LABELS :label0, :label1, :label2, :label3 TO user;"
    )
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON EDGE_TYPES * TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "DENY READ ON LABELS :label4 TO user;")

    source_destination_path = common.execute_and_fetch_all(
        user_connnection.cursor(), "MATCH p=(n:label0)-[r *BFS]->(m:label4) RETURN p;"
    )

    assert len(source_destination_path) == 0


def test_bfs_singe_source_denied_label_1():
    admin_connection = common.connect(username="admin", password="test")
    user_connnection = common.connect(username="user", password="test")
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT READ ON LABELS :label0, :label2, :label3, :label4 TO user;"
    )
    common.execute_and_fetch_all(admin_connection.cursor(), "DENY READ ON READ ON LABELS :label1 TO user;")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON EDGE_TYPES * TO user;")
    source_destination_path = common.execute_and_fetch_all(
        user_connnection.cursor(), "MATCH p=(n:label0)-[r *BFS]->(m:label4) RETURN nodes(p);"
    )

    assert len(source_destination_path) == 1
    assert not any("label1" in node.labels for node in source_destination_path[0][0])


def test_bfs_singe_source_denied_edge_type_3():
    admin_connection = common.connect(username="admin", password="test")
    user_connnection = common.connect(username="user", password="test")
    common.execute_and_fetch_all(admin_connection.cursor(), "GRANT READ ON LABELS * TO user;")
    common.execute_and_fetch_all(
        admin_connection.cursor(), "GRANT READ ON EDGE_TYPES :edge_type_1, :edge_type_2, :edge_type_4 TO user;"
    )
    common.execute_and_fetch_all(admin_connection.cursor(), "DENY READ ON EDGE_TYPES :edge_type_3 TO user;")

    source_destination_path = common.execute_and_fetch_all(
        user_connnection.cursor(), "MATCH p=(n:label0)-[r *BFS]->(m:label4) RETURN r;"
    )

    assert len(source_destination_path) == 1
    assert not any("edge_type_3" == edge.type for edge in source_destination_path[0][0])


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
