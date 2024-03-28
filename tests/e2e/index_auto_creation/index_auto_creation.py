# Copyright 2024 Memgraph Ltd.
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
from common import cursor, execute_and_fetch_all


# Helper functions
def get_index_stats(cursor):
    return execute_and_fetch_all(cursor, "SHOW INDEX INFO")


def index_exists(indices, index_name):
    for index in indices:
        if index[1] == index_name:
            return True

    return False


def index_count_is(indices, index_name, count):
    return count == index_count(indices, index_name)


def number_of_index_structures_are(indices, predicted_size):
    return len(indices) == predicted_size


def index_count(indices, index_name):
    for index in indices:
        if index[1] == index_name:
            return index[3]

    return 0


#####################
# Label index tests #
#####################


def test_auto_create_single_label_index(cursor):
    label = "SOMELABEL"
    execute_and_fetch_all(cursor, f"CREATE (n:{label})")
    index_stats = get_index_stats(cursor)
    assert number_of_index_structures_are(index_stats, 1)
    assert index_exists(index_stats, label)
    assert index_count_is(index_stats, label, 1)

    execute_and_fetch_all(cursor, f"DROP INDEX ON :{label}")
    index_stats = get_index_stats(cursor)
    assert number_of_index_structures_are(index_stats, 0)

    execute_and_fetch_all(cursor, f"MATCH (n) DETACH DELETE n")


def test_auto_create_several_different_label_index(cursor):
    label1 = "SOMELABEL1"
    label2 = "SOMELABEL2"
    label3 = "SOMELABEL3"
    execute_and_fetch_all(cursor, f"CREATE (n:{label1}:{label2}:{label3})")
    index_stats = get_index_stats(cursor)
    assert number_of_index_structures_are(index_stats, 3)

    execute_and_fetch_all(cursor, f"DROP INDEX ON :{label1}")
    execute_and_fetch_all(cursor, f"DROP INDEX ON :{label2}")
    execute_and_fetch_all(cursor, f"DROP INDEX ON :{label3}")
    index_stats = get_index_stats(cursor)
    assert number_of_index_structures_are(index_stats, 0)

    execute_and_fetch_all(cursor, f"MATCH (n) DETACH DELETE n")


def test_auto_create_multiple_label_index(cursor):
    label1 = "SOMELABEL1"
    execute_and_fetch_all(cursor, f"CREATE (n:{label1})")

    label2 = "SOMELABEL2"
    execute_and_fetch_all(cursor, f"CREATE (n:{label2})")

    label3 = "SOMELABEL3"
    execute_and_fetch_all(cursor, f"CREATE (n:{label3})")

    index_stats = get_index_stats(cursor)

    assert number_of_index_structures_are(index_stats, 3)
    assert index_exists(index_stats, label1)
    assert index_exists(index_stats, label2)
    assert index_exists(index_stats, label3)
    assert index_count_is(index_stats, label1, 1)
    assert index_count_is(index_stats, label2, 1)
    assert index_count_is(index_stats, label3, 1)

    execute_and_fetch_all(cursor, f"DROP INDEX ON :{label1}")
    index_stats = get_index_stats(cursor)
    assert number_of_index_structures_are(index_stats, 2)

    execute_and_fetch_all(cursor, f"DROP INDEX ON :{label2}")
    index_stats = get_index_stats(cursor)
    assert number_of_index_structures_are(index_stats, 1)

    execute_and_fetch_all(cursor, f"DROP INDEX ON :{label3}")
    index_stats = get_index_stats(cursor)
    assert number_of_index_structures_are(index_stats, 0)

    execute_and_fetch_all(cursor, f"MATCH (n) DETACH DELETE n")


def test_auto_create_single_label_index_with_multiple_entries(cursor):
    label = "SOMELABEL"
    for _ in range(100):
        execute_and_fetch_all(cursor, f"CREATE (n:{label})")
    index_stats = get_index_stats(cursor)

    print(len(index_stats[0]))

    assert number_of_index_structures_are(index_stats, 1)
    assert index_exists(index_stats, label)
    assert index_count_is(index_stats, label, 100)

    execute_and_fetch_all(cursor, f"DROP INDEX ON :{label}")
    index_stats = get_index_stats(cursor)
    assert number_of_index_structures_are(index_stats, 0)

    execute_and_fetch_all(cursor, f"MATCH (n) DETACH DELETE n")


def test_auto_create_multiple_label_index_with_multiple_entries(cursor):
    label1 = "SOMELABEL1"
    for _ in range(120):
        execute_and_fetch_all(cursor, f"CREATE (n:{label1})")

    label2 = "SOMELABEL2"
    for _ in range(100):
        execute_and_fetch_all(cursor, f"CREATE (n:{label2})")

    label3 = "SOMELABEL3"
    for _ in range(80):
        execute_and_fetch_all(cursor, f"CREATE (n:{label3})")

    index_stats = get_index_stats(cursor)

    print(index_stats)

    assert number_of_index_structures_are(index_stats, 3)
    assert index_exists(index_stats, label1)
    assert index_exists(index_stats, label2)
    assert index_exists(index_stats, label3)
    assert index_count_is(index_stats, label1, 120)
    assert index_count_is(index_stats, label2, 100)
    assert index_count_is(index_stats, label3, 80)

    execute_and_fetch_all(cursor, f"DROP INDEX ON :{label1}")
    index_stats = get_index_stats(cursor)
    assert number_of_index_structures_are(index_stats, 2)

    execute_and_fetch_all(cursor, f"DROP INDEX ON :{label2}")
    index_stats = get_index_stats(cursor)
    assert number_of_index_structures_are(index_stats, 1)

    execute_and_fetch_all(cursor, f"DROP INDEX ON :{label3}")
    index_stats = get_index_stats(cursor)
    assert number_of_index_structures_are(index_stats, 0)

    execute_and_fetch_all(cursor, f"MATCH (n) DETACH DELETE n")


#########################
# Edge-type index tests #
#########################


def test_auto_create_single_edge_type_index(cursor):
    label_from = "LABEL_FROM"
    label_to = "LABEL_TO"
    edge_type = "SOMEEDGETYPE"
    execute_and_fetch_all(cursor, f"CREATE (:{label_from})-[:{edge_type}]->(:{label_to})")

    index_stats = get_index_stats(cursor)
    assert index_exists(index_stats, label_from)
    assert index_exists(index_stats, label_to)
    assert index_exists(index_stats, edge_type)
    assert index_count_is(index_stats, label_from, 1)
    assert index_count_is(index_stats, label_to, 1)
    assert index_count_is(index_stats, edge_type, 1)
    assert number_of_index_structures_are(index_stats, 3)  # 2 label + 1 edge-type

    execute_and_fetch_all(cursor, f"DROP INDEX ON :{label_from}")
    execute_and_fetch_all(cursor, f"DROP INDEX ON :{label_to}")
    execute_and_fetch_all(cursor, f"DROP EDGE INDEX ON :{edge_type}")
    index_stats = get_index_stats(cursor)
    assert number_of_index_structures_are(index_stats, 0)

    execute_and_fetch_all(cursor, f"MATCH (n) DETACH DELETE n")


def test_auto_create_multiple_edge_type_index(cursor):
    label_from = "LABEL_FROM"
    label_to = "LABEL_TO"
    edge_type1 = "SOMEEDGETYPE1"
    edge_type2 = "SOMEEDGETYPE2"
    edge_type3 = "SOMEEDGETYPE3"

    execute_and_fetch_all(cursor, f"CREATE (n:{label_from})")
    execute_and_fetch_all(cursor, f"CREATE (n:{label_to})")
    execute_and_fetch_all(cursor, f"MATCH (n:{label_from}), (m:{label_to}) CREATE (n)-[:{edge_type1}]->(m)")
    execute_and_fetch_all(cursor, f"MATCH (n:{label_from}), (m:{label_to}) CREATE (n)-[:{edge_type2}]->(m)")
    execute_and_fetch_all(cursor, f"MATCH (n:{label_from}), (m:{label_to}) CREATE (n)-[:{edge_type3}]->(m)")

    index_stats = get_index_stats(cursor)

    assert number_of_index_structures_are(index_stats, 5)  # 2 label + 3 edge-type
    assert index_exists(index_stats, label_from)
    assert index_exists(index_stats, label_to)
    assert index_exists(index_stats, edge_type1)
    assert index_exists(index_stats, edge_type2)
    assert index_exists(index_stats, edge_type3)
    assert index_count_is(index_stats, edge_type1, 1)
    assert index_count_is(index_stats, edge_type2, 1)
    assert index_count_is(index_stats, edge_type3, 1)

    execute_and_fetch_all(cursor, f"DROP INDEX ON :{label_from}")
    index_stats = get_index_stats(cursor)
    assert number_of_index_structures_are(index_stats, 4)

    execute_and_fetch_all(cursor, f"DROP INDEX ON :{label_to}")
    index_stats = get_index_stats(cursor)
    assert number_of_index_structures_are(index_stats, 3)

    execute_and_fetch_all(cursor, f"DROP EDGE INDEX ON :{edge_type1}")
    index_stats = get_index_stats(cursor)
    assert number_of_index_structures_are(index_stats, 2)

    execute_and_fetch_all(cursor, f"DROP EDGE INDEX ON :{edge_type2}")
    index_stats = get_index_stats(cursor)
    assert number_of_index_structures_are(index_stats, 1)

    execute_and_fetch_all(cursor, f"DROP EDGE INDEX ON :{edge_type3}")
    index_stats = get_index_stats(cursor)
    assert number_of_index_structures_are(index_stats, 0)

    execute_and_fetch_all(cursor, f"MATCH (n) DETACH DELETE n")


def test_auto_create_single_edge_type_index_with_multiple_entries(cursor):
    label_from = "LABEL_FROM"
    label_to = "LABEL_TO"
    edge_type = "SOMEEDGETYPE"
    execute_and_fetch_all(cursor, f"CREATE (n:{label_from})")
    execute_and_fetch_all(cursor, f"CREATE (n:{label_to})")
    for _ in range(100):
        execute_and_fetch_all(cursor, f"MATCH (n:{label_from}), (m:{label_to}) CREATE (n)-[:{edge_type}]->(m)")

    index_stats = get_index_stats(cursor)
    assert index_exists(index_stats, label_from)
    assert index_exists(index_stats, label_to)
    assert index_exists(index_stats, edge_type)
    assert number_of_index_structures_are(index_stats, 3)  # 2 label + 1 edge-type

    assert index_count_is(index_stats, edge_type, 100)

    execute_and_fetch_all(cursor, f"DROP INDEX ON :{label_from}")
    execute_and_fetch_all(cursor, f"DROP INDEX ON :{label_to}")
    execute_and_fetch_all(cursor, f"DROP EDGE INDEX ON :{edge_type}")
    index_stats = get_index_stats(cursor)
    assert number_of_index_structures_are(index_stats, 0)

    execute_and_fetch_all(cursor, f"MATCH (n) DETACH DELETE n")


def test_auto_create_multiple_edge_type_index_with_multiple_entries(cursor):
    label_from = "LABEL_FROM"
    label_to = "LABEL_TO"
    edge_type1 = "SOMEEDGETYPE1"
    edge_type2 = "SOMEEDGETYPE2"
    edge_type3 = "SOMEEDGETYPE3"

    execute_and_fetch_all(cursor, f"CREATE (n:{label_from})")
    execute_and_fetch_all(cursor, f"CREATE (n:{label_to})")
    for _ in range(120):
        execute_and_fetch_all(cursor, f"MATCH (n:{label_from}), (m:{label_to}) CREATE (n)-[:{edge_type1}]->(m)")
    for _ in range(100):
        execute_and_fetch_all(cursor, f"MATCH (n:{label_from}), (m:{label_to}) CREATE (n)-[:{edge_type2}]->(m)")
    for _ in range(80):
        execute_and_fetch_all(cursor, f"MATCH (n:{label_from}), (m:{label_to}) CREATE (n)-[:{edge_type3}]->(m)")

    index_stats = get_index_stats(cursor)

    assert number_of_index_structures_are(index_stats, 5)  # 2 label + 3 edge-type
    assert index_exists(index_stats, label_from)
    assert index_exists(index_stats, label_to)
    assert index_exists(index_stats, edge_type1)
    assert index_exists(index_stats, edge_type2)
    assert index_exists(index_stats, edge_type3)
    assert index_count_is(index_stats, edge_type1, 120)
    assert index_count_is(index_stats, edge_type2, 100)
    assert index_count_is(index_stats, edge_type3, 80)

    execute_and_fetch_all(cursor, f"DROP INDEX ON :{label_from}")
    index_stats = get_index_stats(cursor)
    assert number_of_index_structures_are(index_stats, 4)

    execute_and_fetch_all(cursor, f"DROP INDEX ON :{label_to}")
    index_stats = get_index_stats(cursor)
    assert number_of_index_structures_are(index_stats, 3)

    execute_and_fetch_all(cursor, f"DROP EDGE INDEX ON :{edge_type1}")
    index_stats = get_index_stats(cursor)
    assert number_of_index_structures_are(index_stats, 2)

    execute_and_fetch_all(cursor, f"DROP EDGE INDEX ON :{edge_type2}")
    index_stats = get_index_stats(cursor)
    assert number_of_index_structures_are(index_stats, 1)

    execute_and_fetch_all(cursor, f"DROP EDGE INDEX ON :{edge_type3}")
    index_stats = get_index_stats(cursor)
    assert number_of_index_structures_are(index_stats, 0)

    execute_and_fetch_all(cursor, f"MATCH (n) DETACH DELETE n")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
