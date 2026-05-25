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

import json
import sys

import pytest
from common import *

SHOW_SCHEMA_INFO_QUERY = "SHOW SCHEMA INFO;"


def get_admin_cursor():
    return connect(username="admin", password="").cursor()


def get_josip_cursor():
    return connect(username="josip", password="").cursor()


def get_toni_cursor():
    return connect(username="toni", password="").cursor()


def get_buda_cursor():
    return connect(username="buda", password="").cursor()


def get_kate_cursor():
    return connect(username="kate", password="").cursor()


def get_matea_cursor():
    return connect(username="matea", password="").cursor()


def get_schema_for(user_cursor):
    return json.loads(execute_and_fetch_all(user_cursor, SHOW_SCHEMA_INFO_QUERY)[0][0])


def can_see_node(schema, node_label):
    return node_label in [label for node in schema["nodes"] for label in node["labels"]]


def can_see_edge(schema, edge_type):
    return edge_type in [edge["type"] for edge in schema["edges"]]


def can_see_node_index(schema, node_label):
    return node_label in [label for node_index in schema["node_indexes"] for label in node_index["labels"]]


def can_see_edge_index(schema, edge_type):
    return edge_type in [et for edge in schema["edge_indexes"] for et in edge["edge_type"]]


def can_see_constraint(schema, node_label):
    return node_label in [label for constraint in schema["node_constraints"] for label in constraint["labels"]]


def test_show_schema_info_with_fine_grained_access_control():
    admin = get_admin_cursor()
    josip = get_josip_cursor()
    toni = get_toni_cursor()
    buda = get_buda_cursor()
    kate = get_kate_cursor()
    matea = get_matea_cursor()

    execute_and_fetch_all(admin, "CREATE (:Public {id: 1})-[:TYPE {id: 3}]->(:Private {id: 2})")
    execute_and_fetch_all(admin, "CREATE INDEX ON :Public;")
    execute_and_fetch_all(admin, "CREATE INDEX ON :Public(id);")
    execute_and_fetch_all(admin, "CREATE INDEX ON :Private;")
    execute_and_fetch_all(admin, "CREATE INDEX ON :Private(id);")
    execute_and_fetch_all(admin, "CREATE EDGE INDEX ON :TYPE;")
    execute_and_fetch_all(admin, "CREATE EDGE INDEX ON :TYPE(id);")
    execute_and_fetch_all(admin, "CREATE CONSTRAINT ON (n:Public) ASSERT EXISTS (n.id);")
    execute_and_fetch_all(admin, "CREATE CONSTRAINT ON (n:Public) ASSERT n.id IS UNIQUE;")
    execute_and_fetch_all(admin, "CREATE CONSTRAINT ON (n:Public) ASSERT n.id IS TYPED INTEGER;")
    admin_schema = get_schema_for(admin)

    assert can_see_node(admin_schema, "Public")
    assert can_see_node(admin_schema, "Private")
    assert can_see_edge(admin_schema, "TYPE")
    assert can_see_node_index(admin_schema, "Public")
    assert can_see_node_index(admin_schema, "Private")
    assert can_see_edge_index(admin_schema, "TYPE")
    assert can_see_constraint(admin_schema, "Public")

    josip_schema = get_schema_for(josip)

    assert can_see_node(josip_schema, "Public")
    assert can_see_node(josip_schema, "Private")
    assert can_see_edge(josip_schema, "TYPE")
    assert can_see_node_index(josip_schema, "Public")
    assert can_see_node_index(josip_schema, "Private")
    assert can_see_edge_index(josip_schema, "TYPE")
    assert can_see_constraint(josip_schema, "Public")

    toni_schema = get_schema_for(toni)

    assert can_see_node(toni_schema, "Public")
    assert can_see_node(toni_schema, "Private")
    assert not can_see_edge(toni_schema, "TYPE")
    assert can_see_node_index(toni_schema, "Public")
    assert can_see_node_index(toni_schema, "Private")
    assert not can_see_edge_index(toni_schema, "TYPE")
    assert can_see_constraint(toni_schema, "Public")

    buda_schema = get_schema_for(buda)

    assert not can_see_node(buda_schema, "Public")
    assert not can_see_node(buda_schema, "Private")
    assert not can_see_edge(buda_schema, "TYPE")
    assert not can_see_node_index(buda_schema, "Public")
    assert not can_see_node_index(buda_schema, "Private")
    assert not can_see_edge_index(buda_schema, "TYPE")
    assert not can_see_constraint(buda_schema, "Public")

    kate_schema = get_schema_for(kate)

    assert can_see_node(kate_schema, "Public")
    assert not can_see_node(kate_schema, "Private")
    assert not can_see_edge(kate_schema, "TYPE")
    assert can_see_node_index(kate_schema, "Public")
    assert not can_see_node_index(kate_schema, "Private")
    assert not can_see_edge_index(kate_schema, "TYPE")
    assert can_see_constraint(kate_schema, "Public")

    matea_schema = get_schema_for(matea)

    assert not can_see_node(matea_schema, "Public")
    assert not can_see_node(matea_schema, "Private")
    assert not can_see_edge(matea_schema, "TYPE")
    assert not can_see_node_index(matea_schema, "Public")
    assert not can_see_node_index(matea_schema, "Private")
    assert can_see_edge_index(matea_schema, "TYPE")
    assert not can_see_constraint(matea_schema, "Public")

    assert True


def test_schema_info_with_matching_any_lbac():
    """Test that SHOW SCHEMA INFO respects MATCHING ANY semantics for LBAC.

    Regression test for https://github.com/memgraph/memgraph/issues/3809

    When a user has READ permission on a label with MATCHING ANY, they should
    see nodes in schema info if the node has at least one permitted label,
    even if the node also has other labels the user doesn't have permission for.
    """
    admin = get_admin_cursor()

    execute_and_fetch_all(admin, "MATCH (n) DETACH DELETE n")

    execute_and_fetch_all(admin, "CREATE USER testuser;")
    execute_and_fetch_all(admin, "CREATE ROLE test_role;")
    execute_and_fetch_all(admin, "GRANT MATCH, STATS TO test_role;")
    execute_and_fetch_all(admin, "GRANT READ ON NODES CONTAINING LABELS :Visible MATCHING ANY TO test_role;")
    execute_and_fetch_all(admin, "SET ROLE FOR testuser TO test_role;")

    # Create a node with two labels - user only has permission on :Visible
    execute_and_fetch_all(admin, "CREATE (:Visible:Hidden {name: 'test'})")

    testuser = connect(username="testuser", password="").cursor()

    # User should be able to see the node via MATCH (MATCHING ANY semantics)
    result = execute_and_fetch_all(testuser, "MATCH (n) RETURN labels(n) AS labels")
    assert len(result) == 1, f"Expected 1 node, got {len(result)}"
    labels = result[0][0]
    assert "Visible" in labels, f"Expected 'Visible' in labels, got {labels}"
    assert "Hidden" in labels, f"Expected 'Hidden' in labels, got {labels}"

    # SHOW SCHEMA INFO should also show the node (same MATCHING ANY semantics)
    schema = get_schema_for(testuser)
    nodes = schema["nodes"]

    assert len(nodes) == 1, f"Expected 1 node type in schema, got {len(nodes)}: {nodes}"

    node_labels = nodes[0]["labels"]
    assert "Visible" in node_labels, f"Expected 'Visible' in schema labels, got {node_labels}"
    assert "Hidden" in node_labels, f"Expected 'Hidden' in schema labels, got {node_labels}"

    execute_and_fetch_all(admin, "DROP USER testuser")
    execute_and_fetch_all(admin, "DROP ROLE test_role")


def has_node_vector_index(schema, label_set):
    """Return True iff schema has a label+property_vector index whose labels (set) match label_set.

    `label_set` is an iterable of strings; pass [] for the wildcard index.
    """
    target = sorted(label_set)
    return any(
        ni.get("type") == "label+property_vector" and sorted(ni["labels"]) == target for ni in schema["node_indexes"]
    )


def has_edge_vector_index(schema, edge_type_set):
    """Return True iff schema has an edge_type+property_vector index whose edge_types match.

    `edge_type_set` is an iterable of strings; pass [] for the wildcard index.
    """
    target = sorted(edge_type_set)
    return any(
        ei.get("type") == "edge_type+property_vector" and sorted(ei["edge_type"]) == target
        for ei in schema["edge_indexes"]
    )


def test_show_schema_info_wildcard_vector_index_requires_global_read():
    """Wildcard vector indexes are visible only to users with global READ on the matching kind.
    Multi-label indexes require READ on EVERY listed label (else the missing labels leak)."""
    admin = get_admin_cursor()
    josip = get_josip_cursor()  # READ on nodes * AND edges *
    toni = get_toni_cursor()  # READ on nodes *
    buda = get_buda_cursor()  # STATS only
    kate = get_kate_cursor()  # READ on :Public only
    matea = get_matea_cursor()  # READ on :TYPE edges only

    cfg = '{"dimension": 2, "capacity": 10}'
    execute_and_fetch_all(admin, f"CREATE VECTOR INDEX wild_node ON (embedding) WITH CONFIG {cfg};")
    execute_and_fetch_all(admin, f"CREATE VECTOR INDEX pub_node ON :Public(embedding) WITH CONFIG {cfg};")
    # Multi-label index — auth must require READ on EVERY listed label, not just one.
    execute_and_fetch_all(admin, f"CREATE VECTOR INDEX or_node ON :Public|Private(embedding) WITH CONFIG {cfg};")
    execute_and_fetch_all(admin, f"CREATE VECTOR EDGE INDEX wild_edge ON (embedding) WITH CONFIG {cfg};")
    execute_and_fetch_all(admin, f"CREATE VECTOR EDGE INDEX type_edge ON :TYPE(embedding) WITH CONFIG {cfg};")

    # admin sees everything
    admin_schema = get_schema_for(admin)
    assert has_node_vector_index(admin_schema, [])
    assert has_node_vector_index(admin_schema, ["Public"])
    assert has_node_vector_index(admin_schema, ["Public", "Private"])
    assert has_edge_vector_index(admin_schema, [])
    assert has_edge_vector_index(admin_schema, ["TYPE"])

    # josip has global READ on both nodes and edges -> sees everything.
    josip_schema = get_schema_for(josip)
    assert has_node_vector_index(josip_schema, [])
    assert has_node_vector_index(josip_schema, ["Public"])
    assert has_node_vector_index(josip_schema, ["Public", "Private"])
    assert has_edge_vector_index(josip_schema, [])
    assert has_edge_vector_index(josip_schema, ["TYPE"])

    # toni has global READ on nodes only -> sees node wildcards but not edge wildcards.
    toni_schema = get_schema_for(toni)
    assert has_node_vector_index(toni_schema, [])
    assert has_node_vector_index(toni_schema, ["Public"])
    assert has_node_vector_index(toni_schema, ["Public", "Private"])
    assert not has_edge_vector_index(toni_schema, [])
    assert not has_edge_vector_index(toni_schema, ["TYPE"])

    # buda has no vertex/edge READ -> sees nothing
    buda_schema = get_schema_for(buda)
    assert not has_node_vector_index(buda_schema, [])
    assert not has_node_vector_index(buda_schema, ["Public"])
    assert not has_node_vector_index(buda_schema, ["Public", "Private"])
    assert not has_edge_vector_index(buda_schema, [])
    assert not has_edge_vector_index(buda_schema, ["TYPE"])

    # kate has READ only on :Public -> sees specific :Public index, NOT wildcard, NOT :Public|Private
    # (multi-label requires READ on EVERY listed label, otherwise we leak the existence of :Private).
    kate_schema = get_schema_for(kate)
    assert not has_node_vector_index(kate_schema, [])
    assert has_node_vector_index(kate_schema, ["Public"])
    assert not has_node_vector_index(kate_schema, ["Public", "Private"])
    assert not has_edge_vector_index(kate_schema, [])
    assert not has_edge_vector_index(kate_schema, ["TYPE"])

    # matea has READ only on :TYPE edges -> sees specific :TYPE edge index, NOT wildcards
    matea_schema = get_schema_for(matea)
    assert not has_node_vector_index(matea_schema, [])
    assert not has_node_vector_index(matea_schema, ["Public"])
    assert not has_node_vector_index(matea_schema, ["Public", "Private"])
    assert not has_edge_vector_index(matea_schema, [])
    assert has_edge_vector_index(matea_schema, ["TYPE"])

    execute_and_fetch_all(admin, "DROP VECTOR INDEX wild_node;")
    execute_and_fetch_all(admin, "DROP VECTOR INDEX pub_node;")
    execute_and_fetch_all(admin, "DROP VECTOR INDEX or_node;")
    execute_and_fetch_all(admin, "DROP VECTOR INDEX wild_edge;")
    execute_and_fetch_all(admin, "DROP VECTOR INDEX type_edge;")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
