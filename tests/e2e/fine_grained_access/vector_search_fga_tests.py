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

import sys

import common
import mgclient
import pytest

# Fixture summary (setup lives in workloads.yaml):
#   user                    - label-restricted: GRANT :Public + :LINKS_PUB + :MIXED, DENY :Document + :LINKS_DOC,
#                             plus global GRANT READ {*} (reads embeddings on the labels it can see)
#   user_prop               - full label READ + GRANT READ {*}, DENY {title}/{label} on some labels/types
#   user_prop_deny_indexed  - full label READ + GRANT READ {*}, DENY {embedding} :Public (drops :Public hits)
# Indexed data: pub/doc vector indexes on :Public/:Document(embedding); pub/doc/mixed edge vector
# indexes on :LINKS_PUB/:LINKS_DOC/:MIXED(embedding); wild_vec wildcard index on (embedding).


def admin_cursor():
    return common.connect(username="admin", password="test").cursor()


def user_cursor():
    return common.connect(username="user", password="test").cursor()


def user_prop_cursor():
    return common.connect(username="user_prop", password="test").cursor()


def user_prop_deny_indexed_cursor():
    return common.connect(username="user_prop_deny_indexed", password="test").cursor()


def user_grant_only_cursor():
    return common.connect(username="user_grant_only", password="test").cursor()


# vector_search on vertices --------------------------------------------------


# a denied label yields no hits (silent per-row filter), not an error
def test_vector_search_dropped_on_denied_label():
    res = common.execute_and_fetch_all(
        user_cursor(),
        "CALL vector_search.search('doc_vec', 10, [1.0, 0.0]) YIELD node RETURN node;",
    )
    assert res == []


# deny-by-default: user_grant_only has the :Public label but no {embedding} grant, so the embedding is
# unreadable — the similarity match/score must not surface. Locks in the NEUTRAL (ungranted) case that
# the old code leaked, distinct from an explicit DENY.
def test_vector_search_dropped_for_label_only_user():
    res = common.execute_and_fetch_all(
        user_grant_only_cursor(),
        "CALL vector_search.search('pub_vec', 10, [1.0, 0.0]) YIELD node RETURN node;",
    )
    assert res == []


def test_vector_search_returns_allowed_label():
    res = common.execute_and_fetch_all(
        user_cursor(),
        "CALL vector_search.search('pub_vec', 10, [1.0, 0.0]) YIELD node RETURN node;",
    )
    assert len(res) >= 1


def test_vector_search_skips_multi_label_node_with_denied_label():
    res = common.execute_and_fetch_all(
        user_cursor(),
        "CALL vector_search.search('pub_vec', 10, [1.0, 0.0]) YIELD node RETURN node.title AS t;",
    )
    titles = {row[0] for row in res}
    assert "Hybrid" not in titles


def test_admin_vector_search_returns_results_on_denied_index():
    res = common.execute_and_fetch_all(
        admin_cursor(),
        "CALL vector_search.search('doc_vec', 10, [1.0, 0.0]) YIELD node RETURN node;",
    )
    assert len(res) >= 1


# vector_search on edges -----------------------------------------------------


def test_vector_search_edges_dropped_on_denied_type():
    res = common.execute_and_fetch_all(
        user_cursor(),
        "CALL vector_search.search_edges('doc_evec', 10, [1.0, 0.0]) YIELD edge RETURN edge;",
    )
    assert res == []


# NEUTRAL on edges: user_grant_only reads :LINKS_PUB edges but has no {embedding} grant, so the edge
# embedding is unreadable and the hit is dropped (the edge-vector NEUTRAL case behind F1).
def test_vector_search_edges_dropped_for_label_only_user():
    res = common.execute_and_fetch_all(
        user_grant_only_cursor(),
        "CALL vector_search.search_edges('pub_evec', 10, [1.0, 0.0]) YIELD edge RETURN edge;",
    )
    assert res == []


def test_vector_search_edges_returns_allowed_type():
    res = common.execute_and_fetch_all(
        user_cursor(),
        "CALL vector_search.search_edges('pub_evec', 10, [1.0, 0.0]) YIELD edge RETURN edge;",
    )
    assert len(res) >= 1


def test_vector_search_edges_skips_when_endpoint_denied():
    res = common.execute_and_fetch_all(
        user_cursor(),
        "CALL vector_search.search_edges('mixed_evec', 10, [1.0, 0.0]) YIELD edge RETURN edge;",
    )
    assert res == []


# property-level RBAC on vector search ---------------------------------------
# vector indexes are on .embedding — user_prop denies unrelated properties, search must still work


def test_vector_search_allowed_when_indexed_property_not_denied():
    res = common.execute_and_fetch_all(
        user_prop_cursor(),
        "CALL vector_search.search('doc_vec', 10, [1.0, 0.0]) YIELD node RETURN node;",
    )
    assert len(res) >= 1


def test_vector_search_edges_allowed_when_indexed_property_not_denied():
    res = common.execute_and_fetch_all(
        user_prop_cursor(),
        "CALL vector_search.search_edges('doc_evec', 10, [1.0, 0.0]) YIELD edge RETURN edge;",
    )
    assert len(res) >= 1


# WILDCARD vector index (CREATE VECTOR INDEX wild_vec ON (embedding))
# label precheck stays permissive; row filter handles partial label access; property precheck relaxes
# to allow when the caller has a global GRANT on the indexed property and no per-label DENY of it.


def test_wildcard_vector_search_allowed_for_label_restricted_user():
    res = common.execute_and_fetch_all(
        user_cursor(),
        "CALL vector_search.search('wild_vec', 10, [1.0, 0.0]) YIELD node RETURN node;",
    )
    # 7 nodes carry .embedding: 3 pure :Public (Welcome, Hello, CrossSrc), 3 pure :Document
    # (Secret, Internal, CrossDst), 1 multi-label :Public:Document (Hybrid). DENY :Document strips
    # every :Document-bearing node; user has no property GRANT so titles come back null.
    labels_per_row = [frozenset(row[0].labels) for row in res]
    assert len(labels_per_row) == 3
    assert all(labels == frozenset({"Public"}) for labels in labels_per_row)


# user_prop has DENY {title} but that doesn't affect the indexed property (.embedding), and a global
# GRANT {*} covers embedding — wildcard search should be allowed
def test_wildcard_vector_search_allowed_when_unrelated_property_denied():
    res = common.execute_and_fetch_all(
        user_prop_cursor(),
        "CALL vector_search.search('wild_vec', 10, [1.0, 0.0]) YIELD node RETURN node;",
    )
    assert len(res) >= 1


# user_prop_deny_indexed has DENY {embedding} :Public. On the WILDCARD index the row filter is per-hit:
# any node carrying :Public has its embedding denied (deny wins) and is dropped; nodes without :Public
# keep their readable embedding. So the search returns results, but none bearing :Public.
def test_wildcard_vector_search_drops_nodes_with_per_label_indexed_property_deny():
    res = common.execute_and_fetch_all(
        user_prop_deny_indexed_cursor(),
        "CALL vector_search.search('wild_vec', 10, [1.0, 0.0]) YIELD node RETURN node;",
    )
    assert len(res) >= 1
    assert all("Public" not in row[0].labels for row in res)


# Hybrid is :Public:Document and lives in doc_vec (:Document). user_prop_deny_indexed DENY {embedding}
# :Public makes Hybrid's embedding unreadable (deny wins over the global grant) even though the index is
# keyed on :Document — the per-row filter must drop it, matching normal iteration (n.embedding is NULL).
def test_vector_search_drops_multilabel_node_with_denied_indexed_property():
    res = common.execute_and_fetch_all(
        user_prop_deny_indexed_cursor(),
        "CALL vector_search.search('doc_vec', 10, [1.0, 0.0]) YIELD node RETURN node.title AS title;",
    )
    titles = {row[0] for row in res}
    assert "Hybrid" not in titles
    assert "Secret" in titles


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
