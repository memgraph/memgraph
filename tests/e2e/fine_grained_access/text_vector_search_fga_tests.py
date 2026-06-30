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

# `user` has GRANT READ on :Public + :LINKS_PUB + :MIXED and DENY READ on :Document + :LINKS_DOC.
# Indexed data: pub/doc text + vector indexes on labels, edge variants on edge types,
# plus a :Public:Document multi-label "Hybrid" node and a :MIXED edge spanning Public→Document.


def admin_cursor():
    return common.connect(username="admin", password="test").cursor()


def user_cursor():
    return common.connect(username="user", password="test").cursor()


# text_search on vertices -----------------------------------------------------


def test_text_search_filters_denied_label():
    res = common.execute_and_fetch_all(
        user_cursor(),
        "CALL text_search.search('doc_text', 'data.title:Secret') YIELD node RETURN node;",
    )
    assert res == []


def test_text_search_returns_allowed_label():
    res = common.execute_and_fetch_all(
        user_cursor(),
        "CALL text_search.search('pub_text', 'data.title:Welcome') YIELD node RETURN node;",
    )
    assert len(res) == 1


def test_text_search_all_filters_denied_label():
    res = common.execute_and_fetch_all(
        user_cursor(),
        "CALL text_search.search_all('doc_text', 'Secret') YIELD node RETURN node;",
    )
    assert res == []


def test_text_regex_search_filters_denied_label():
    res = common.execute_and_fetch_all(
        user_cursor(),
        "CALL text_search.regex_search('doc_text', 'Sec.*') YIELD node RETURN node;",
    )
    assert res == []


# Hybrid is :Public:Document — visible via pub_text index but DENY :Document wins per-row.
def test_text_search_skips_multi_label_node_with_denied_label():
    res = common.execute_and_fetch_all(
        user_cursor(),
        "CALL text_search.search('pub_text', 'data.title:Hybrid') YIELD node RETURN node;",
    )
    assert res == []


def test_admin_text_search_returns_results_on_denied_index():
    res = common.execute_and_fetch_all(
        admin_cursor(),
        "CALL text_search.search('doc_text', 'data.title:Secret') YIELD node RETURN node;",
    )
    assert len(res) >= 1


# text_search on edges -------------------------------------------------------


def test_text_search_edges_filters_denied_type():
    res = common.execute_and_fetch_all(
        user_cursor(),
        "CALL text_search.search_edges('doc_etext', 'data.label:Confidential') YIELD edge RETURN edge;",
    )
    assert res == []


def test_text_search_edges_returns_allowed_type():
    res = common.execute_and_fetch_all(
        user_cursor(),
        "CALL text_search.search_edges('pub_etext', 'data.label:Open') YIELD edge RETURN edge;",
    )
    assert len(res) == 1


# :MIXED edge is allowed by edge-type READ, but its endpoint is :Document — endpoint deny wins.
def test_text_search_edges_skips_when_endpoint_denied():
    res = common.execute_and_fetch_all(
        user_cursor(),
        "CALL text_search.search_edges('mixed_etext', 'data.label:CrossOver') YIELD edge RETURN edge;",
    )
    assert res == []


# vector_search on vertices --------------------------------------------------


def test_vector_search_filters_denied_label():
    res = common.execute_and_fetch_all(
        user_cursor(),
        "CALL vector_search.search('doc_vec', 10, [1.0, 0.0]) YIELD node RETURN node;",
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


def test_vector_search_edges_filters_denied_type():
    res = common.execute_and_fetch_all(
        user_cursor(),
        "CALL vector_search.search_edges('doc_evec', 10, [1.0, 0.0]) YIELD edge RETURN edge;",
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


# aggregate procs — blocked under any fine-grained restriction since Tantivy can't honor per-row perms


def test_aggregate_blocked_for_restricted_user():
    with pytest.raises(mgclient.DatabaseError):
        common.execute_and_fetch_all(
            user_cursor(),
            "CALL text_search.aggregate('doc_text', 'data.title:Secret', "
            '\'{"c":{"value_count":{"field":"data.title"}}}\') YIELD aggregation RETURN aggregation;',
        )


def test_aggregate_edges_blocked_for_restricted_user():
    with pytest.raises(mgclient.DatabaseError):
        common.execute_and_fetch_all(
            user_cursor(),
            "CALL text_search.aggregate_edges('doc_etext', 'data.label:Confidential', "
            '\'{"c":{"value_count":{"field":"data.label"}}}\') YIELD aggregation RETURN aggregation;',
        )


def test_admin_aggregate_works():
    res = common.execute_and_fetch_all(
        admin_cursor(),
        "CALL text_search.aggregate('doc_text', 'data.title:Secret', "
        '\'{"c":{"value_count":{"field":"data.title"}}}\') YIELD aggregation RETURN aggregation;',
    )
    assert len(res) == 1


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
