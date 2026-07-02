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
#   user             - GRANT READ :Public + :LINKS_PUB + :MIXED, DENY READ :Document + :LINKS_DOC
#   user_prop        - full label READ + GRANT READ {*}, DENY {title} :Public/:Document, DENY {label} :LINKS_*
#   user_grant_only  - GRANT READ :Public + :LINKS_PUB only, no denies, no property rules
# Indexed data: pub/doc text indexes on :Public/:Document; pub/doc/mixed edge text indexes.


def admin_cursor():
    return common.connect(username="admin", password="test").cursor()


def user_cursor():
    return common.connect(username="user", password="test").cursor()


def user_prop_cursor():
    return common.connect(username="user_prop", password="test").cursor()


def user_grant_only_cursor():
    return common.connect(username="user_grant_only", password="test").cursor()


# text_search on vertices -----------------------------------------------------


def test_text_search_blocked_on_denied_label():
    with pytest.raises(mgclient.DatabaseError):
        common.execute_and_fetch_all(
            user_cursor(),
            "CALL text_search.search('doc_text', 'data.title:Secret') YIELD node RETURN node;",
        )


def test_text_search_returns_allowed_label():
    res = common.execute_and_fetch_all(
        user_cursor(),
        "CALL text_search.search('pub_text', 'data.title:Welcome') YIELD node RETURN node;",
    )
    assert len(res) == 1


def test_text_search_all_blocked_on_denied_label():
    with pytest.raises(mgclient.DatabaseError):
        common.execute_and_fetch_all(
            user_cursor(),
            "CALL text_search.search_all('doc_text', 'Secret') YIELD node RETURN node;",
        )


def test_text_regex_search_blocked_on_denied_label():
    with pytest.raises(mgclient.DatabaseError):
        common.execute_and_fetch_all(
            user_cursor(),
            "CALL text_search.regex_search('doc_text', 'Sec.*') YIELD node RETURN node;",
        )


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


def test_text_search_edges_blocked_on_denied_type():
    with pytest.raises(mgclient.DatabaseError):
        common.execute_and_fetch_all(
            user_cursor(),
            "CALL text_search.search_edges('doc_etext', 'data.label:Confidential') YIELD edge RETURN edge;",
        )


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


# property-level RBAC on text search ----------------------------------------
# text indexes without an explicit property list cover every string property; any property RBAC blocks.


def test_text_search_blocked_on_property_denied():
    with pytest.raises(mgclient.DatabaseError):
        common.execute_and_fetch_all(
            user_prop_cursor(),
            "CALL text_search.search('doc_text', 'data.title:Secret') YIELD node RETURN node;",
        )


def test_text_search_edges_blocked_on_property_denied():
    with pytest.raises(mgclient.DatabaseError):
        common.execute_and_fetch_all(
            user_prop_cursor(),
            "CALL text_search.search_edges('doc_etext', 'data.label:Confidential') YIELD edge RETURN edge;",
        )


# aggregate procs -----------------------------------------------------------
# Tantivy can't honor per-row RBAC, so aggregate is blocked whenever a deny/property rule could
# alter what the aggregate counts. Pure-GRANT users on a granted label/type may run it.


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


# user_grant_only has label GRANT :Public + edge GRANT :LINKS_PUB with zero denies and no property rules
def test_aggregate_allowed_for_pure_grant_user_on_granted_label():
    res = common.execute_and_fetch_all(
        user_grant_only_cursor(),
        "CALL text_search.aggregate('pub_text', 'data.title:Welcome', "
        '\'{"c":{"value_count":{"field":"data.title"}}}\') YIELD aggregation RETURN aggregation;',
    )
    assert len(res) == 1


def test_aggregate_edges_allowed_for_pure_grant_user_on_granted_type():
    res = common.execute_and_fetch_all(
        user_grant_only_cursor(),
        "CALL text_search.aggregate_edges('pub_etext', 'data.label:Open', "
        '\'{"c":{"value_count":{"field":"data.label"}}}\') YIELD aggregation RETURN aggregation;',
    )
    assert len(res) == 1


def test_aggregate_still_blocked_on_denied_label():
    with pytest.raises(mgclient.DatabaseError):
        common.execute_and_fetch_all(
            user_grant_only_cursor(),
            "CALL text_search.aggregate('doc_text', 'data.title:Secret', "
            '\'{"c":{"value_count":{"field":"data.title"}}}\') YIELD aggregation RETURN aggregation;',
        )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
