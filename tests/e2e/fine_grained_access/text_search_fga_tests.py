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
#   user                 - label-restricted: GRANT READ :Public + :LINKS_PUB + :MIXED, DENY :Document + :LINKS_DOC,
#                          plus global GRANT READ {*} (so it reads content on the labels it can see)
#   user_prop            - full label READ + GRANT READ {*}, DENY {title} :Public/:Document, DENY {label} :LINKS_*
#   user_grant_only      - GRANT READ :Public + :LINKS_PUB only, no property rules (deny-by-default: reads no values)
#   user_prop_deny_cross - full label READ + GRANT READ {*}, DENY {title} :Public only


def admin_cursor():
    return common.connect(username="admin", password="test").cursor()


def user_cursor():
    return common.connect(username="user", password="test").cursor()


def user_prop_cursor():
    return common.connect(username="user_prop", password="test").cursor()


def user_grant_only_cursor():
    return common.connect(username="user_grant_only", password="test").cursor()


def user_prop_deny_cross_cursor():
    return common.connect(username="user_prop_deny_cross", password="test").cursor()


# text_search on vertices -----------------------------------------------------


def test_text_search_dropped_on_denied_label():
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


# search_all and regex are distinct procs; confirm each returns a permitted hit (so the empty results
# below are meaningful drops, not a broken mode). Their RBAC drop path is the shared row filter, already
# covered by the label-deny and NEUTRAL cases.
def test_text_search_all_returns_allowed():
    res = common.execute_and_fetch_all(
        user_cursor(),
        "CALL text_search.search_all('pub_text', 'Welcome') YIELD node RETURN node;",
    )
    assert len(res) == 1


def test_text_regex_search_returns_allowed():
    res = common.execute_and_fetch_all(
        user_cursor(),
        "CALL text_search.regex_search('pub_text', 'Wel.*') YIELD node RETURN node;",
    )
    assert len(res) == 1


# deny-by-default: user_grant_only has the :Public label but no property grant, so it can read no title
# value — a content match must not surface.
def test_text_search_all_dropped_for_label_only_user():
    res = common.execute_and_fetch_all(
        user_grant_only_cursor(),
        "CALL text_search.search_all('pub_text', 'Welcome') YIELD node RETURN node;",
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


def test_text_search_edges_dropped_on_denied_type():
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


# property-level RBAC on text search ----------------------------------------
# a hit is dropped when the searched property is unreadable on its real labels


def test_text_search_dropped_on_property_denied():
    res = common.execute_and_fetch_all(
        user_prop_cursor(),
        "CALL text_search.search('doc_text', 'data.title:Secret') YIELD node RETURN node;",
    )
    assert res == []


def test_text_search_edges_dropped_on_property_denied():
    res = common.execute_and_fetch_all(
        user_prop_cursor(),
        "CALL text_search.search_edges('doc_etext', 'data.label:Confidential') YIELD edge RETURN edge;",
    )
    assert res == []


# per-row property drop on a specified text index (doc_title_text = :Document(title)):
# Hybrid is :Public:Document; user_prop_deny_cross DENY {title} :Public denies its title (deny wins),
# so it must be dropped, while a pure :Document title stays readable.
def test_text_search_drops_multilabel_node_with_denied_property():
    res = common.execute_and_fetch_all(
        user_prop_deny_cross_cursor(),
        "CALL text_search.search('doc_title_text', 'data.title:Hybrid') YIELD node RETURN node.title AS title;",
    )
    assert res == []


def test_text_search_returns_readable_title_on_specified_index():
    res = common.execute_and_fetch_all(
        user_prop_deny_cross_cursor(),
        "CALL text_search.search('doc_title_text', 'data.title:Secret') YIELD node RETURN node.title AS title;",
    )
    assert [row[0] for row in res] == ["Secret"]


# aggregate procs -----------------------------------------------------------
# Aggregation runs inside Tantivy with no per-row hook, so it's gated up front: the caller must be able
# to READ the index's bound label AND every indexed property (property read is deny-by-default, so a
# label-only grant is not enough — it can't read the values it would aggregate). Admins (no fine-grained
# checker) always may.


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


# user_grant_only has label GRANT :Public but NO property grant — deny-by-default means it can't read
# the title values, so aggregating over them would leak. The gate must block it.
def test_aggregate_blocked_for_label_only_user_without_property_grant():
    with pytest.raises(mgclient.DatabaseError):
        common.execute_and_fetch_all(
            user_grant_only_cursor(),
            "CALL text_search.aggregate('pub_text', 'data.title:Welcome', "
            '\'{"c":{"value_count":{"field":"data.title"}}}\') YIELD aggregation RETURN aggregation;',
        )


def test_aggregate_edges_blocked_for_label_only_user_without_property_grant():
    with pytest.raises(mgclient.DatabaseError):
        common.execute_and_fetch_all(
            user_grant_only_cursor(),
            "CALL text_search.aggregate_edges('pub_etext', 'data.label:Open', "
            '\'{"c":{"value_count":{"field":"data.label"}}}\') YIELD aggregation RETURN aggregation;',
        )


# user_prop_deny_cross reads title on :Document (global {*}, DENY only on :Public), so it may aggregate
# the specified doc_title_text index whose sole property it can read.
def test_aggregate_allowed_with_property_grant_on_specified_index():
    res = common.execute_and_fetch_all(
        user_prop_deny_cross_cursor(),
        "CALL text_search.aggregate('doc_title_text', 'data.title:Secret', "
        '\'{"c":{"value_count":{"field":"data.title"}}}\') YIELD aggregation RETURN aggregation;',
    )
    assert len(res) == 1


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
