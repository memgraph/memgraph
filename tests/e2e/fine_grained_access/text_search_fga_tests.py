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


# =============================================================================
# admin — unrestricted (no fine-grained checker attached): sees every label and property, so search
# and aggregate work even on labels/indexes a restricted user is denied.
# =============================================================================


def test_admin_text_search_returns_results_on_denied_index():
    res = common.execute_and_fetch_all(
        admin_cursor(),
        "CALL text_search.search('doc_text', 'data.title:Secret') YIELD node RETURN node;",
    )
    assert len(res) >= 1


def test_admin_search_all_returns_results_on_denied_index():
    res = common.execute_and_fetch_all(
        admin_cursor(),
        "CALL text_search.search_all('doc_text', 'Secret') YIELD node RETURN node;",
    )
    assert len(res) >= 1


def test_admin_aggregate_works():
    res = common.execute_and_fetch_all(
        admin_cursor(),
        "CALL text_search.aggregate('doc_text', 'data.title:Secret', "
        '\'{"c":{"value_count":{"field":"data.title"}}}\') YIELD aggregation RETURN aggregation;',
    )
    assert len(res) == 1


# =============================================================================
# user — label-restricted. GRANT READ :Public, :LINKS_PUB, :MIXED; DENY READ :Document, :LINKS_DOC.
# Also global GRANT READ {*}, so on the labels it can see it reads all content (it is NOT property-
# restricted). Consequences: :Public content is fully searchable; :Document/:LINKS_DOC are invisible;
# a :MIXED edge is dropped because its endpoint is :Document; aggregate over a denied label is blocked.
# =============================================================================


def test_text_search_returns_allowed_label():
    res = common.execute_and_fetch_all(
        user_cursor(),
        "CALL text_search.search('pub_text', 'data.title:Welcome') YIELD node RETURN node;",
    )
    assert len(res) == 1


# search_all and regex are distinct procs; confirm each returns a permitted hit (so the empty results
# elsewhere are meaningful drops, not a broken mode). Their RBAC drop path is the shared row filter.
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


def test_text_fuzzy_phrase_search_returns_allowed():
    res = common.execute_and_fetch_all(
        user_cursor(),
        "CALL text_search.fuzzy_phrase_search('pub_text', 'data.title:Welcome') YIELD node RETURN node;",
    )
    assert len(res) == 1


def test_text_fuzzy_phrase_search_dropped_on_denied_label():
    res = common.execute_and_fetch_all(
        user_cursor(),
        "CALL text_search.fuzzy_phrase_search('doc_text', 'data.title:Secret') YIELD node RETURN node;",
    )
    assert res == []


def test_text_fuzzy_phrase_search_edges_dropped_on_denied_type():
    res = common.execute_and_fetch_all(
        user_cursor(),
        "CALL text_search.fuzzy_phrase_search_edges('doc_etext', 'data.label:Confidential') YIELD edge RETURN edge;",
    )
    assert res == []


def test_text_search_dropped_on_denied_label():
    res = common.execute_and_fetch_all(
        user_cursor(),
        "CALL text_search.search('doc_text', 'data.title:Secret') YIELD node RETURN node;",
    )
    assert res == []


# Hybrid is :Public:Document — reachable via the :Public index, but DENY :Document wins per-row.
def test_text_search_skips_multi_label_node_with_denied_label():
    res = common.execute_and_fetch_all(
        user_cursor(),
        "CALL text_search.search('pub_text', 'data.title:Hybrid') YIELD node RETURN node;",
    )
    assert res == []


def test_text_search_edges_returns_allowed_type():
    res = common.execute_and_fetch_all(
        user_cursor(),
        "CALL text_search.search_edges('pub_etext', 'data.label:Open') YIELD edge RETURN edge;",
    )
    assert len(res) == 1


def test_text_search_edges_dropped_on_denied_type():
    res = common.execute_and_fetch_all(
        user_cursor(),
        "CALL text_search.search_edges('doc_etext', 'data.label:Confidential') YIELD edge RETURN edge;",
    )
    assert res == []


# :MIXED edge-type READ is granted, but the edge's endpoint is :Document — endpoint deny wins.
def test_text_search_edges_skips_when_endpoint_denied():
    res = common.execute_and_fetch_all(
        user_cursor(),
        "CALL text_search.search_edges('mixed_etext', 'data.label:CrossOver') YIELD edge RETURN edge;",
    )
    assert res == []


# aggregate is blocked because user lacks READ on the index's bound label/type (:Document / :LINKS_DOC).
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


# =============================================================================
# user_prop — full label READ + global GRANT READ {*}, but DENY {title} on :Public/:Document and
# DENY {label} on :LINKS_*. So it reads every property except title/label. A search over a denied
# property (or a blob search_all, which cannot isolate the matched field) never surfaces; but a
# field-scoped search over a readable property does, even when a co-located property is denied.
# =============================================================================


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


# the Dual node is :Public {title, body}; title is denied, body is readable. A field-scoped search over
# body surfaces it — only the queried property gates the hit, not every indexed property.
def test_text_search_returns_readable_queried_property():
    res = common.execute_and_fetch_all(
        user_prop_cursor(),
        "CALL text_search.search('pub_text', 'data.body:Bodyword') YIELD node RETURN node.body AS body;",
    )
    assert [row[0] for row in res] == ["Bodyword"]


def test_text_search_dropped_when_queried_property_denied():
    res = common.execute_and_fetch_all(
        user_prop_cursor(),
        "CALL text_search.search('pub_text', 'data.title:Dual') YIELD node RETURN node;",
    )
    assert res == []


# search_all cannot tell which property matched, so any denied string property on the node drops it.
def test_text_search_all_dropped_when_any_string_property_denied():
    res = common.execute_and_fetch_all(
        user_prop_cursor(),
        "CALL text_search.search_all('pub_text', 'Bodyword') YIELD node RETURN node;",
    )
    assert res == []


# the Solo node has only body; an OR query names both the denied title and the readable body. The queried
# title is absent on the node, so it is skipped rather than dropping the hit, and body surfaces it.
def test_text_search_or_query_skips_queried_property_the_node_lacks():
    res = common.execute_and_fetch_all(
        user_prop_cursor(),
        "CALL text_search.search('pub_text', 'data.title:nomatch OR data.body:Solo') YIELD node RETURN node.body AS body;",
    )
    assert [row[0] for row in res] == ["Solo"]


# same field-scoped precision on edges: DualEdge is :LINKS_PUB {label, note}; label is denied, note is
# readable, so a search over note surfaces it, and an OR naming the absent label still returns SoloEdge.
def test_text_search_edges_returns_readable_queried_property():
    res = common.execute_and_fetch_all(
        user_prop_cursor(),
        "CALL text_search.search_edges('pub_etext', 'data.note:EdgeNote') YIELD edge RETURN edge.note AS note;",
    )
    assert [row[0] for row in res] == ["EdgeNote"]


def test_text_search_edges_or_query_skips_queried_property_the_edge_lacks():
    res = common.execute_and_fetch_all(
        user_prop_cursor(),
        "CALL text_search.search_edges('pub_etext', 'data.label:none OR data.note:SoloEdge') YIELD edge RETURN edge.note AS note;",
    )
    assert [row[0] for row in res] == ["SoloEdge"]


# =============================================================================
# user_grant_only — GRANT READ :Public + :LINKS_PUB only, NO property grant. Property read is
# deny-by-default, so it can read no property value: a content match never surfaces, and aggregation
# (which would count values it cannot read) is blocked even on a label it can see.
# =============================================================================


def test_text_search_all_dropped_for_label_only_user():
    res = common.execute_and_fetch_all(
        user_grant_only_cursor(),
        "CALL text_search.search_all('pub_text', 'Welcome') YIELD node RETURN node;",
    )
    assert res == []


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


# =============================================================================
# user_prop_deny_cross — full label READ + global GRANT READ {*}, DENY {title} on :Public ONLY.
# Exercised against the specified index doc_title_text (:Document(title)): a pure :Document title is
# readable, but the :Public:Document Hybrid's title is denied because the deny on its co-label :Public
# wins. Since it can read :Document titles, it may also aggregate that index.
# =============================================================================


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


def test_aggregate_allowed_with_property_grant_on_specified_index():
    res = common.execute_and_fetch_all(
        user_prop_deny_cross_cursor(),
        "CALL text_search.aggregate('doc_title_text', 'data.title:Secret', "
        '\'{"c":{"value_count":{"field":"data.title"}}}\') YIELD aggregation RETURN aggregation;',
    )
    assert len(res) == 1


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
