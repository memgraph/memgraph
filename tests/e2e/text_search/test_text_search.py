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

import mgclient
import pytest
from common import memgraph, memgraph_with_mixed_data, memgraph_with_text_indexed_data

GET_RULES_2024_DOCUMENT = """CALL text_search.search('complianceDocuments', 'data.title:Rules2024') YIELD node
             RETURN node.title AS title, node.version AS version
             ORDER BY version ASC, title ASC;"""


def test_create_index(memgraph):
    memgraph.execute("CREATE TEXT INDEX exampleIndex ON :Document;")

    index_info = memgraph.execute_and_fetch("SHOW INDEX INFO")

    assert list(index_info) == [{"index type": "text", "label": "exampleIndex", "property": None, "count": None}]


def test_drop_index(memgraph):
    memgraph.execute("DROP TEXT INDEX exampleIndex;")

    index_info = memgraph.execute_and_fetch("SHOW INDEX INFO")

    assert list(index_info) == []


def test_text_search_given_property(memgraph_with_text_indexed_data):
    result = list(memgraph_with_text_indexed_data.execute_and_fetch(GET_RULES_2024_DOCUMENT))

    assert len(result) == 2 and result == [{"title": "Rules2024", "version": 1}, {"title": "Rules2024", "version": 2}]


def test_text_search_all_properties(memgraph_with_text_indexed_data):
    # json issue
    result = list(
        memgraph_with_text_indexed_data.execute_and_fetch(
            """CALL text_search.search('complianceDocuments', 'Rules2024') YIELD node
             RETURN node.title AS title, node.version AS version
             ORDER BY version ASC, title ASC;"""
        )
    )

    assert len(result) == 2 and result == [{"title": "Rules2024", "version": 1}, {"title": "Rules2024", "version": 2}]


def test_create_indexed_node(memgraph_with_text_indexed_data):
    memgraph_with_text_indexed_data.execute("CREATE (:Document {title: 'Rules2024', version: 3});")

    result = list(memgraph_with_text_indexed_data.execute_and_fetch(GET_RULES_2024_DOCUMENT))

    assert len(result) == 3 and result == [
        {"title": "Rules2024", "version": 1},
        {"title": "Rules2024", "version": 2},
        {"title": "Rules2024", "version": 3},
    ]


def test_delete_indexed_node(memgraph_with_text_indexed_data):
    memgraph_with_text_indexed_data.execute("MATCH (n:Document {title: 'Rules2024', version: 2}) DETACH DELETE n;")

    result = list(memgraph_with_text_indexed_data.execute_and_fetch(GET_RULES_2024_DOCUMENT))

    assert len(result) == 1 and result == [{"title": "Rules2024", "version": 1}]


def test_add_indexed_label(memgraph_with_mixed_data):
    # flaky search results
    memgraph_with_mixed_data.execute("MATCH (n:Revision) SET n:Document;")

    result = list(memgraph_with_mixed_data.execute_and_fetch(GET_RULES_2024_DOCUMENT))

    assert len(result) == 2 and result == [{"title": "Rules2024", "version": 1}, {"title": "Rules2024", "version": 2}]


def test_remove_indexed_label(memgraph_with_mixed_data):
    memgraph_with_mixed_data.execute("MATCH (n:Document {version: 1}) REMOVE n:Document;")

    result = list(memgraph_with_mixed_data.execute_and_fetch(GET_RULES_2024_DOCUMENT))

    assert len(result) == 0


def test_add_text_property_to_indexed_node(memgraph_with_text_indexed_data):
    try:
        memgraph_with_text_indexed_data.execute("MATCH (n:Document {version:1}) SET n.author = 'Nenad Patel';")
    except mgclient.DatabaseError:
        assert True

    assert False


def test_update_text_property_of_indexed_node(memgraph_with_text_indexed_data):
    memgraph_with_text_indexed_data.execute("MATCH (n:Document {version:1}) SET n.title = 'Rules2030';")

    result = list(
        memgraph_with_text_indexed_data.execute_and_fetch(
            """CALL text_search.search('complianceDocuments', 'data.title:Rules2030') YIELD node
             RETURN node.title AS title, node.version AS version
             ORDER BY version ASC, title ASC;"""
        )
    )

    assert len(result) == 1 and result == [{"title": "Rules2030", "version": 1}]


def test_add_non_text_property_to_indexed_node(memgraph_with_text_indexed_data):
    memgraph_with_text_indexed_data.execute("MATCH (n:Document {version:1}) SET n.wordcount = 1926;")


def test_remove_text_property_from_indexed_node(memgraph_with_text_indexed_data):
    try:
        memgraph_with_text_indexed_data.execute("MATCH (n:Document {version:1}) REMOVE n.title;")
    except mgclient.DatabaseError:
        assert True

    assert False


def test_remove_non_text_property_from_indexed_node(memgraph_with_text_indexed_data):
    memgraph_with_text_indexed_data.execute_and_fetch("MATCH (n:Document {version:1}) REMOVE n.version;")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
