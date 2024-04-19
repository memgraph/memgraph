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

import json
import re
import sys

import gqlalchemy
import mgclient
import pytest
from common import memgraph, memgraph_with_mixed_data, memgraph_with_text_indexed_data

GET_RULES_2024_DOCUMENT = """CALL text_search.search("complianceDocuments", "data.title:Rules2024") YIELD node
             RETURN node.title AS title, node.version AS version
             ORDER BY version ASC, title ASC;"""


def test_create_index(memgraph):
    memgraph.execute("""CREATE TEXT INDEX exampleIndex ON :Document;""")

    index_info = memgraph.execute_and_fetch("""SHOW INDEX INFO""")

    assert list(index_info) == [
        {"index type": "text (name: exampleIndex)", "label": "Document", "property": None, "count": None}
    ]


def test_drop_index(memgraph):
    memgraph.execute("""DROP TEXT INDEX exampleIndex;""")

    index_info = memgraph.execute_and_fetch("""SHOW INDEX INFO""")

    assert list(index_info) == []


def test_create_existing_index(memgraph):
    memgraph.execute("""CREATE TEXT INDEX duplicatedIndex ON :Document;""")
    with pytest.raises(
        gqlalchemy.exceptions.GQLAlchemyDatabaseError, match='Text index "duplicatedIndex" already exists.'
    ) as _:
        memgraph.execute("""CREATE TEXT INDEX duplicatedIndex ON :Document;""")
    memgraph.execute("""DROP TEXT INDEX duplicatedIndex;""")  # cleanup


def test_drop_nonexistent_index(memgraph):
    with pytest.raises(
        gqlalchemy.exceptions.GQLAlchemyDatabaseError, match='Text index "noSuchIndex" doesn’t exist.'
    ) as _:
        memgraph.execute("""DROP TEXT INDEX noSuchIndex;""")


def test_text_search_given_property(memgraph_with_text_indexed_data):
    result = list(memgraph_with_text_indexed_data.execute_and_fetch(GET_RULES_2024_DOCUMENT))

    assert len(result) == 2 and result == [{"title": "Rules2024", "version": 1}, {"title": "Rules2024", "version": 2}]


def test_text_search_all_properties(memgraph_with_text_indexed_data):
    SEARCH_QUERY = "Rules2024"

    ALL_PROPERTIES_QUERY = f"""CALL text_search.search_all("complianceDocuments", "{SEARCH_QUERY}") YIELD node
             RETURN node
             ORDER BY node.version ASC, node.title ASC;"""

    result = list(memgraph_with_text_indexed_data.execute_and_fetch(ALL_PROPERTIES_QUERY))
    result_nodes = [record["node"] for record in result]

    assert len(result) == 3 and (
        result_nodes[0].title == SEARCH_QUERY
        and result_nodes[1].title == SEARCH_QUERY
        and SEARCH_QUERY in result_nodes[2].fulltext
    )


def test_regex_text_search(memgraph_with_text_indexed_data):
    REGEX_QUERY = """CALL text_search.regex_search("complianceDocuments", "wor.*s") YIELD node
             RETURN node
             ORDER BY node.version ASC, node.title ASC;"""

    result = list(memgraph_with_text_indexed_data.execute_and_fetch(REGEX_QUERY))

    assert (
        len(result) == 2
        and re.search("wor.*s", result[0]["node"].fulltext)
        and re.search("wor.*s", result[1]["node"].fulltext)
        # In this test, all values matching the regex string are found in the .node property only ^
    )


def test_text_search_aggregate(memgraph_with_text_indexed_data):
    input_aggregation = json.dumps({"count": {"value_count": {"field": "metadata.gid"}}}, separators=(",", ":"))
    expected_aggregation = json.dumps({"count": {"value": 2.0}}, separators=(",", ":"))

    AGGREGATION_QUERY = f"""CALL text_search.aggregate("complianceDocuments", "data.title:Rules2024", '{input_aggregation}')
                YIELD aggregation
                RETURN aggregation;"""

    result = list(memgraph_with_text_indexed_data.execute_and_fetch(AGGREGATION_QUERY))

    assert len(result) == 1 and result[0]["aggregation"] == expected_aggregation


def test_text_search_query_boolean(memgraph_with_text_indexed_data):
    BOOLEAN_QUERY = """CALL text_search.search("complianceDocuments", "(data.title:Rules2023 OR data.title:Rules2024) AND data.fulltext:words") YIELD node
                RETURN node.title AS title, node.version AS version
                ORDER BY version ASC, title ASC;"""

    result = list(memgraph_with_text_indexed_data.execute_and_fetch(BOOLEAN_QUERY))

    assert len(result) == 1 and result == [{"title": "Rules2024", "version": 2}]


def test_create_indexed_node(memgraph_with_text_indexed_data):
    memgraph_with_text_indexed_data.execute("""CREATE (:Document {title: "Rules2024", version: 3});""")

    result = list(memgraph_with_text_indexed_data.execute_and_fetch(GET_RULES_2024_DOCUMENT))

    assert len(result) == 3 and result == [
        {"title": "Rules2024", "version": 1},
        {"title": "Rules2024", "version": 2},
        {"title": "Rules2024", "version": 3},
    ]


def test_delete_indexed_node(memgraph_with_text_indexed_data):
    memgraph_with_text_indexed_data.execute("""MATCH (n:Document {title: "Rules2024", version: 2}) DETACH DELETE n;""")

    result = list(memgraph_with_text_indexed_data.execute_and_fetch(GET_RULES_2024_DOCUMENT))

    assert len(result) == 1 and result == [{"title": "Rules2024", "version": 1}]


def test_add_indexed_label(memgraph_with_mixed_data):
    memgraph_with_mixed_data.execute("""MATCH (n:Revision {version:2}) SET n:Document;""")

    result = list(memgraph_with_mixed_data.execute_and_fetch(GET_RULES_2024_DOCUMENT))

    assert len(result) == 2 and result == [{"title": "Rules2024", "version": 1}, {"title": "Rules2024", "version": 2}]


def test_remove_indexed_label(memgraph_with_mixed_data):
    memgraph_with_mixed_data.execute("""MATCH (n:Document {version: 1}) REMOVE n:Document;""")

    result = list(memgraph_with_mixed_data.execute_and_fetch(GET_RULES_2024_DOCUMENT))

    assert len(result) == 0


def test_update_text_property_of_indexed_node(memgraph_with_text_indexed_data):
    memgraph_with_text_indexed_data.execute("""MATCH (n:Document {version:1}) SET n.title = "Rules2030";""")

    result = list(
        memgraph_with_text_indexed_data.execute_and_fetch(
            """CALL text_search.search("complianceDocuments", "data.title:Rules2030") YIELD node
             RETURN node.title AS title, node.version AS version
             ORDER BY version ASC, title ASC;"""
        )
    )

    assert len(result) == 1 and result == [{"title": "Rules2030", "version": 1}]


def test_add_unindexable_property_to_indexed_node(memgraph_with_text_indexed_data):
    try:
        memgraph_with_text_indexed_data.execute("""MATCH (n:Document {version:1}) SET n.randomList = [2, 3, 4, 5];""")
    except Exception:
        assert False


def test_remove_indexable_property_from_indexed_node(memgraph_with_text_indexed_data):
    try:
        memgraph_with_text_indexed_data.execute(
            """MATCH (n:Document {version:1}) REMOVE n.title, n.version, n.fulltext, n.date;"""
        )
    except Exception:
        assert False


def test_remove_unindexable_property_from_indexed_node(memgraph_with_text_indexed_data):
    try:
        memgraph_with_text_indexed_data.execute_and_fetch(
            """MATCH (n:Document {date: date("2023-12-15")}) REMOVE n.date;"""
        )
    except Exception:
        assert False


def test_text_search_nonexistent_index(memgraph_with_text_indexed_data):
    NONEXISTENT_INDEX_QUERY = """CALL text_search.search("noSuchIndex", "data.fulltext:words") YIELD node
                RETURN node.title AS title, node.version AS version
                ORDER BY version ASC, title ASC;"""

    with pytest.raises(mgclient.DatabaseError, match='Text index "noSuchIndex" doesn’t exist.') as _:
        list(memgraph_with_text_indexed_data.execute_and_fetch(NONEXISTENT_INDEX_QUERY))


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
