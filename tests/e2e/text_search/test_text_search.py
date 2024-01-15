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
from common import memgraph, memgraph_with_text_indexed_data


def test_create_index(memgraph):
    memgraph.execute("CREATE TEXT INDEX complianceDocuments ON :Document;")

    assert True


def test_drop_index(memgraph):
    memgraph.execute("DROP TEXT INDEX complianceDocuments;")
    assert True


def test_text_search_given_property(memgraph_with_text_indexed_data):
    memgraph_with_text_indexed_data.execute_and_fetch("CALL text_search.search('complianceDocuments', 'b') YIELD *;")


def test_text_search_all_properties(memgraph_with_text_indexed_data):
    memgraph_with_text_indexed_data.execute_and_fetch("CALL text_search.search('complianceDocuments', 'b') YIELD *;")


def test_create_indexed_node(memgraph_with_text_indexed_data):
    memgraph_with_text_indexed_data.execute_and_fetch("CALL text_search.search('complianceDocuments', 'b') YIELD *;")


def test_delete_indexed_node(memgraph_with_text_indexed_data):
    memgraph_with_text_indexed_data.execute_and_fetch("CALL text_search.search('complianceDocuments', 'b') YIELD *;")


def test_add_indexed_label(memgraph_with_text_indexed_data):
    memgraph_with_text_indexed_data.execute_and_fetch("CALL text_search.search('complianceDocuments', 'b') YIELD *;")


def test_remove_indexed_label(memgraph_with_text_indexed_data):
    memgraph_with_text_indexed_data.execute_and_fetch("CALL text_search.search('complianceDocuments', 'b') YIELD *;")


def test_add_property_to_indexed_node(memgraph_with_text_indexed_data):
    memgraph_with_text_indexed_data.execute_and_fetch("CALL text_search.search('complianceDocuments', 'b') YIELD *;")


def test_remove_property_from_indexed_node(memgraph_with_text_indexed_data):
    memgraph_with_text_indexed_data.execute_and_fetch("CALL text_search.search('complianceDocuments', 'b') YIELD *;")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
