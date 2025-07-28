# Copyright 2025 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import os
import sys
import tempfile

import interactive_mg_runner
import pytest
from common import execute_and_fetch_all

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))
interactive_mg_runner.MEMGRAPH_QUERY_MODULES_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.BUILD_DIR, "query_modules")
)


def test_durability_with_text_index(connection):
    # Goal: That text indices and their data are correctly restored after restart.
    # 0/ Setup the database
    # 1/ Create text index and add searchable data
    # 2/ Validate text search works
    # 3/ Kill MAIN
    # 4/ Start MAIN
    # 5/ Validate text index and data are restored
    # 6/ Validate text search still works

    data_directory = tempfile.TemporaryDirectory()

    MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL = {
        "main": {
            "args": [
                "--experimental-enabled=text-search",
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
                "--query-modules-directory",
                interactive_mg_runner.MEMGRAPH_QUERY_MODULES_DIR,
            ],
            "log_file": "main_durability_with_text_index.log",
            "data_directory": data_directory.name,
        },
    }

    # 0/
    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    # 1/
    # Create text index
    execute_and_fetch_all(cursor, "CREATE TEXT INDEX document_index ON :Document;")

    # Add data that will be indexed
    execute_and_fetch_all(
        cursor, "CREATE (:Document {title: 'Technical Documentation', content: 'Memgraph is a graph database'});"
    )
    execute_and_fetch_all(
        cursor, "CREATE (:Document {title: 'User Manual', content: 'How to use the text search functionality'});"
    )

    # 2/
    def get_text_index_info(cursor):
        return execute_and_fetch_all(cursor, "SHOW INDEX INFO;")

    def search_documents(cursor, query):
        return execute_and_fetch_all(
            cursor,
            f"CALL text_search.search('document_index', '{query}') YIELD node RETURN node.title AS title ORDER BY title;",
        )

    # Validate text index exists
    index_info = get_text_index_info(cursor)
    assert len(index_info) == 1
    assert index_info[0][0] == "text (name: document_index)"
    assert index_info[0][1] == "Document"

    # Validate text search works before restart
    search_results = search_documents(cursor, "data.content:database")
    assert len(search_results) == 1
    assert search_results[0][0] == "Technical Documentation"

    search_results = search_documents(cursor, "data.content:text")
    assert len(search_results) == 1
    assert search_results[0][0] == "User Manual"

    search_results = search_documents(cursor, "data.content:Memgraph")
    assert len(search_results) == 1
    assert search_results[0][0] == "Technical Documentation"

    # 3/
    interactive_mg_runner.kill(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")

    # 4/
    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    # 5/
    # Validate text index is restored
    index_info = get_text_index_info(cursor)
    assert len(index_info) == 1
    assert index_info[0][0] == "text (name: document_index)"
    assert index_info[0][1] == "Document"

    # Validate all documents are restored
    all_documents = execute_and_fetch_all(cursor, "MATCH (d:Document) RETURN d.title ORDER BY d.title;")
    assert len(all_documents) == 2
    assert all_documents == [("Technical Documentation",), ("User Manual",)]

    # 6/
    # Validate text search still works after restart
    search_results = search_documents(cursor, "data.content:database")
    assert len(search_results) == 1
    assert search_results[0][0] == "Technical Documentation"

    search_results = search_documents(cursor, "data.content:text")
    assert len(search_results) == 1
    assert search_results[0][0] == "User Manual"

    search_results = search_documents(cursor, "data.content:Memgraph")
    assert len(search_results) == 1
    assert search_results[0][0] == "Technical Documentation"

    # Additional test: Verify we can add new data and search it after restart
    execute_and_fetch_all(
        cursor, "CREATE (:Document {title: 'New Guide', content: 'Post-restart text indexing test'});"
    )

    search_results = search_documents(cursor, "data.content:indexing")
    assert len(search_results) == 1
    assert search_results[0][0] == "New Guide"

    interactive_mg_runner.stop(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")


def test_durability_with_text_index_recovery_disabled(connection):
    # Goal: When data recovery is disabled, text indices should not be restored and new empty indices should work.
    # 0/ Setup the database with recovery enabled
    # 1/ Create text index and add searchable data
    # 2/ Kill MAIN
    # 3/ Start MAIN with recovery disabled
    # 4/ Validate text index is not restored
    # 5/ Create new empty text index
    # 6/ Validate search returns empty results
    # 7/ Add new data and validate it can be found

    data_directory = tempfile.TemporaryDirectory()

    # First, create data with recovery enabled
    MEMGRAPH_INSTANCE_DESCRIPTION_WITHOUT_RECOVERY = {
        "main": {
            "args": [
                "--experimental-enabled=text-search",
                "--log-level=TRACE",
                "--data-recovery-on-startup=false",
                "--query-modules-directory",
                interactive_mg_runner.MEMGRAPH_QUERY_MODULES_DIR,
            ],
            "log_file": "main_durability_text_index_recovery_disabled_setup.log",
            "data_directory": data_directory.name,
        },
    }

    # 0/
    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_WITHOUT_RECOVERY, "main")
    cursor = connection(7687, "main").cursor()

    # 1/
    # Create text index and add data
    execute_and_fetch_all(cursor, "CREATE TEXT INDEX document_index ON :Document;")
    execute_and_fetch_all(
        cursor, "CREATE (:Document {title: 'Old Document', content: 'This should not be recovered'});"
    )

    # Verify data exists before restart
    search_results = execute_and_fetch_all(
        cursor, "CALL text_search.search('document_index', 'data.content:recovered') YIELD node RETURN node.title;"
    )
    assert len(search_results) == 1
    assert search_results[0][0] == "Old Document"

    # 2/
    interactive_mg_runner.kill(MEMGRAPH_INSTANCE_DESCRIPTION_WITHOUT_RECOVERY, "main")

    # 3/
    # Start with recovery disabled
    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_WITHOUT_RECOVERY, "main")
    cursor = connection(7687, "main").cursor()

    # 4/
    # Validate text index is not restored
    index_info = execute_and_fetch_all(cursor, "SHOW INDEX INFO;")
    assert len(index_info) == 0  # No indices should exist

    # Validate old documents are not restored
    all_documents = execute_and_fetch_all(cursor, "MATCH (d:Document) RETURN d.title;")
    assert len(all_documents) == 0  # No documents should exist

    # 5/
    # Create new empty text index
    execute_and_fetch_all(cursor, "CREATE TEXT INDEX new_document_index ON :Document;")

    # Verify the new index exists
    index_info = execute_and_fetch_all(cursor, "SHOW INDEX INFO;")
    assert len(index_info) == 1
    assert index_info[0][0] == "text (name: new_document_index)"
    assert index_info[0][1] == "Document"

    # 6/
    # Validate search returns empty results (no documents to index)
    search_results = execute_and_fetch_all(
        cursor, "CALL text_search.search('new_document_index', 'data.content:anything') YIELD node RETURN node.title;"
    )
    assert len(search_results) == 0  # Should be empty since no documents exist

    # 7/
    # Add new data and validate it can be found
    execute_and_fetch_all(cursor, "CREATE (:Document {title: 'New Document', content: 'Fresh content after restart'});")

    search_results = execute_and_fetch_all(
        cursor, "CALL text_search.search('new_document_index', 'data.content:Fresh') YIELD node RETURN node.title;"
    )
    assert len(search_results) == 1
    assert search_results[0][0] == "New Document"

    # Ensure old data is not accessible
    search_results = execute_and_fetch_all(
        cursor, "CALL text_search.search('new_document_index', 'data.content:recovered') YIELD node RETURN node.title;"
    )
    assert len(search_results) == 0  # Old data should not be found

    interactive_mg_runner.stop(MEMGRAPH_INSTANCE_DESCRIPTION_WITHOUT_RECOVERY, "main")


def test_durability_with_text_index_experimental_disabled_to_enabled(connection):
    # Goal: Test that when text-search is disabled initially, then enabled after snapshot recovery,
    # existing nodes can be indexed by newly created text indices.
    # 0/ Setup the database with text-search disabled
    # 1/ Create nodes without text index
    # 2/ Create snapshot
    # 3/ Kill MAIN
    # 4/ Start MAIN with text-search enabled and recovery from snapshot
    # 5/ Create text index on existing label
    # 6/ Validate existing nodes are found in the new index

    data_directory = tempfile.TemporaryDirectory()

    # First, start without text-search experimental flag
    MEMGRAPH_INSTANCE_DESCRIPTION_WITHOUT_TEXT_SEARCH = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
                "--query-modules-directory",
                interactive_mg_runner.MEMGRAPH_QUERY_MODULES_DIR,
            ],
            "log_file": "main_durability_text_index_experimental_disabled.log",
            "data_directory": data_directory.name,
        },
    }

    # 0/
    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_WITHOUT_TEXT_SEARCH, "main")
    cursor = connection(7687, "main").cursor()

    # 1/
    # Create nodes that will later be indexed
    execute_and_fetch_all(
        cursor,
        "CREATE (:Article {title: 'Database Systems', content: 'Introduction to graph databases and their applications'});",
    )
    execute_and_fetch_all(
        cursor,
        "CREATE (:Article {title: 'Query Languages', content: 'Cypher query language for graph database operations'});",
    )

    # Verify nodes exist
    all_articles = execute_and_fetch_all(cursor, "MATCH (a:Article) RETURN a.title ORDER BY a.title;")
    assert len(all_articles) == 2
    assert all_articles == [("Database Systems",), ("Query Languages",)]

    # 2/
    # Create snapshot
    execute_and_fetch_all(cursor, "CREATE SNAPSHOT;")

    # 3/
    interactive_mg_runner.kill(MEMGRAPH_INSTANCE_DESCRIPTION_WITHOUT_TEXT_SEARCH, "main")

    # 4/
    # Start with text-search enabled and recovery from snapshot
    MEMGRAPH_INSTANCE_DESCRIPTION_WITH_TEXT_SEARCH = {
        "main": {
            "args": [
                "--experimental-enabled=text-search",
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
                "--query-modules-directory",
                interactive_mg_runner.MEMGRAPH_QUERY_MODULES_DIR,
            ],
            "log_file": "main_durability_text_index_experimental_enabled.log",
            "data_directory": data_directory.name,
        },
    }

    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_WITH_TEXT_SEARCH, "main")
    cursor = connection(7687, "main").cursor()

    # Verify nodes were recovered from snapshot
    all_articles = execute_and_fetch_all(cursor, "MATCH (a:Article) RETURN a.title ORDER BY a.title;")
    assert len(all_articles) == 2
    assert all_articles == [("Database Systems",), ("Query Languages",)]

    # 5/
    # Create text index on the existing label
    execute_and_fetch_all(cursor, "CREATE TEXT INDEX article_index ON :Article;")

    # Verify the index exists
    index_info = execute_and_fetch_all(cursor, "SHOW INDEX INFO;")
    assert len(index_info) == 1
    assert index_info[0][0] == "text (name: article_index)"
    assert index_info[0][1] == "Article"

    # 6/
    # Validate existing nodes are found in the new index
    def search_articles(cursor, query):
        return execute_and_fetch_all(
            cursor,
            f"CALL text_search.search('article_index', '{query}') YIELD node RETURN node.title AS title ORDER BY title;",
        )

    # Search for content from first article
    search_results = search_articles(cursor, "data.content:graph")
    assert len(search_results) == 2  # Both articles contain "graph"
    assert search_results == [("Database Systems",), ("Query Languages",)]

    # Search for content specific to first article
    search_results = search_articles(cursor, "data.content:Introduction")
    assert len(search_results) == 1
    assert search_results[0][0] == "Database Systems"

    # Search for content specific to second article
    search_results = search_articles(cursor, "data.content:Cypher")
    assert len(search_results) == 1
    assert search_results[0][0] == "Query Languages"

    # Search by title
    search_results = search_articles(cursor, "data.title:Database")
    assert len(search_results) == 1
    assert search_results[0][0] == "Database Systems"

    interactive_mg_runner.stop(MEMGRAPH_INSTANCE_DESCRIPTION_WITH_TEXT_SEARCH, "main")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
