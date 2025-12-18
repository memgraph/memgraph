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
    # Goal: That text indices (both node and edge) and their data are correctly restored after restart.
    # 0/ Setup the database
    # 1/ Create text indices on both nodes and edges, add searchable data
    # 2/ Validate text search works for both nodes and edges
    # 3/ Kill MAIN
    # 4/ Start MAIN
    # 5/ Validate text indices and data are restored
    # 6/ Validate text search still works for both nodes and edges

    data_directory = tempfile.TemporaryDirectory()

    MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL = {
        "main": {
            "args": [
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
    # Create text indices on both nodes and edges
    execute_and_fetch_all(cursor, "CREATE TEXT INDEX document_index ON :Document;")
    execute_and_fetch_all(cursor, "CREATE TEXT EDGE INDEX relation_index ON :RELATES_TO(title, content);")

    # Add data that will be indexed (both nodes and edges)
    execute_and_fetch_all(
        cursor,
        """CREATE (d1:Document {title: 'Technical Documentation', content: 'Memgraph is a graph database'})
           -[:RELATES_TO {title: 'Technical Link', content: 'Database connection and usage details'}]->
           (d2:Document {title: 'User Manual', content: 'How to use the text search functionality'});""",
    )

    # 2/
    def get_text_index_info(cursor):
        return execute_and_fetch_all(cursor, "SHOW INDEX INFO;")

    def search_documents(cursor, query):
        return execute_and_fetch_all(
            cursor,
            f"CALL text_search.search('document_index', '{query}') YIELD node RETURN node.title AS title ORDER BY title;",
        )

    def search_relations(cursor, query):
        return execute_and_fetch_all(
            cursor,
            f"CALL text_search.search_edges('relation_index', '{query}') YIELD edge RETURN edge.title AS title ORDER BY title;",
        )

    # Validate text indices exist
    index_info = get_text_index_info(cursor)
    assert len(index_info) == 2
    assert sorted(index_info) == sorted(
        [
            ("label_text (name: document_index)", "Document", [], 2),
            ("edge-type_text (name: relation_index)", "RELATES_TO", ["title", "content"], 1),
        ]
    )

    # Validate text search works before restart - nodes
    search_results = search_documents(cursor, "data.content:database")
    assert len(search_results) == 1
    assert search_results[0][0] == "Technical Documentation"

    search_results = search_documents(cursor, "data.content:text")
    assert len(search_results) == 1
    assert search_results[0][0] == "User Manual"

    search_results = search_documents(cursor, "data.content:Memgraph")
    assert len(search_results) == 1
    assert search_results[0][0] == "Technical Documentation"

    # Validate text search works before restart - edges
    search_results = search_relations(cursor, "data.title:Technical")
    assert len(search_results) == 1
    assert search_results[0][0] == "Technical Link"

    search_results = search_relations(cursor, "data.content:Database")
    assert len(search_results) == 1
    assert search_results[0][0] == "Technical Link"

    search_results = search_relations(cursor, "data.content:usage")
    assert len(search_results) == 1
    assert search_results[0][0] == "Technical Link"

    # 3/
    interactive_mg_runner.kill(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")

    # 4/
    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connection(7687, "main").cursor()

    # 5/
    # Validate text indices are restored
    index_info = get_text_index_info(cursor)
    assert len(index_info) == 2
    assert sorted(index_info) == sorted(
        [
            ("label_text (name: document_index)", "Document", [], 2),
            ("edge-type_text (name: relation_index)", "RELATES_TO", ["title", "content"], 1),
        ]
    )

    # Validate all nodes are restored
    all_documents = execute_and_fetch_all(cursor, "MATCH (d:Document) RETURN d.title ORDER BY d.title;")
    assert len(all_documents) == 2
    assert all_documents == [("Technical Documentation",), ("User Manual",)]

    # Validate all edges are restored
    all_relations = execute_and_fetch_all(cursor, "MATCH ()-[r:RELATES_TO]->() RETURN r.title ORDER BY r.title;")
    assert len(all_relations) == 1
    assert all_relations == [("Technical Link",)]

    # 6/
    # Validate text search still works after restart - nodes
    search_results = search_documents(cursor, "data.content:database")
    assert len(search_results) == 1
    assert search_results[0][0] == "Technical Documentation"

    search_results = search_documents(cursor, "data.content:text")
    assert len(search_results) == 1
    assert search_results[0][0] == "User Manual"

    search_results = search_documents(cursor, "data.content:Memgraph")
    assert len(search_results) == 1
    assert search_results[0][0] == "Technical Documentation"

    # Validate text search still works after restart - edges
    search_results = search_relations(cursor, "data.title:Technical")
    assert len(search_results) == 1
    assert search_results[0][0] == "Technical Link"

    search_results = search_relations(cursor, "data.content:Database")
    assert len(search_results) == 1
    assert search_results[0][0] == "Technical Link"

    search_results = search_relations(cursor, "data.content:usage")
    assert len(search_results) == 1
    assert search_results[0][0] == "Technical Link"

    # Additional test: Verify we can add new data and search it after restart
    execute_and_fetch_all(
        cursor,
        """CREATE (d3:Document {title: 'New Guide', content: 'Post-restart text indexing test'})
           -[:RELATES_TO {title: 'New Link', content: 'Post-restart edge indexing test'}]->
           (d4:Document {title: 'Target Doc', content: 'Target document content'});""",
    )

    # Test new node data
    search_results = search_documents(cursor, "data.content:indexing")
    assert len(search_results) == 1
    assert search_results[0][0] == "New Guide"

    # Test new edge data
    search_results = search_relations(cursor, "data.content:edge")
    assert len(search_results) == 1
    assert search_results[0][0] == "New Link"

    interactive_mg_runner.stop(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")


def test_durability_with_text_index_recovery_disabled(connection):
    # Goal: When data recovery is disabled, text indices (both node and edge) should not be restored and new empty indices should work.
    # 0/ Setup the database with recovery enabled
    # 1/ Create text indices on both nodes and edges, add searchable data
    # 2/ Kill MAIN
    # 3/ Start MAIN with recovery disabled
    # 4/ Validate text indices are not restored
    # 5/ Create new empty text indices
    # 6/ Validate search returns empty results
    # 7/ Add new data and validate it can be found

    data_directory = tempfile.TemporaryDirectory()

    # First, create data with recovery enabled
    MEMGRAPH_INSTANCE_DESCRIPTION_WITHOUT_RECOVERY = {
        "main": {
            "args": [
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
    # Create text indices on both nodes and edges, add data
    execute_and_fetch_all(cursor, "CREATE TEXT INDEX document_index ON :Document;")
    execute_and_fetch_all(cursor, "CREATE TEXT EDGE INDEX relation_index ON :RELATES_TO(title, content);")
    execute_and_fetch_all(
        cursor,
        """CREATE (d1:Document {title: 'Old Document', content: 'This should not be recovered'})
           -[:RELATES_TO {title: 'Old Relation', content: 'This edge should not be recovered'}]->
           (d2:Document {title: 'Old Target', content: 'Old target content'});""",
    )

    # Verify data exists before restart - nodes
    search_results = execute_and_fetch_all(
        cursor, "CALL text_search.search('document_index', 'data.content:recovered') YIELD node RETURN node.title;"
    )
    assert len(search_results) == 1
    assert search_results[0][0] == "Old Document"

    # Verify data exists before restart - edges
    search_results = execute_and_fetch_all(
        cursor,
        "CALL text_search.search_edges('relation_index', 'data.content:recovered') YIELD edge RETURN edge.title;",
    )
    assert len(search_results) == 1
    assert search_results[0][0] == "Old Relation"

    # 2/
    interactive_mg_runner.kill(MEMGRAPH_INSTANCE_DESCRIPTION_WITHOUT_RECOVERY, "main")

    # 3/
    # Start with recovery disabled
    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_WITHOUT_RECOVERY, "main")
    cursor = connection(7687, "main").cursor()

    # 4/
    # Validate text indices are not restored
    index_info = execute_and_fetch_all(cursor, "SHOW INDEX INFO;")
    assert len(index_info) == 0  # No indices should exist

    # Validate old nodes are not restored
    all_documents = execute_and_fetch_all(cursor, "MATCH (d:Document) RETURN d.title;")
    assert len(all_documents) == 0  # No documents should exist

    # Validate old edges are not restored
    all_relations = execute_and_fetch_all(cursor, "MATCH ()-[r:RELATES_TO]->() RETURN r.title;")
    assert len(all_relations) == 0  # No relations should exist

    # 5/
    # Create new empty text indices
    execute_and_fetch_all(cursor, "CREATE TEXT INDEX new_document_index ON :Document;")
    execute_and_fetch_all(cursor, "CREATE TEXT EDGE INDEX new_relation_index ON :RELATES_TO(title, content);")

    # Verify the new indices exist
    index_info = execute_and_fetch_all(cursor, "SHOW INDEX INFO;")
    assert len(index_info) == 2
    assert sorted(index_info) == sorted(
        [
            ("label_text (name: new_document_index)", "Document", [], 0),
            ("edge-type_text (name: new_relation_index)", "RELATES_TO", ["title", "content"], 0),
        ]
    )

    # 6/
    # Validate search returns empty results (no data to index)
    search_results = execute_and_fetch_all(
        cursor, "CALL text_search.search('new_document_index', 'data.content:anything') YIELD node RETURN node.title;"
    )
    assert len(search_results) == 0  # Should be empty since no documents exist

    search_results = execute_and_fetch_all(
        cursor,
        "CALL text_search.search_edges('new_relation_index', 'data.content:anything') YIELD edge RETURN edge.title;",
    )
    assert len(search_results) == 0  # Should be empty since no edges exist

    # 7/
    # Add new data and validate it can be found
    execute_and_fetch_all(
        cursor,
        """CREATE (d1:Document {title: 'New Document', content: 'Fresh content after restart'})
           -[:RELATES_TO {title: 'New Relation', content: 'Fresh edge content after restart'}]->
           (d2:Document {title: 'New Target', content: 'New target content'});""",
    )

    # Test new node data
    search_results = execute_and_fetch_all(
        cursor, "CALL text_search.search('new_document_index', 'data.content:Fresh') YIELD node RETURN node.title;"
    )
    assert len(search_results) == 1
    assert search_results[0][0] == "New Document"

    # Test new edge data
    search_results = execute_and_fetch_all(
        cursor, "CALL text_search.search_edges('new_relation_index', 'data.title:New') YIELD edge RETURN edge.title;"
    )
    assert len(search_results) == 1
    assert search_results[0][0] == "New Relation"

    # Ensure old data is not accessible - nodes
    search_results = execute_and_fetch_all(
        cursor, "CALL text_search.search('new_document_index', 'data.content:recovered') YIELD node RETURN node.title;"
    )
    assert len(search_results) == 0  # Old node data should not be found

    # Ensure old data is not accessible - edges
    search_results = execute_and_fetch_all(
        cursor,
        "CALL text_search.search_edges('new_relation_index', 'data.content:recovered') YIELD edge RETURN edge.title;",
    )
    assert len(search_results) == 0  # Old edge data should not be found

    interactive_mg_runner.stop(MEMGRAPH_INSTANCE_DESCRIPTION_WITHOUT_RECOVERY, "main")


def test_durability_text_index_recovery_from_snapshot_only(connection):
    # Goal: Test that text indices are correctly rebuilt when text_indices folder is deleted but snapshot exists.
    # This simulates a scenario where the text index files are corrupted/missing but the data is in the snapshot.
    # 0/ Setup the database
    # 1/ Create text indices on both nodes and edges, add searchable data
    # 2/ Validate text search works for both nodes and edges
    # 3/ Kill MAIN
    # 4/ Delete the text_indices folder (simulating corruption/missing files)
    # 5/ Start MAIN (recovery from snapshot should rebuild the indices)
    # 6/ Validate text indices exist and data is searchable

    import shutil

    data_directory = tempfile.TemporaryDirectory()
    text_indices_path = os.path.join(data_directory.name, "text_indices")

    MEMGRAPH_INSTANCE_DESCRIPTION = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
                "--query-modules-directory",
                interactive_mg_runner.MEMGRAPH_QUERY_MODULES_DIR,
            ],
            "log_file": "main_durability_text_index_snapshot_recovery.log",
            "data_directory": data_directory.name,
        },
    }

    # 0/
    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION, "main")
    cursor = connection(7687, "main").cursor()

    # 1/
    # Create text indices on both nodes and edges
    execute_and_fetch_all(cursor, "CREATE TEXT INDEX article_index ON :Article;")
    execute_and_fetch_all(cursor, "CREATE TEXT EDGE INDEX reference_index ON :REFERENCES(description, notes);")

    # Add data that will be indexed (both nodes and edges)
    execute_and_fetch_all(
        cursor,
        """CREATE (a1:Article {title: 'Article One', content: 'alpha'})
           -[:REFERENCES {description: 'Link One', notes: 'first'}]->
           (a2:Article {title: 'Article Two', content: 'beta'});""",
    )
    execute_and_fetch_all(
        cursor,
        """CREATE (a3:Article {title: 'Article Three', content: 'gamma'})
           -[:REFERENCES {description: 'Link Two', notes: 'second'}]->
           (a4:Article {title: 'Article Four', content: 'delta'});""",
    )

    # 2/
    def search_articles(cursor, query):
        return execute_and_fetch_all(
            cursor,
            f"CALL text_search.search('article_index', '{query}') YIELD node RETURN node.title AS title ORDER BY title;",
        )

    def search_references(cursor, query):
        return execute_and_fetch_all(
            cursor,
            f"CALL text_search.search_edges('reference_index', '{query}') YIELD edge RETURN edge.description AS description ORDER BY description;",
        )

    # Validate text indices exist
    index_info = execute_and_fetch_all(cursor, "SHOW INDEX INFO;")
    assert len(index_info) == 2
    assert sorted(index_info) == sorted(
        [
            ("label_text (name: article_index)", "Article", [], 4),
            ("edge-type_text (name: reference_index)", "REFERENCES", ["description", "notes"], 2),
        ]
    )

    # Validate text search works before restart - nodes
    search_results = search_articles(cursor, "data.content:alpha")
    assert len(search_results) == 1
    assert search_results[0][0] == "Article One"

    search_results = search_articles(cursor, "data.content:gamma")
    assert len(search_results) == 1
    assert search_results[0][0] == "Article Three"

    # Validate text search works before restart - edges
    search_results = search_references(cursor, "data.notes:first")
    assert len(search_results) == 1
    assert search_results[0][0] == "Link One"

    search_results = search_references(cursor, "data.notes:second")
    assert len(search_results) == 1
    assert search_results[0][0] == "Link Two"

    # 3/
    interactive_mg_runner.kill(MEMGRAPH_INSTANCE_DESCRIPTION, "main")

    # 4/
    # Delete the text_indices folder to simulate missing/corrupted index files
    if os.path.exists(text_indices_path):
        shutil.rmtree(text_indices_path)
    assert not os.path.exists(text_indices_path), "text_indices folder should be deleted"

    # 5/
    # Start MAIN - recovery from snapshot should rebuild the text indices
    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION, "main")
    cursor = connection(7687, "main").cursor()

    # 6/
    # Validate text indices are restored
    index_info = execute_and_fetch_all(cursor, "SHOW INDEX INFO;")
    assert len(index_info) == 2
    assert sorted(index_info) == sorted(
        [
            ("label_text (name: article_index)", "Article", [], 4),
            ("edge-type_text (name: reference_index)", "REFERENCES", ["description", "notes"], 2),
        ]
    )

    # Validate the text_indices folder was recreated
    assert os.path.exists(text_indices_path), "text_indices folder should be recreated after recovery"

    # Validate all nodes are restored
    all_articles = execute_and_fetch_all(cursor, "MATCH (a:Article) RETURN a.title ORDER BY a.title;")
    assert len(all_articles) == 4
    assert all_articles == [
        ("Article Four",),
        ("Article One",),
        ("Article Three",),
        ("Article Two",),
    ]

    # Validate all edges are restored
    all_references = execute_and_fetch_all(
        cursor, "MATCH ()-[r:REFERENCES]->() RETURN r.description ORDER BY r.description;"
    )
    assert len(all_references) == 2
    assert all_references == [("Link One",), ("Link Two",)]

    # Validate text search works after recovery - nodes
    search_results = search_articles(cursor, "data.content:alpha")
    assert len(search_results) == 1
    assert search_results[0][0] == "Article One"

    search_results = search_articles(cursor, "data.content:beta")
    assert len(search_results) == 1
    assert search_results[0][0] == "Article Two"

    search_results = search_articles(cursor, "data.content:gamma")
    assert len(search_results) == 1
    assert search_results[0][0] == "Article Three"

    search_results = search_articles(cursor, "data.content:delta")
    assert len(search_results) == 1
    assert search_results[0][0] == "Article Four"

    # Validate text search works after recovery - edges
    search_results = search_references(cursor, "data.notes:first")
    assert len(search_results) == 1
    assert search_results[0][0] == "Link One"

    search_results = search_references(cursor, "data.notes:second")
    assert len(search_results) == 1
    assert search_results[0][0] == "Link Two"

    interactive_mg_runner.stop(MEMGRAPH_INSTANCE_DESCRIPTION, "main")


def test_partial_text_index_folder_deletion_nodes(connection):
    # Goal: Test that when one of two node text index folders is deleted, both indices are recovered from snapshot.
    # 0/ Setup the database
    # 1/ Create two text indices on different node labels
    # 2/ Add one node to each index
    # 3/ Kill MAIN (snapshot is created)
    # 4/ Delete only ONE text index folder
    # 5/ Start MAIN (recovery from snapshot)
    # 6/ Validate both indices exist and are searchable

    import shutil

    data_directory = tempfile.TemporaryDirectory()
    text_indices_path = os.path.join(data_directory.name, "text_indices")

    MEMGRAPH_INSTANCE_DESCRIPTION = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
                "--query-modules-directory",
                interactive_mg_runner.MEMGRAPH_QUERY_MODULES_DIR,
            ],
            "log_file": "main_partial_text_index_deletion_nodes.log",
            "data_directory": data_directory.name,
        },
    }

    # 0/
    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION, "main")
    cursor = connection(7687, "main").cursor()

    # 1/
    execute_and_fetch_all(cursor, "CREATE TEXT INDEX index_one ON :LabelOne;")
    execute_and_fetch_all(cursor, "CREATE TEXT INDEX index_two ON :LabelTwo;")

    # 2/
    execute_and_fetch_all(cursor, "CREATE (:LabelOne {content: 'alpha'});")
    execute_and_fetch_all(cursor, "CREATE (:LabelTwo {content: 'beta'});")

    # Validate indices exist with correct counts
    index_info = execute_and_fetch_all(cursor, "SHOW INDEX INFO;")
    assert len(index_info) == 2
    assert sorted(index_info) == sorted(
        [
            ("label_text (name: index_one)", "LabelOne", [], 1),
            ("label_text (name: index_two)", "LabelTwo", [], 1),
        ]
    )

    # Validate search works before restart
    search_results = execute_and_fetch_all(
        cursor, "CALL text_search.search('index_one', 'data.content:alpha') YIELD node RETURN node.content;"
    )
    assert len(search_results) == 1
    assert search_results[0][0] == "alpha"

    search_results = execute_and_fetch_all(
        cursor, "CALL text_search.search('index_two', 'data.content:beta') YIELD node RETURN node.content;"
    )
    assert len(search_results) == 1
    assert search_results[0][0] == "beta"

    # 3/
    interactive_mg_runner.kill(MEMGRAPH_INSTANCE_DESCRIPTION, "main")

    # 4/ Delete only ONE text index folder
    index_one_path = os.path.join(text_indices_path, "index_one")
    assert os.path.exists(index_one_path), "index_one folder should exist before deletion"
    shutil.rmtree(index_one_path)
    assert not os.path.exists(index_one_path), "index_one folder should be deleted"
    # index_two folder should still exist
    index_two_path = os.path.join(text_indices_path, "index_two")
    assert os.path.exists(index_two_path), "index_two folder should still exist"

    # 5/
    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION, "main")
    cursor = connection(7687, "main").cursor()

    # 6/ Validate both indices exist
    index_info = execute_and_fetch_all(cursor, "SHOW INDEX INFO;")
    assert len(index_info) == 2
    assert sorted(index_info) == sorted(
        [
            ("label_text (name: index_one)", "LabelOne", [], 1),
            ("label_text (name: index_two)", "LabelTwo", [], 1),
        ]
    )

    # Validate index_one folder was recreated
    assert os.path.exists(index_one_path), "index_one folder should be recreated after recovery"

    # Validate search works for both indices after recovery
    search_results = execute_and_fetch_all(
        cursor, "CALL text_search.search('index_one', 'data.content:alpha') YIELD node RETURN node.content;"
    )
    assert len(search_results) == 1
    assert search_results[0][0] == "alpha"

    search_results = execute_and_fetch_all(
        cursor, "CALL text_search.search('index_two', 'data.content:beta') YIELD node RETURN node.content;"
    )
    assert len(search_results) == 1
    assert search_results[0][0] == "beta"

    interactive_mg_runner.stop(MEMGRAPH_INSTANCE_DESCRIPTION, "main")


def test_partial_text_index_folder_deletion_edges(connection):
    # Goal: Test that when one of two edge text index folders is deleted, both indices are recovered from snapshot.
    # 0/ Setup the database
    # 1/ Create two text indices on different edge types
    # 2/ Add one edge to each index
    # 3/ Kill MAIN (snapshot is created)
    # 4/ Delete only ONE text index folder
    # 5/ Start MAIN (recovery from snapshot)
    # 6/ Validate both indices exist and are searchable

    import shutil

    data_directory = tempfile.TemporaryDirectory()
    text_indices_path = os.path.join(data_directory.name, "text_indices")

    MEMGRAPH_INSTANCE_DESCRIPTION = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
                "--query-modules-directory",
                interactive_mg_runner.MEMGRAPH_QUERY_MODULES_DIR,
            ],
            "log_file": "main_partial_text_index_deletion_edges.log",
            "data_directory": data_directory.name,
        },
    }

    # 0/
    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION, "main")
    cursor = connection(7687, "main").cursor()

    # 1/
    execute_and_fetch_all(cursor, "CREATE TEXT EDGE INDEX edge_index_one ON :EDGE_ONE(content);")
    execute_and_fetch_all(cursor, "CREATE TEXT EDGE INDEX edge_index_two ON :EDGE_TWO(content);")

    # 2/
    execute_and_fetch_all(cursor, "CREATE (:Node)-[:EDGE_ONE {content: 'alpha'}]->(:Node);")
    execute_and_fetch_all(cursor, "CREATE (:Node)-[:EDGE_TWO {content: 'beta'}]->(:Node);")

    # Validate indices exist with correct counts
    index_info = execute_and_fetch_all(cursor, "SHOW INDEX INFO;")
    assert len(index_info) == 2
    assert sorted(index_info) == sorted(
        [
            ("edge-type_text (name: edge_index_one)", "EDGE_ONE", ["content"], 1),
            ("edge-type_text (name: edge_index_two)", "EDGE_TWO", ["content"], 1),
        ]
    )

    # Validate search works before restart
    search_results = execute_and_fetch_all(
        cursor, "CALL text_search.search_edges('edge_index_one', 'data.content:alpha') YIELD edge RETURN edge.content;"
    )
    assert len(search_results) == 1
    assert search_results[0][0] == "alpha"

    search_results = execute_and_fetch_all(
        cursor, "CALL text_search.search_edges('edge_index_two', 'data.content:beta') YIELD edge RETURN edge.content;"
    )
    assert len(search_results) == 1
    assert search_results[0][0] == "beta"

    # 3/
    interactive_mg_runner.kill(MEMGRAPH_INSTANCE_DESCRIPTION, "main")

    # 4/ Delete only ONE text index folder
    edge_index_one_path = os.path.join(text_indices_path, "edge_index_one")
    assert os.path.exists(edge_index_one_path), "edge_index_one folder should exist before deletion"
    shutil.rmtree(edge_index_one_path)
    assert not os.path.exists(edge_index_one_path), "edge_index_one folder should be deleted"
    # edge_index_two folder should still exist
    edge_index_two_path = os.path.join(text_indices_path, "edge_index_two")
    assert os.path.exists(edge_index_two_path), "edge_index_two folder should still exist"

    # 5/
    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION, "main")
    cursor = connection(7687, "main").cursor()

    # 6/ Validate both indices exist
    index_info = execute_and_fetch_all(cursor, "SHOW INDEX INFO;")
    assert len(index_info) == 2
    assert sorted(index_info) == sorted(
        [
            ("edge-type_text (name: edge_index_one)", "EDGE_ONE", ["content"], 1),
            ("edge-type_text (name: edge_index_two)", "EDGE_TWO", ["content"], 1),
        ]
    )

    # Validate edge_index_one folder was recreated
    assert os.path.exists(edge_index_one_path), "edge_index_one folder should be recreated after recovery"

    # Validate search works for both indices after recovery
    search_results = execute_and_fetch_all(
        cursor, "CALL text_search.search_edges('edge_index_one', 'data.content:alpha') YIELD edge RETURN edge.content;"
    )
    assert len(search_results) == 1
    assert search_results[0][0] == "alpha"

    search_results = execute_and_fetch_all(
        cursor, "CALL text_search.search_edges('edge_index_two', 'data.content:beta') YIELD edge RETURN edge.content;"
    )
    assert len(search_results) == 1
    assert search_results[0][0] == "beta"

    interactive_mg_runner.stop(MEMGRAPH_INSTANCE_DESCRIPTION, "main")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
