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
import shutil
import sys

import interactive_mg_runner
import pytest
from common import connect, execute_and_fetch_all, get_data_path

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))
interactive_mg_runner.MEMGRAPH_QUERY_MODULES_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.BUILD_DIR, "query_modules")
)

FILE = "durability_with_text_index"


@pytest.fixture(autouse=True)
def cleanup_after_test():
    yield
    interactive_mg_runner.kill_all(keep_directories=False)


@pytest.fixture
def test_name(request):
    return request.node.name


def test_durability_with_text_index(test_name):
    # Goal: Text indices (both node and edge) and their data are correctly restored after restart.
    data_directory = get_data_path(FILE, test_name)

    MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
                "--query-modules-directory",
                interactive_mg_runner.MEMGRAPH_QUERY_MODULES_DIR,
            ],
            "log_file": "main_durability_with_text_index.log",
            "data_directory": data_directory,
        },
    }

    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connect(host="localhost", port=7687).cursor()

    execute_and_fetch_all(cursor, "CREATE TEXT INDEX document_index ON :Document;")
    execute_and_fetch_all(cursor, "CREATE TEXT EDGE INDEX relation_index ON :RELATES_TO(title, content);")

    execute_and_fetch_all(
        cursor,
        """CREATE (d1:Document {title: 'Technical Documentation', content: 'Memgraph is a graph database'})
           -[:RELATES_TO {title: 'Technical Link', content: 'Database connection and usage details'}]->
           (d2:Document {title: 'User Manual', content: 'How to use the text search functionality'});""",
    )

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

    index_info = get_text_index_info(cursor)
    assert len(index_info) == 2
    assert sorted(index_info) == sorted(
        [
            ("label_text (name: document_index)", "Document", [], 2),
            ("edge-type_text (name: relation_index)", "RELATES_TO", ["title", "content"], 1),
        ]
    )

    search_results = search_documents(cursor, "data.content:database")
    assert len(search_results) == 1
    assert search_results[0][0] == "Technical Documentation"

    search_results = search_documents(cursor, "data.content:text")
    assert len(search_results) == 1
    assert search_results[0][0] == "User Manual"

    search_results = search_documents(cursor, "data.content:Memgraph")
    assert len(search_results) == 1
    assert search_results[0][0] == "Technical Documentation"

    search_results = search_relations(cursor, "data.title:Technical")
    assert len(search_results) == 1
    assert search_results[0][0] == "Technical Link"

    search_results = search_relations(cursor, "data.content:Database")
    assert len(search_results) == 1
    assert search_results[0][0] == "Technical Link"

    search_results = search_relations(cursor, "data.content:usage")
    assert len(search_results) == 1
    assert search_results[0][0] == "Technical Link"

    interactive_mg_runner.kill(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")

    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_MANUAL, "main")
    cursor = connect(host="localhost", port=7687).cursor()

    index_info = get_text_index_info(cursor)
    assert len(index_info) == 2
    assert sorted(index_info) == sorted(
        [
            ("label_text (name: document_index)", "Document", [], 2),
            ("edge-type_text (name: relation_index)", "RELATES_TO", ["title", "content"], 1),
        ]
    )

    all_documents = execute_and_fetch_all(cursor, "MATCH (d:Document) RETURN d.title ORDER BY d.title;")
    assert len(all_documents) == 2
    assert all_documents == [("Technical Documentation",), ("User Manual",)]

    all_relations = execute_and_fetch_all(cursor, "MATCH ()-[r:RELATES_TO]->() RETURN r.title ORDER BY r.title;")
    assert len(all_relations) == 1
    assert all_relations == [("Technical Link",)]

    search_results = search_documents(cursor, "data.content:database")
    assert len(search_results) == 1
    assert search_results[0][0] == "Technical Documentation"

    search_results = search_documents(cursor, "data.content:text")
    assert len(search_results) == 1
    assert search_results[0][0] == "User Manual"

    search_results = search_documents(cursor, "data.content:Memgraph")
    assert len(search_results) == 1
    assert search_results[0][0] == "Technical Documentation"

    search_results = search_relations(cursor, "data.title:Technical")
    assert len(search_results) == 1
    assert search_results[0][0] == "Technical Link"

    search_results = search_relations(cursor, "data.content:Database")
    assert len(search_results) == 1
    assert search_results[0][0] == "Technical Link"

    search_results = search_relations(cursor, "data.content:usage")
    assert len(search_results) == 1
    assert search_results[0][0] == "Technical Link"

    execute_and_fetch_all(
        cursor,
        """CREATE (d3:Document {title: 'New Guide', content: 'Post-restart text indexing test'})
           -[:RELATES_TO {title: 'New Link', content: 'Post-restart edge indexing test'}]->
           (d4:Document {title: 'Target Doc', content: 'Target document content'});""",
    )

    search_results = search_documents(cursor, "data.content:indexing")
    assert len(search_results) == 1
    assert search_results[0][0] == "New Guide"

    search_results = search_relations(cursor, "data.content:edge")
    assert len(search_results) == 1
    assert search_results[0][0] == "New Link"


def test_durability_with_text_index_recovery_disabled(test_name):
    # Goal: When data recovery is disabled, text indices should not be restored.
    data_directory = get_data_path(FILE, test_name)

    MEMGRAPH_INSTANCE_DESCRIPTION_WITHOUT_RECOVERY = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=false",
                "--query-modules-directory",
                interactive_mg_runner.MEMGRAPH_QUERY_MODULES_DIR,
            ],
            "log_file": "main_durability_text_index_recovery_disabled_setup.log",
            "data_directory": data_directory,
        },
    }

    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_WITHOUT_RECOVERY, "main")
    cursor = connect(host="localhost", port=7687).cursor()

    execute_and_fetch_all(cursor, "CREATE TEXT INDEX document_index ON :Document;")
    execute_and_fetch_all(cursor, "CREATE TEXT EDGE INDEX relation_index ON :RELATES_TO(title, content);")
    execute_and_fetch_all(
        cursor,
        """CREATE (d1:Document {title: 'Old Document', content: 'This should not be recovered'})
           -[:RELATES_TO {title: 'Old Relation', content: 'This edge should not be recovered'}]->
           (d2:Document {title: 'Old Target', content: 'Old target content'});""",
    )

    search_results = execute_and_fetch_all(
        cursor, "CALL text_search.search('document_index', 'data.content:recovered') YIELD node RETURN node.title;"
    )
    assert len(search_results) == 1
    assert search_results[0][0] == "Old Document"

    search_results = execute_and_fetch_all(
        cursor,
        "CALL text_search.search_edges('relation_index', 'data.content:recovered') YIELD edge RETURN edge.title;",
    )
    assert len(search_results) == 1
    assert search_results[0][0] == "Old Relation"

    interactive_mg_runner.kill(MEMGRAPH_INSTANCE_DESCRIPTION_WITHOUT_RECOVERY, "main")

    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION_WITHOUT_RECOVERY, "main")
    cursor = connect(host="localhost", port=7687).cursor()

    index_info = execute_and_fetch_all(cursor, "SHOW INDEX INFO;")
    assert len(index_info) == 0

    all_documents = execute_and_fetch_all(cursor, "MATCH (d:Document) RETURN d.title;")
    assert len(all_documents) == 0

    all_relations = execute_and_fetch_all(cursor, "MATCH ()-[r:RELATES_TO]->() RETURN r.title;")
    assert len(all_relations) == 0

    execute_and_fetch_all(cursor, "CREATE TEXT INDEX new_document_index ON :Document;")
    execute_and_fetch_all(cursor, "CREATE TEXT EDGE INDEX new_relation_index ON :RELATES_TO(title, content);")

    index_info = execute_and_fetch_all(cursor, "SHOW INDEX INFO;")
    assert len(index_info) == 2
    assert sorted(index_info) == sorted(
        [
            ("label_text (name: new_document_index)", "Document", [], 0),
            ("edge-type_text (name: new_relation_index)", "RELATES_TO", ["title", "content"], 0),
        ]
    )

    search_results = execute_and_fetch_all(
        cursor, "CALL text_search.search('new_document_index', 'data.content:anything') YIELD node RETURN node.title;"
    )
    assert len(search_results) == 0

    search_results = execute_and_fetch_all(
        cursor,
        "CALL text_search.search_edges('new_relation_index', 'data.content:anything') YIELD edge RETURN edge.title;",
    )
    assert len(search_results) == 0

    execute_and_fetch_all(
        cursor,
        """CREATE (d1:Document {title: 'New Document', content: 'Fresh content after restart'})
           -[:RELATES_TO {title: 'New Relation', content: 'Fresh edge content after restart'}]->
           (d2:Document {title: 'New Target', content: 'New target content'});""",
    )

    search_results = execute_and_fetch_all(
        cursor, "CALL text_search.search('new_document_index', 'data.content:Fresh') YIELD node RETURN node.title;"
    )
    assert len(search_results) == 1
    assert search_results[0][0] == "New Document"

    search_results = execute_and_fetch_all(
        cursor, "CALL text_search.search_edges('new_relation_index', 'data.title:New') YIELD edge RETURN edge.title;"
    )
    assert len(search_results) == 1
    assert search_results[0][0] == "New Relation"

    search_results = execute_and_fetch_all(
        cursor, "CALL text_search.search('new_document_index', 'data.content:recovered') YIELD node RETURN node.title;"
    )
    assert len(search_results) == 0

    search_results = execute_and_fetch_all(
        cursor,
        "CALL text_search.search_edges('new_relation_index', 'data.content:recovered') YIELD edge RETURN edge.title;",
    )
    assert len(search_results) == 0


def test_durability_text_index_recovery_from_snapshot_only(test_name):
    # Goal: Text indices are correctly rebuilt when text_indices folder is deleted but snapshot exists.
    data_directory = os.path.join(interactive_mg_runner.BUILD_DIR, get_data_path(FILE, test_name))
    text_indices_path = os.path.join(data_directory, "text_indices")

    MEMGRAPH_INSTANCE_DESCRIPTION = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
                "--query-modules-directory",
                interactive_mg_runner.MEMGRAPH_QUERY_MODULES_DIR,
            ],
            "log_file": "main_durability_text_index_snapshot_recovery.log",
            "data_directory": data_directory,
        },
    }

    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION, "main")
    cursor = connect(host="localhost", port=7687).cursor()

    execute_and_fetch_all(cursor, "CREATE TEXT INDEX article_index ON :Article;")
    execute_and_fetch_all(cursor, "CREATE TEXT EDGE INDEX reference_index ON :REFERENCES(description, notes);")

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

    index_info = execute_and_fetch_all(cursor, "SHOW INDEX INFO;")
    assert len(index_info) == 2
    assert sorted(index_info) == sorted(
        [
            ("label_text (name: article_index)", "Article", [], 4),
            ("edge-type_text (name: reference_index)", "REFERENCES", ["description", "notes"], 2),
        ]
    )

    search_results = search_articles(cursor, "data.content:alpha")
    assert len(search_results) == 1
    assert search_results[0][0] == "Article One"

    search_results = search_articles(cursor, "data.content:gamma")
    assert len(search_results) == 1
    assert search_results[0][0] == "Article Three"

    search_results = search_references(cursor, "data.notes:first")
    assert len(search_results) == 1
    assert search_results[0][0] == "Link One"

    search_results = search_references(cursor, "data.notes:second")
    assert len(search_results) == 1
    assert search_results[0][0] == "Link Two"

    interactive_mg_runner.kill(MEMGRAPH_INSTANCE_DESCRIPTION, "main")

    if os.path.exists(text_indices_path):
        shutil.rmtree(text_indices_path)
    assert not os.path.exists(text_indices_path), "text_indices folder should be deleted"

    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION, "main")
    cursor = connect(host="localhost", port=7687).cursor()

    index_info = execute_and_fetch_all(cursor, "SHOW INDEX INFO;")
    assert len(index_info) == 2
    assert sorted(index_info) == sorted(
        [
            ("label_text (name: article_index)", "Article", [], 4),
            ("edge-type_text (name: reference_index)", "REFERENCES", ["description", "notes"], 2),
        ]
    )

    all_articles = execute_and_fetch_all(cursor, "MATCH (a:Article) RETURN a.title ORDER BY a.title;")
    assert len(all_articles) == 4
    assert all_articles == [
        ("Article Four",),
        ("Article One",),
        ("Article Three",),
        ("Article Two",),
    ]

    all_references = execute_and_fetch_all(
        cursor, "MATCH ()-[r:REFERENCES]->() RETURN r.description ORDER BY r.description;"
    )
    assert len(all_references) == 2
    assert all_references == [("Link One",), ("Link Two",)]

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

    search_results = search_references(cursor, "data.notes:first")
    assert len(search_results) == 1
    assert search_results[0][0] == "Link One"

    search_results = search_references(cursor, "data.notes:second")
    assert len(search_results) == 1
    assert search_results[0][0] == "Link Two"


def test_partial_text_index_folder_deletion_nodes(test_name):
    # Goal: When one of two node text index folders is deleted, both indices are recovered from snapshot.
    data_directory = os.path.join(interactive_mg_runner.BUILD_DIR, get_data_path(FILE, test_name))
    text_indices_path = os.path.join(data_directory, "text_indices")

    MEMGRAPH_INSTANCE_DESCRIPTION = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
                "--query-modules-directory",
                interactive_mg_runner.MEMGRAPH_QUERY_MODULES_DIR,
            ],
            "log_file": "main_partial_text_index_deletion_nodes.log",
            "data_directory": data_directory,
        },
    }

    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION, "main")
    cursor = connect(host="localhost", port=7687).cursor()

    execute_and_fetch_all(cursor, "CREATE TEXT INDEX index_one ON :LabelOne;")
    execute_and_fetch_all(cursor, "CREATE TEXT INDEX index_two ON :LabelTwo;")

    execute_and_fetch_all(cursor, "CREATE (:LabelOne {content: 'alpha'});")
    execute_and_fetch_all(cursor, "CREATE (:LabelTwo {content: 'beta'});")

    index_info = execute_and_fetch_all(cursor, "SHOW INDEX INFO;")
    assert len(index_info) == 2
    assert sorted(index_info) == sorted(
        [
            ("label_text (name: index_one)", "LabelOne", [], 1),
            ("label_text (name: index_two)", "LabelTwo", [], 1),
        ]
    )

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

    interactive_mg_runner.kill(MEMGRAPH_INSTANCE_DESCRIPTION, "main")

    index_one_path = os.path.join(text_indices_path, "index_one")
    assert os.path.exists(index_one_path), "index_one folder should exist before deletion"
    shutil.rmtree(index_one_path)
    assert not os.path.exists(index_one_path), "index_one folder should be deleted"
    index_two_path = os.path.join(text_indices_path, "index_two")
    assert os.path.exists(index_two_path), "index_two folder should still exist"

    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION, "main")
    cursor = connect(host="localhost", port=7687).cursor()

    index_info = execute_and_fetch_all(cursor, "SHOW INDEX INFO;")
    assert len(index_info) == 2
    assert sorted(index_info) == sorted(
        [
            ("label_text (name: index_one)", "LabelOne", [], 1),
            ("label_text (name: index_two)", "LabelTwo", [], 1),
        ]
    )

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


def test_partial_text_index_folder_deletion_edges(test_name):
    # Goal: When one of two edge text index folders is deleted, both indices are recovered from snapshot.
    data_directory = os.path.join(interactive_mg_runner.BUILD_DIR, get_data_path(FILE, test_name))
    text_indices_path = os.path.join(data_directory, "text_indices")

    MEMGRAPH_INSTANCE_DESCRIPTION = {
        "main": {
            "args": [
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
                "--query-modules-directory",
                interactive_mg_runner.MEMGRAPH_QUERY_MODULES_DIR,
            ],
            "log_file": "main_partial_text_index_deletion_edges.log",
            "data_directory": data_directory,
        },
    }

    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION, "main")
    cursor = connect(host="localhost", port=7687).cursor()

    execute_and_fetch_all(cursor, "CREATE TEXT EDGE INDEX edge_index_one ON :EDGE_ONE(content);")
    execute_and_fetch_all(cursor, "CREATE TEXT EDGE INDEX edge_index_two ON :EDGE_TWO(content);")

    execute_and_fetch_all(cursor, "CREATE (:Node)-[:EDGE_ONE {content: 'alpha'}]->(:Node);")
    execute_and_fetch_all(cursor, "CREATE (:Node)-[:EDGE_TWO {content: 'beta'}]->(:Node);")

    index_info = execute_and_fetch_all(cursor, "SHOW INDEX INFO;")
    assert len(index_info) == 2
    assert sorted(index_info) == sorted(
        [
            ("edge-type_text (name: edge_index_one)", "EDGE_ONE", ["content"], 1),
            ("edge-type_text (name: edge_index_two)", "EDGE_TWO", ["content"], 1),
        ]
    )

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

    interactive_mg_runner.kill(MEMGRAPH_INSTANCE_DESCRIPTION, "main")

    edge_index_one_path = os.path.join(text_indices_path, "edge_index_one")
    assert os.path.exists(edge_index_one_path), "edge_index_one folder should exist before deletion"
    shutil.rmtree(edge_index_one_path)
    assert not os.path.exists(edge_index_one_path), "edge_index_one folder should be deleted"
    edge_index_two_path = os.path.join(text_indices_path, "edge_index_two")
    assert os.path.exists(edge_index_two_path), "edge_index_two folder should still exist"

    interactive_mg_runner.start(MEMGRAPH_INSTANCE_DESCRIPTION, "main")
    cursor = connect(host="localhost", port=7687).cursor()

    index_info = execute_and_fetch_all(cursor, "SHOW INDEX INFO;")
    assert len(index_info) == 2
    assert sorted(index_info) == sorted(
        [
            ("edge-type_text (name: edge_index_one)", "EDGE_ONE", ["content"], 1),
            ("edge-type_text (name: edge_index_two)", "EDGE_TWO", ["content"], 1),
        ]
    )

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


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
