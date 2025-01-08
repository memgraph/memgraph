# Copyright 2022 Memgraph Ltd.
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
from pathlib import Path

import pytest
from common import memgraph
from mgclient import DatabaseError
from neo4j import GraphDatabase

SIMPLE_NODES_CSV_FILE = "simple_nodes.csv"
SIMPLE_EDGES_CSV_FILE = "simple_edges.csv"


def get_file_path(file: str) -> str:
    return os.path.join(Path(__file__).parent.absolute(), file)


def test_given_two_rows_in_db_when_load_csv_after_match_then_throw_exception(memgraph):
    with pytest.raises(DatabaseError):
        next(
            memgraph.execute_and_fetch(
                f"""MATCH (n) LOAD CSV
            FROM '{get_file_path(SIMPLE_NODES_CSV_FILE)}' WITH HEADER AS row
            CREATE (:Person {{name: row.name}})
            """
            )
        )


def test_given_one_row_in_db_when_load_csv_after_match_then_pass(memgraph):
    results = memgraph.execute_and_fetch(
        f"""MATCH (n {{prop: 1}}) LOAD CSV
        FROM '{get_file_path(SIMPLE_NODES_CSV_FILE)}' WITH HEADER AS row
        CREATE (:Person {{name: row.name}})
        RETURN n
        """
    )

    assert len(list(results)) == 4


def test_creating_labels_with_load_csv_variable(memgraph):
    results = list(
        memgraph.execute_and_fetch(
            f"""LOAD CSV FROM '{get_file_path(SIMPLE_NODES_CSV_FILE)}' WITH HEADER AS row
        CREATE (p:row.name)
        RETURN p
        """
        )
    )

    assert len(results) == 4
    assert results[0]["p"]._labels == {"Joseph"}
    assert results[1]["p"]._labels == {"Peter"}
    assert results[2]["p"]._labels == {"Ella"}
    assert results[3]["p"]._labels == {"Joe"}


def test_create_relationships_with_load_csv_variable2(memgraph):
    results = list(
        memgraph.execute_and_fetch(
            f"""LOAD CSV FROM '{get_file_path(SIMPLE_NODES_CSV_FILE)}' WITH HEADER AS row
        CREATE (p:row.name:Person:row.id)
        RETURN p
        """
        )
    )

    assert len(results) == 4
    assert results[0]["p"]._labels == {"Joseph", "Person", "1"}
    assert results[1]["p"]._labels == {"Peter", "Person", "2"}
    assert results[2]["p"]._labels == {"Ella", "Person", "3"}
    assert results[3]["p"]._labels == {"Joe", "Person", "4"}


def test_load_csv_dynamic_labels_with_parameters(memgraph):
    URI = "bolt://localhost:7687"
    AUTH = ("", "")

    with GraphDatabase.driver(URI, auth=AUTH) as client:
        with client.session(database="memgraph") as session:
            results = session.run(
                f"""MATCH (n {{prop: 1}}) LOAD CSV
                FROM $file WITH HEADER AS row
                CREATE (:Person {{name: row.name}})
                RETURN n""",
                file=get_file_path(SIMPLE_NODES_CSV_FILE),
            )
            assert len(list(results)) == 4


def test_load_csv_dynamic_labels_with_parameter_for_label(memgraph):
    URI = "bolt://localhost:7687"
    AUTH = ("", "")

    with GraphDatabase.driver(URI, auth=AUTH) as client:
        with client.session(database="memgraph") as session:
            results = session.run(
                f"""MATCH (n {{prop: 1}}) LOAD CSV
                FROM $file WITH HEADER AS row
                CREATE (:Person {{name: $node_label}})
                RETURN n""",
                file=get_file_path(SIMPLE_NODES_CSV_FILE),
                node_label="NODE_LABEL",
            )
            assert len(list(results)) == 4


def test_creating_edge_types_with_load_csv_variable(memgraph):
    memgraph.execute("CREATE INDEX ON :Person")
    memgraph.execute("CREATE INDEX ON :Person(id)")

    memgraph.execute(
        f"""LOAD CSV FROM '{get_file_path(SIMPLE_EDGES_CSV_FILE)}' WITH HEADER AS row
    MERGE (:Person {{id: row.from_id}})
    MERGE (:Person {{id: row.to_id}})
    """
    )

    results = list(
        memgraph.execute_and_fetch(
            f"""LOAD CSV FROM '{get_file_path(SIMPLE_EDGES_CSV_FILE)}' WITH HEADER AS row
        MATCH (p1:Person {{id: row.from_id}})
        MATCH (p2:Person {{id: row.to_id}})
        CREATE (p1)-[e:row.edge_type]->(p2)
        RETURN e
        """,
        )
    )

    assert len(results) == 4
    assert results[0]["e"]._type == "EDGE_1"
    assert results[1]["e"]._type == "EDGE_2"
    assert results[2]["e"]._type == "EDGE_3"
    assert results[3]["e"]._type == "EDGE_4"


def test_creating_edge_types_with_load_csv_parameter(memgraph):
    memgraph.execute("CREATE INDEX ON :Person")
    memgraph.execute("CREATE INDEX ON :Person(id)")

    memgraph.execute(
        f"""LOAD CSV FROM '{get_file_path(SIMPLE_EDGES_CSV_FILE)}' WITH HEADER AS row
    MERGE (:Person {{id: row.from_id}})
    MERGE (:Person {{id: row.to_id}})
    """
    )

    URI = "bolt://localhost:7687"
    AUTH = ("", "")

    with GraphDatabase.driver(URI, auth=AUTH) as client:
        with client.session(database="memgraph") as session:
            results = session.run(
                f"""LOAD CSV FROM '{get_file_path(SIMPLE_EDGES_CSV_FILE)}' WITH HEADER AS row
            MATCH (p1:Person {{id: row.from_id}})
            MATCH (p2:Person {{id: row.to_id}})
            CREATE (p1)-[e:$edge_type]->(p2)
            RETURN e
            """,
                edge_type="EDGE_TYPE",
            )
            assert len(list(results)) == 4


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
