# Copyright 2023 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import random

from benchmark_context import BenchmarkContext
from constants import GraphVendors
from workloads.base import Workload
from workloads.importers.importer_pokec import ImporterPokec


class PokecTraversals(Workload):
    NAME = "pokec_traversals"
    VARIANTS = ["small", "medium", "large"]
    DEFAULT_VARIANT = "small"
    FILE = None

    URL_FILE = {
        "small": "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/pokec/benchmark/pokec_small_import.cypher",
        "medium": "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/pokec/benchmark/pokec_medium_import.cypher",
        "large": "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/pokec/benchmark/pokec_large.setup.cypher.gz",
    }

    SQL_URL_FILE = {
        "small": "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/pokec/benchmark/pokec_small_import.sql",
        "medium": "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/pokec/benchmark/pokec_medium_import.sql",
    }

    SIZES = {
        "small": {"vertices": 10000, "edges": 121716},
        "medium": {"vertices": 100000, "edges": 1768515},
        "large": {"vertices": 1632803, "edges": 30622564},
    }

    URL_INDEX_FILE = {
        GraphVendors.MEMGRAPH: "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/pokec/benchmark/memgraph.cypher",
        GraphVendors.NEO4J: "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/pokec/benchmark/neo4j.cypher",
        GraphVendors.FALKORDB: "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/pokec/benchmark/falkordb.cypher",
        GraphVendors.POSTGRESQL: "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/pokec/benchmark/postgresql.sql",
    }

    PROPERTIES_ON_EDGES = False

    def __init__(self, variant: str = None, benchmark_context: BenchmarkContext = None):
        super().__init__(variant, benchmark_context=benchmark_context)

    def custom_import(self, client) -> bool:
        importer = ImporterPokec(
            benchmark_context=self.benchmark_context,
            client=client,
            dataset_name=self.NAME,
            index_file=self._file_index,
            dataset_file=self._file,
            variant=self._variant,
        )
        return importer.execute_import()

    # Helpers used to generate the queries
    def _get_random_vertex(self):
        # All vertices in the Pokec dataset have an ID in the range
        # [1, _num_vertices].
        return random.randint(1, self._num_vertices)

    def _get_random_from_to(self):
        vertex_from = self._get_random_vertex()
        vertex_to = vertex_from
        while vertex_to == vertex_from:
            vertex_to = self._get_random_vertex()
        return (vertex_from, vertex_to)

    # Basic benchmark queries

    def benchmark__traversals__expansion_1_analytical(self):
        vertex_id = self._get_random_vertex()

        match self._vendor:
            case GraphVendors.POSTGRESQL:
                query = "SELECT friend_id FROM friendships WHERE user_id = %(id)s"
                params = {"id": vertex_id}
            case GraphVendors.MEMGRAPH | GraphVendors.NEO4J | GraphVendors.FALKORDB:
                query = "MATCH (s:User {id: $id})-->(n:User) RETURN n.id"
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__traversals__expansion_1_with_filter_analytical(self):
        vertex_id = self._get_random_vertex()

        match self._vendor:
            case GraphVendors.POSTGRESQL:
                query = """
                    SELECT f.friend_id
                    FROM friendships f
                    JOIN users u ON f.friend_id = u.id
                    WHERE f.user_id = %(id)s AND u.age >= 18
                """
                params = {"id": vertex_id}
            case GraphVendors.MEMGRAPH | GraphVendors.NEO4J | GraphVendors.FALKORDB:
                query = """
                    MATCH (s:User {id: $id})-->(n:User)
                    WHERE n.age >= 18
                    RETURN n.id
                """
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__traversals__expansion_2_analytical(self):
        vertex_id = self._get_random_vertex()

        match self._vendor:
            case GraphVendors.POSTGRESQL:
                query = """
                    SELECT DISTINCT f2.friend_id
                    FROM friendships f1
                    JOIN friendships f2 ON f1.friend_id = f2.user_id
                    WHERE f1.user_id = %(id)s
                """
                params = {"id": vertex_id}
            case GraphVendors.MEMGRAPH | GraphVendors.NEO4J | GraphVendors.FALKORDB:
                query = "MATCH (s:User {id: $id})-->()-->(n:User) RETURN DISTINCT n.id"
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__traversals__expansion_2_with_filter_analytical(self):
        vertex_id = self._get_random_vertex()

        match self._vendor:
            case GraphVendors.POSTGRESQL:
                query = """
                    SELECT DISTINCT f2.friend_id
                    FROM friendships f1
                    JOIN friendships f2 ON f1.friend_id = f2.user_id
                    JOIN users u ON f2.friend_id = u.id
                    WHERE f1.user_id = %(id)s AND u.age >= 18
                """
                params = {"id": vertex_id}
            case GraphVendors.MEMGRAPH | GraphVendors.NEO4J | GraphVendors.FALKORDB:
                query = """
                    MATCH (s:User {id: $id})-->()-->(n:User)
                    WHERE n.age >= 18
                    RETURN DISTINCT n.id
                """
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__traversals__expansion_3_analytical(self):
        vertex_id = self._get_random_vertex()

        match self._vendor:
            case GraphVendors.POSTGRESQL:
                query = """
                    SELECT DISTINCT f3.friend_id
                    FROM friendships f1
                    JOIN friendships f2 ON f1.friend_id = f2.user_id
                    JOIN friendships f3 ON f2.friend_id = f3.user_id
                    WHERE f1.user_id = %(id)s
                """
                params = {"id": vertex_id}
            case GraphVendors.MEMGRAPH | GraphVendors.NEO4J | GraphVendors.FALKORDB:
                query = "MATCH (s:User {id: $id})-->()-->()-->(n:User) RETURN DISTINCT n.id"
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__traversals__expansion_3_with_filter_analytical(self):
        vertex_id = self._get_random_vertex()

        match self._vendor:
            case GraphVendors.POSTGRESQL:
                query = """
                    SELECT DISTINCT f3.friend_id
                    FROM friendships f1
                    JOIN friendships f2 ON f1.friend_id = f2.user_id
                    JOIN friendships f3 ON f2.friend_id = f3.user_id
                    JOIN users u ON f3.friend_id = u.id
                    WHERE f1.user_id = %(id)s AND u.age >= 18
                """
                params = {"id": vertex_id}
            case GraphVendors.MEMGRAPH | GraphVendors.NEO4J | GraphVendors.FALKORDB:
                query = """
                    MATCH (s:User {id: $id})-->()-->()-->(n:User)
                    WHERE n.age >= 18
                    RETURN DISTINCT n.id
                """
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__traversals__expansion_4_analytical(self):
        vertex_id = self._get_random_vertex()

        match self._vendor:
            case GraphVendors.POSTGRESQL:
                query = """
                    SELECT DISTINCT f4.friend_id
                    FROM friendships f1
                    JOIN friendships f2 ON f1.friend_id = f2.user_id
                    JOIN friendships f3 ON f2.friend_id = f3.user_id
                    JOIN friendships f4 ON f3.friend_id = f4.user_id
                    WHERE f1.user_id = %(id)s
                """
                params = {"id": vertex_id}
            case GraphVendors.MEMGRAPH | GraphVendors.NEO4J | GraphVendors.FALKORDB:
                query = "MATCH (s:User {id: $id})-->()-->()-->()-->(n:User) RETURN DISTINCT n.id"
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__traversals__expansion_4_with_filter_analytical(self):
        vertex_id = self._get_random_vertex()

        match self._vendor:
            case GraphVendors.POSTGRESQL:
                query = """
                    SELECT DISTINCT f4.friend_id
                    FROM friendships f1
                    JOIN friendships f2 ON f1.friend_id = f2.user_id
                    JOIN friendships f3 ON f2.friend_id = f3.user_id
                    JOIN friendships f4 ON f3.friend_id = f4.user_id
                    JOIN users u ON f4.friend_id = u.id
                    WHERE f1.user_id = %(id)s AND u.age >= 18
                """
                params = {"id": vertex_id}
            case GraphVendors.MEMGRAPH | GraphVendors.NEO4J | GraphVendors.FALKORDB:
                query = """
                    MATCH (s:User {id: $id})-->()-->()-->()-->(n:User)
                    WHERE n.age >= 18
                    RETURN DISTINCT n.id
                """
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__traversals__expansion_5_analytical(self):
        vertex_id = self._get_random_vertex()

        match self._vendor:
            case GraphVendors.POSTGRESQL:
                query = """
                    SELECT DISTINCT f5.friend_id
                    FROM friendships f1
                    JOIN friendships f2 ON f1.friend_id = f2.user_id
                    JOIN friendships f3 ON f2.friend_id = f3.user_id
                    JOIN friendships f4 ON f3.friend_id = f4.user_id
                    JOIN friendships f5 ON f4.friend_id = f5.user_id
                    WHERE f1.user_id = %(id)s
                """
                params = {"id": vertex_id}
            case GraphVendors.MEMGRAPH | GraphVendors.NEO4J | GraphVendors.FALKORDB:
                query = "MATCH (s:User {id: $id})-->()-->()-->()-->()-->(n:User) RETURN DISTINCT n.id"
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__traversals__expansion_5_with_filter_analytical(self):
        vertex_id = self._get_random_vertex()

        match self._vendor:
            case GraphVendors.POSTGRESQL:
                query = """
                    SELECT DISTINCT f5.friend_id
                    FROM friendships f1
                    JOIN friendships f2 ON f1.friend_id = f2.user_id
                    JOIN friendships f3 ON f2.friend_id = f3.user_id
                    JOIN friendships f4 ON f3.friend_id = f4.user_id
                    JOIN friendships f5 ON f4.friend_id = f5.user_id
                    JOIN users u ON f5.friend_id = u.id
                    WHERE f1.user_id = %(id)s AND u.age >= 18
                """
                params = {"id": vertex_id}
            case GraphVendors.MEMGRAPH | GraphVendors.NEO4J | GraphVendors.FALKORDB:
                query = """
                    MATCH (s:User {id: $id})-->()-->()-->()-->()-->(n:User)
                    WHERE n.age >= 18
                    RETURN DISTINCT n.id
                """
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__traversals__neighbours_2_analytical(self):
        vertex_id = self._get_random_vertex()

        match self._vendor:
            case GraphVendors.POSTGRESQL:
                query = """
                    WITH RECURSIVE friends AS (
                        SELECT user_id, friend_id, 1 as depth
                        FROM friendships
                        WHERE user_id = %(id)s
                        UNION ALL
                        SELECT f.user_id, f.friend_id, fr.depth + 1
                        FROM friendships f
                        JOIN friends fr ON f.user_id = fr.friend_id
                        WHERE fr.depth < 2
                    )
                    SELECT DISTINCT friend_id FROM friends
                """
                params = {"id": vertex_id}
            case GraphVendors.MEMGRAPH | GraphVendors.NEO4J | GraphVendors.FALKORDB:
                query = "MATCH (s:User {id: $id})-[*1..2]->(n:User) RETURN DISTINCT n.id"
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__traversals__neighbours_2_with_filter_analytical(self):
        vertex_id = self._get_random_vertex()

        match self._vendor:
            case GraphVendors.POSTGRESQL:
                query = """
                    WITH RECURSIVE friends AS (
                        SELECT user_id, friend_id, 1 as depth
                        FROM friendships
                        WHERE user_id = %(id)s
                        UNION ALL
                        SELECT f.user_id, f.friend_id, fr.depth + 1
                        FROM friendships f
                        JOIN friends fr ON f.user_id = fr.friend_id
                        WHERE fr.depth < 2
                    )
                    SELECT DISTINCT f.friend_id
                    FROM friends f
                    JOIN users u ON f.friend_id = u.id
                    WHERE u.age >= 18
                """
                params = {"id": vertex_id}
            case GraphVendors.MEMGRAPH | GraphVendors.NEO4J | GraphVendors.FALKORDB:
                query = """
                    MATCH (s:User {id: $id})-[*1..2]->(n:User)
                    WHERE n.age >= 18
                    RETURN DISTINCT n.id
                """
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__traversals__neighbours_2_with_data_analytical(self):
        vertex_id = self._get_random_vertex()

        match self._vendor:
            case GraphVendors.POSTGRESQL:
                query = """
                    WITH RECURSIVE friends AS (
                        SELECT user_id, friend_id, 1 as depth
                        FROM friendships
                        WHERE user_id = %(id)s
                        UNION ALL
                        SELECT f.user_id, f.friend_id, fr.depth + 1
                        FROM friendships f
                        JOIN friends fr ON f.user_id = fr.friend_id
                        WHERE fr.depth < 2
                    )
                    SELECT DISTINCT u.*
                    FROM friends f
                    JOIN users u ON f.friend_id = u.id
                """
                params = {"id": vertex_id}
            case GraphVendors.MEMGRAPH | GraphVendors.NEO4J | GraphVendors.FALKORDB:
                query = "MATCH (s:User {id: $id})-[*1..2]->(n:User) RETURN DISTINCT n.id, n"
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__traversals__neighbours_2_with_data_and_filter_analytical(self):
        vertex_id = self._get_random_vertex()

        match self._vendor:
            case GraphVendors.POSTGRESQL:
                query = """
                    WITH RECURSIVE friends AS (
                        SELECT user_id, friend_id, 1 as depth
                        FROM friendships
                        WHERE user_id = %(id)s
                        UNION ALL
                        SELECT f.user_id, f.friend_id, fr.depth + 1
                        FROM friendships f
                        JOIN friends fr ON f.user_id = fr.friend_id
                        WHERE fr.depth < 2
                    )
                    SELECT DISTINCT u.*
                    FROM friends f
                    JOIN users u ON f.friend_id = u.id
                    WHERE u.age >= 18
                """
                params = {"id": vertex_id}
            case GraphVendors.MEMGRAPH | GraphVendors.NEO4J | GraphVendors.FALKORDB:
                query = """
                    MATCH (s:User {id: $id})-[*1..2]->(n:User)
                    WHERE n.age >= 18
                    RETURN DISTINCT n.id, n
                """
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__traversals__pattern_cycle_analytical(self):
        vertex_id = self._get_random_vertex()

        match self._vendor:
            case GraphVendors.POSTGRESQL:
                query = """
                    WITH RECURSIVE cycle AS (
                        SELECT f1.user_id, f1.friend_id, f2.user_id as cycle_user_id
                        FROM friendships f1
                        JOIN friendships f2 ON f1.friend_id = f2.user_id
                        WHERE f1.user_id = %(id)s AND f2.friend_id = %(id)s
                    )
                    SELECT * FROM cycle
                """
                params = {"id": vertex_id}
            case GraphVendors.MEMGRAPH | GraphVendors.NEO4J | GraphVendors.FALKORDB:
                query = "MATCH (n:User {id: $id})-[e1]->(m)-[e2]->(n) RETURN e1, m, e2"
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__traversals__pattern_long_analytical(self):
        vertex_id = self._get_random_vertex()

        match self._vendor:
            case GraphVendors.POSTGRESQL:
                query = """
                    SELECT f5.user_id
                    FROM friendships f1
                    JOIN friendships f2 ON f1.friend_id = f2.user_id
                    JOIN friendships f3 ON f2.friend_id = f3.user_id
                    JOIN friendships f4 ON f3.friend_id = f4.user_id
                    JOIN friendships f5 ON f4.friend_id = f5.user_id
                    WHERE f1.user_id = %(id)s
                    LIMIT 1
                """
                params = {"id": vertex_id}
            case GraphVendors.MEMGRAPH | GraphVendors.NEO4J | GraphVendors.FALKORDB:
                query = """
                    MATCH (n1:User {id: $id})-[e1]->(n2)-[e2]->(n3)-[e3]->(n4)<-[e4]-(n5)
                    RETURN n5 LIMIT 1
                """
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__traversals__pattern_short_analytical(self):
        vertex_id = self._get_random_vertex()

        match self._vendor:
            case GraphVendors.POSTGRESQL:
                query = "SELECT friend_id FROM friendships WHERE user_id = %(id)s LIMIT 1"
                params = {"id": vertex_id}
            case GraphVendors.MEMGRAPH | GraphVendors.NEO4J | GraphVendors.FALKORDB:
                query = "MATCH (n:User {id: $id})-[e]->(m) RETURN m LIMIT 1"
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__traversals__neighbours_3_with_data_and_filter_analytical(self):
        vertex_id = self._get_random_vertex()

        match self._vendor:
            case GraphVendors.POSTGRESQL:
                query = """
                    WITH RECURSIVE friends AS (
                        SELECT user_id, friend_id, 1 as depth
                        FROM friendships
                        WHERE user_id = %(id)s
                        UNION ALL
                        SELECT f.user_id, f.friend_id, fr.depth + 1
                        FROM friendships f
                        JOIN friends fr ON f.user_id = fr.friend_id
                        WHERE fr.depth < 3
                    )
                    SELECT DISTINCT u.*
                    FROM friends f
                    JOIN users u ON f.friend_id = u.id
                    WHERE u.age >= 18
                """
                params = {"id": vertex_id}
            case GraphVendors.MEMGRAPH | GraphVendors.NEO4J | GraphVendors.FALKORDB:
                query = """
                    MATCH (s:User {id: $id})-[*1..3]->(n:User)
                    WHERE n.age >= 18
                    RETURN DISTINCT n.id, n
                """
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__traversals__neighbours_4_with_data_and_filter_analytical(self):
        vertex_id = self._get_random_vertex()

        match self._vendor:
            case GraphVendors.POSTGRESQL:
                query = """
                    WITH RECURSIVE friends AS (
                        SELECT user_id, friend_id, 1 as depth
                        FROM friendships
                        WHERE user_id = %(id)s
                        UNION ALL
                        SELECT f.user_id, f.friend_id, fr.depth + 1
                        FROM friendships f
                        JOIN friends fr ON f.user_id = fr.friend_id
                        WHERE fr.depth < 4
                    )
                    SELECT DISTINCT u.*
                    FROM friends f
                    JOIN users u ON f.friend_id = u.id
                    WHERE u.age >= 18
                """
                params = {"id": vertex_id}
            case GraphVendors.MEMGRAPH | GraphVendors.NEO4J | GraphVendors.FALKORDB:
                query = """
                    MATCH (s:User {id: $id})-[*1..4]->(n:User)
                    WHERE n.age >= 18
                    RETURN DISTINCT n.id, n
                """
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__traversals__neighbours_5_with_data_and_filter_analytical(self):
        vertex_id = self._get_random_vertex()

        match self._vendor:
            case GraphVendors.POSTGRESQL:
                query = """
                    WITH RECURSIVE friends AS (
                        SELECT user_id, friend_id, 1 as depth
                        FROM friendships
                        WHERE user_id = %(id)s
                        UNION ALL
                        SELECT f.user_id, f.friend_id, fr.depth + 1
                        FROM friendships f
                        JOIN friends fr ON f.user_id = fr.friend_id
                        WHERE fr.depth < 5
                    )
                    SELECT DISTINCT u.*
                    FROM friends f
                    JOIN users u ON f.friend_id = u.id
                    WHERE u.age >= 18
                """
                params = {"id": vertex_id}
            case GraphVendors.MEMGRAPH | GraphVendors.NEO4J | GraphVendors.FALKORDB:
                query = """
                    MATCH (s:User {id: $id})-[*1..5]->(n:User)
                    WHERE n.age >= 18
                    RETURN DISTINCT n.id, n
                """
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params
