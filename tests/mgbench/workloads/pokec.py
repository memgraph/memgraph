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


class Pokec(Workload):
    NAME = "pokec"
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
        GraphVendors.ARANGODB: "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/pokec/benchmark/postgresql.sql",
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

    # Arango benchmarks

    def benchmark__arango__single_vertex_read(self):
        vertex_id = self._get_random_vertex()

        match self._vendor:
            case GraphVendors.POSTGRESQL:
                query = "SELECT * FROM users WHERE id = %(id)s"
                params = {"id": vertex_id}
            case GraphVendors.MEMGRAPH | GraphVendors.NEO4J | GraphVendors.FALKORDB:
                query = "MATCH (n:User {id : $id}) RETURN n"
                params = {"id": vertex_id}
            case GraphVendors.ARANGODB:
                query = "FOR u IN users FILTER u.id == @id RETURN u"
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__arango__unwind_range_vertex_write(self):
        return (
            "UNWIND range(1, 100) as x CREATE (:L1:L2:L3:L4:L5:L6:L7 {p1: true, p2: 42, "
            'p3: "Here is some text that is not extremely short", '
            'p4:"Short text", p5: 234.434, p6: 11.11, p7: false})',
            {},
        )

    def benchmark__arango__single_vertex_write(self):
        vertex_id = random.randint(1, self._num_vertices * 10)

        match self._vendor:
            case GraphVendors.POSTGRESQL:
                query = "INSERT INTO users_temp (id) VALUES (%(id)s) RETURNING *"
                params = {"id": vertex_id}
            case GraphVendors.MEMGRAPH | GraphVendors.NEO4J | GraphVendors.FALKORDB:
                query = "CREATE (n:UserTemp {id : $id}) RETURN n"
                params = {"id": vertex_id}
            case GraphVendors.ARANGODB:
                query = "INSERT {id: @id} INTO users_temp RETURN NEW"
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__arango__single_edge_write(self):
        vertex_from, vertex_to = self._get_random_from_to()

        match self._vendor:
            case GraphVendors.POSTGRESQL:
                query = "INSERT INTO friendships (user_id, friend_id) VALUES (%(from)s, %(to)s) RETURNING *"
                params = {"from": vertex_from, "to": vertex_to}
            case GraphVendors.MEMGRAPH | GraphVendors.NEO4J | GraphVendors.FALKORDB:
                query = """
                    MATCH (n:User {id: $from}), (m:User {id: $to}) WITH n, m
                    CREATE (n)-[e:Temp]->(m) RETURN e
                """
                params = {"from": vertex_from, "to": vertex_to}
            case GraphVendors.ARANGODB:
                query = """
                    LET from_user = FIRST(FOR u IN users FILTER u.id == @from LIMIT 1 RETURN u._id)
                    LET to_user = FIRST(FOR u IN users FILTER u.id == @to LIMIT 1 RETURN u._id)
                    INSERT {_from: from_user, _to: to_user} INTO friendships RETURN NEW
                """
                params = {"from": vertex_from, "to": vertex_to}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__arango__aggregate(self):
        match self._vendor:
            case GraphVendors.POSTGRESQL:
                query = "SELECT age, COUNT(*) FROM users GROUP BY age"
                params = {}
            case GraphVendors.MEMGRAPH | GraphVendors.NEO4J | GraphVendors.FALKORDB:
                query = "MATCH (n:User) RETURN n.age, COUNT(*)"
                params = {}
            case GraphVendors.ARANGODB:
                query = "FOR u IN users COLLECT age = u.age WITH COUNT INTO count RETURN age, count"
                params = {}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__arango__aggregate_with_distinct(self):
        match self._vendor:
            case GraphVendors.POSTGRESQL:
                query = "SELECT COUNT(DISTINCT age) FROM users"
                params = {}
            case GraphVendors.MEMGRAPH | GraphVendors.NEO4J | GraphVendors.FALKORDB:
                query = "MATCH (n:User) RETURN COUNT(DISTINCT n.age)"
                params = {}
            case GraphVendors.ARANGODB:
                query = "RETURN LENGTH(UNIQUE(FOR u IN users RETURN u.age))"
                params = {}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__arango__aggregate_with_filter(self):
        match self._vendor:
            case GraphVendors.POSTGRESQL:
                query = "SELECT age, COUNT(*) FROM users WHERE age >= 18 GROUP BY age"
                params = {}
            case GraphVendors.MEMGRAPH | GraphVendors.NEO4J | GraphVendors.FALKORDB:
                query = "MATCH (n:User) WHERE n.age >= 18 RETURN n.age, COUNT(*)"
                params = {}
            case GraphVendors.ARANGODB:
                query = "FOR u IN users FILTER u.age >= 18 COLLECT age = u.age WITH COUNT INTO count RETURN age, count"
                params = {}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__arango__expansion_1(self):
        vertex_id = self._get_random_vertex()

        match self._vendor:
            case GraphVendors.POSTGRESQL:
                query = "SELECT f.friend_id FROM friendships f WHERE f.user_id = %(id)s"
                params = {"id": vertex_id}
            case GraphVendors.MEMGRAPH | GraphVendors.NEO4J | GraphVendors.FALKORDB:
                query = "MATCH (s:User {id: $id})-->(n:User) RETURN n.id"
                params = {"id": vertex_id}
            case GraphVendors.ARANGODB:
                query = """
                    FOR u IN users FILTER u.id == @id
                    FOR v, e, p IN 1..1 OUTBOUND u friendships
                    RETURN v.id
                """
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__arango__expansion_1_with_filter(self):
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
            case GraphVendors.ARANGODB:
                query = """
                    FOR u IN users FILTER u.id == @id
                    FOR v, e, p IN 1..1 OUTBOUND u friendships
                    FILTER v.age >= 18
                    RETURN v.id
                """
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__arango__expansion_2(self):
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
            case GraphVendors.ARANGODB:
                query = """
                    FOR u IN users FILTER u.id == @id
                    FOR v, e, p IN 1..2 OUTBOUND u friendships
                    RETURN DISTINCT v.id
                """
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__arango__expansion_2_with_filter(self):
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
            case GraphVendors.ARANGODB:
                query = """
                    FOR u IN users FILTER u.id == @id
                    FOR v, e, p IN 1..2 OUTBOUND u friendships
                    FILTER v.age >= 18
                    RETURN DISTINCT v.id
                """
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__arango__expansion_3(self):
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
            case GraphVendors.ARANGODB:
                query = """
                    FOR u IN users FILTER u.id == @id
                    FOR v, e, p IN 1..3 OUTBOUND u friendships
                    RETURN DISTINCT v.id
                """
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__arango__expansion_3_with_filter(self):
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
            case GraphVendors.ARANGODB:
                query = """
                    FOR u IN users FILTER u.id == @id
                    FOR v, e, p IN 1..3 OUTBOUND u friendships
                    FILTER v.age >= 18
                    RETURN DISTINCT v.id
                """
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__arango__expansion_4(self):
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
            case GraphVendors.ARANGODB:
                query = """
                    FOR u IN users FILTER u.id == @id
                    FOR v, e, p IN 1..4 OUTBOUND u friendships
                    RETURN DISTINCT v.id
                """
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__arango__expansion_4_with_filter(self):
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
            case GraphVendors.ARANGODB:
                query = """
                    FOR u IN users FILTER u.id == @id
                    FOR v, e, p IN 1..4 OUTBOUND u friendships
                    FILTER v.age >= 18
                    RETURN DISTINCT v.id
                """
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__arango__neighbours_2(self):
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
            case GraphVendors.ARANGODB:
                query = """
                    FOR u IN users FILTER u.id == @id
                    FOR v, e, p IN 1..2 OUTBOUND u friendships
                    RETURN DISTINCT v.id
                """
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__arango__neighbours_2_with_filter(self):
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
            case GraphVendors.ARANGODB:
                query = """
                    FOR u IN users FILTER u.id == @id
                    FOR v, e, p IN 1..2 OUTBOUND u friendships
                    FILTER v.age >= 18
                    RETURN DISTINCT v.id
                """
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__arango__neighbours_2_with_data(self):
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
            case GraphVendors.ARANGODB:
                query = """
                    FOR u IN users FILTER u.id == @id
                    FOR v, e, p IN 1..2 OUTBOUND u friendships
                    RETURN DISTINCT v.id, v
                """
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__arango__neighbours_2_with_data_and_filter(self):
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
            case GraphVendors.ARANGODB:
                query = """
                    FOR u IN users FILTER u.id == @id
                    FOR v, e, p IN 1..2 OUTBOUND u friendships
                    FILTER v.age >= 18
                    RETURN DISTINCT v.id, v
                """
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__arango__shortest_path(self):
        vertex_from, vertex_to = self._get_random_from_to()

        match self._vendor:
            case GraphVendors.POSTGRESQL:
                query = """
                    WITH RECURSIVE path AS (
                        SELECT user_id, friend_id, ARRAY[user_id, friend_id] as path, 1 as depth
                        FROM friendships
                        WHERE user_id = %(from)s
                        UNION ALL
                        SELECT p.user_id, f.friend_id, p.path || f.friend_id, p.depth + 1
                        FROM path p
                        JOIN friendships f ON p.friend_id = f.user_id
                        WHERE f.friend_id != ALL(p.path)
                        AND p.depth < 15
                    )
                    SELECT path
                    FROM path
                    WHERE friend_id = %(to)s
                    ORDER BY depth
                    LIMIT 1
                """
                params = {"from": vertex_from, "to": vertex_to}
            case GraphVendors.MEMGRAPH:
                query = """
                    MATCH (n:User {id: $from}), (m:User {id: $to}) WITH n, m
                    MATCH p=(n)-[*bfs..15]->(m)
                    RETURN extract(n in nodes(p) | n.id) AS path
                """
                params = {"from": vertex_from, "to": vertex_to}
            case GraphVendors.ARANGODB:
                query = """
                    FOR from_user IN users FILTER from_user.id == @from LIMIT 1
                    FOR to_user IN users FILTER to_user.id == @to LIMIT 1
                    FOR v, e, p IN 1..15 ANY SHORTEST_PATH from_user TO to_user friendships
                    RETURN [v2.id FOR v2 IN p.vertices]
                """
                params = {"from": vertex_from, "to": vertex_to}
            case GraphVendors.NEO4J | GraphVendors.FALKORDB:
                query = """
                    MATCH (n:User {id: $from}), (m:User {id: $to}) WITH n, m
                    MATCH p=shortestPath((n)-[*..15]->(m))
                    RETURN [n in nodes(p) | n.id] AS path
                """
                params = {"from": vertex_from, "to": vertex_to}
            case GraphVendors.ARANGODB:
                query = """
                    FOR from_user IN users FILTER from_user.id == @from LIMIT 1
                    FOR to_user IN users FILTER to_user.id == @to LIMIT 1
                    FOR v, e, p IN 1..15 ANY SHORTEST_PATH from_user TO to_user friendships
                    RETURN [v2.id FOR v2 IN p.vertices]
                """
                params = {"from": vertex_from, "to": vertex_to}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__arango__shortest_path_with_filter(self):
        vertex_from, vertex_to = self._get_random_from_to()

        match self._vendor:
            case GraphVendors.POSTGRESQL:
                query = """
                    WITH RECURSIVE path AS (
                        SELECT user_id, friend_id, ARRAY[user_id, friend_id] as path, 1 as depth
                        FROM friendships f
                        JOIN users u ON f.friend_id = u.id
                        WHERE f.user_id = %(from)s AND u.age >= 18
                        UNION ALL
                        SELECT p.user_id, f.friend_id, p.path || f.friend_id, p.depth + 1
                        FROM path p
                        JOIN friendships f ON p.friend_id = f.user_id
                        JOIN users u ON f.friend_id = u.id
                        WHERE f.friend_id != ALL(p.path)
                        AND p.depth < 15
                        AND u.age >= 18
                    )
                    SELECT path
                    FROM path
                    WHERE friend_id = %(to)s
                    ORDER BY depth
                    LIMIT 1
                """
                params = {"from": vertex_from, "to": vertex_to}
            case GraphVendors.MEMGRAPH:
                query = """
                    MATCH (n:User {id: $from}), (m:User {id: $to}) WITH n, m
                    MATCH p=(n)-[*bfs..15 (e, n | n.age >= 18)]->(m)
                    RETURN extract(n in nodes(p) | n.id) AS path
                """
                params = {"from": vertex_from, "to": vertex_to}
            case GraphVendors.NEO4J | GraphVendors.FALKORDB:
                query = """
                    MATCH (n:User {id: $from}), (m:User {id: $to}) WITH n, m
                    MATCH p=shortestPath((n)-[*..15]->(m))
                    WHERE all(node in nodes(p) WHERE node.age >= 18)
                    RETURN [n in nodes(p) | n.id] AS path
                """
                params = {"from": vertex_from, "to": vertex_to}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__arango__allshortest_paths(self):
        vertex_from, vertex_to = self._get_random_from_to()

        match self._vendor:
            case GraphVendors.POSTGRESQL:
                query = """
                    WITH RECURSIVE paths AS (
                        SELECT user_id, friend_id, ARRAY[user_id, friend_id] as path, 1 as depth
                        FROM friendships
                        WHERE user_id = %(from)s
                        UNION ALL
                        SELECT p.user_id, f.friend_id, p.path || f.friend_id, p.depth + 1
                        FROM paths p
                        JOIN friendships f ON p.friend_id = f.user_id
                        WHERE f.friend_id != ALL(p.path)
                        AND p.depth < 2
                    )
                    SELECT path
                    FROM paths
                    WHERE friend_id = %(to)s
                """
                params = {"from": vertex_from, "to": vertex_to}
            case GraphVendors.MEMGRAPH:
                query = """
                    MATCH (n:User {id: $from}), (m:User {id: $to}) WITH n, m
                    MATCH p=(n)-[*allshortest 2 (r, n | 1) total_weight]->(m)
                    RETURN extract(n in nodes(p) | n.id) AS path
                """
                params = {"from": vertex_from, "to": vertex_to}
            case GraphVendors.NEO4J | GraphVendors.FALKORDB:
                query = """
                    MATCH (n:User {id: $from}), (m:User {id: $to}) WITH n, m
                    MATCH p = allShortestPaths((n)-[*..2]->(m))
                    RETURN [node in nodes(p) | node.id] AS path
                """
                params = {"from": vertex_from, "to": vertex_to}
            case GraphVendors.ARANGODB:
                query = """
                    FOR from_user IN users FILTER from_user.id == @from LIMIT 1
                    FOR to_user IN users FILTER to_user.id == @to LIMIT 1
                    FOR v, e, p IN 1..2 ANY SHORTEST_PATH from_user TO to_user friendships
                    RETURN [v2.id FOR v2 IN p.vertices]
                """
                params = {"from": vertex_from, "to": vertex_to}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    # Our benchmark queries

    def benchmark__create__edge(self):
        vertex_from, vertex_to = self._get_random_from_to()

        match self._vendor:
            case GraphVendors.POSTGRESQL:
                query = "INSERT INTO friendships (user_id, friend_id) VALUES (%(from)s, %(to)s) RETURNING *"
                params = {"from": vertex_from, "to": vertex_to}
            case GraphVendors.MEMGRAPH | GraphVendors.NEO4J | GraphVendors.FALKORDB:
                query = "MATCH (a:User {id: $from}), (b:User {id: $to}) CREATE (a)-[:TempEdge]->(b)"
                params = {"from": vertex_from, "to": vertex_to}
            case GraphVendors.ARANGODB:
                query = """
                    LET from_user = FIRST(FOR u IN users FILTER u.id == @from LIMIT 1 RETURN u._id)
                    LET to_user = FIRST(FOR u IN users FILTER u.id == @to LIMIT 1 RETURN u._id)
                    INSERT {_from: from_user, _to: to_user} INTO friendships RETURN NEW
                """
                params = {"from": vertex_from, "to": vertex_to}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__create__pattern(self):
        match self._vendor:
            case GraphVendors.POSTGRESQL:
                query = "INSERT INTO friendships (user_id, friend_id) VALUES (1, 2) RETURNING *"
                params = {}
            case GraphVendors.MEMGRAPH | GraphVendors.NEO4J | GraphVendors.FALKORDB:
                query = "CREATE ()-[:TempEdge]->()"
                params = {}
            case GraphVendors.ARANGODB:
                query = "LET v1 = FIRST(FOR u IN users LIMIT 1 RETURN u._id) LET v2 = FIRST(FOR u IN users LIMIT 2 RETURN u._id) INSERT {_from: v1, _to: v2} INTO friendships RETURN NEW"
                params = {}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__create__vertex(self):
        match self._vendor:
            case GraphVendors.POSTGRESQL:
                query = "INSERT INTO users_temp (id) VALUES (1) RETURNING *"
                params = {}
            case GraphVendors.MEMGRAPH | GraphVendors.NEO4J | GraphVendors.FALKORDB:
                query = "CREATE ()"
                params = {}
            case GraphVendors.ARANGODB:
                query = "INSERT {} INTO users_temp RETURN NEW"
                params = {}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__create__vertex_big(self):
        match self._vendor:
            case GraphVendors.POSTGRESQL:
                query = """
                    INSERT INTO users_temp (
                        id, p1, p2, p3, p4, p5, p6, p7
                    ) VALUES (
                        1, true, 42, 'Here is some text that is not extremely short',
                        'Short text', 234.434, 11.11, false
                    ) RETURNING *
                """
                params = {}
            case GraphVendors.MEMGRAPH | GraphVendors.NEO4J | GraphVendors.FALKORDB:
                query = """
                    CREATE (:L1:L2:L3:L4:L5:L6:L7 {
                        p1: true, p2: 42,
                        p3: "Here is some text that is not extremely short",
                        p4: "Short text", p5: 234.434, p6: 11.11, p7: false
                    })
                """
                params = {}
            case GraphVendors.ARANGODB:
                query = """
                    INSERT {
                        p1: true, p2: 42,
                        p3: "Here is some text that is not extremely short",
                        p4: "Short text", p5: 234.434, p6: 11.11, p7: false
                    } INTO users_temp RETURN NEW
                """
                params = {}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__aggregation__count(self):
        match self._vendor:
            case GraphVendors.POSTGRESQL:
                query = "SELECT COUNT(*), COUNT(age) FROM users"
                params = {}
            case GraphVendors.MEMGRAPH | GraphVendors.NEO4J | GraphVendors.FALKORDB:
                query = "MATCH (n) RETURN count(n), count(n.age)"
                params = {}
            case GraphVendors.ARANGODB:
                query = "RETURN LENGTH(users), LENGTH(FOR u IN users FILTER u.age != null RETURN 1)"
                params = {}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__aggregation__min_max_avg(self):
        match self._vendor:
            case GraphVendors.POSTGRESQL:
                query = "SELECT MIN(age), MAX(age), AVG(age) FROM users"
                params = {}
            case GraphVendors.MEMGRAPH | GraphVendors.NEO4J | GraphVendors.FALKORDB:
                query = "MATCH (n) RETURN min(n.age), max(n.age), avg(n.age)"
                params = {}
            case GraphVendors.ARANGODB:
                query = "RETURN MIN(FOR u IN users RETURN u.age), MAX(FOR u IN users RETURN u.age), AVG(FOR u IN users RETURN u.age)"
                params = {}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__match__pattern_cycle(self):
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

    def benchmark__match__pattern_long(self):
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

    def benchmark__match__pattern_short(self):
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

    def benchmark__match__vertex_on_label_property(self):
        vertex_id = self._get_random_vertex()

        match self._vendor:
            case GraphVendors.POSTGRESQL:
                query = "SELECT * FROM users WHERE id = %(id)s"
                params = {"id": vertex_id}
            case GraphVendors.MEMGRAPH | GraphVendors.NEO4J | GraphVendors.FALKORDB:
                query = "MATCH (n:User) WITH n WHERE n.id = $id RETURN n"
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__match__vertex_on_label_property_index(self):
        vertex_id = self._get_random_vertex()

        match self._vendor:
            case GraphVendors.POSTGRESQL:
                query = "SELECT * FROM users WHERE id = %(id)s"
                params = {"id": vertex_id}
            case GraphVendors.MEMGRAPH | GraphVendors.NEO4J | GraphVendors.FALKORDB:
                query = "MATCH (n:User {id: $id}) RETURN n"
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__match__vertex_on_property(self):
        vertex_id = self._get_random_vertex()

        match self._vendor:
            case GraphVendors.POSTGRESQL:
                query = "SELECT * FROM users WHERE id = %(id)s"
                params = {"id": vertex_id}
            case GraphVendors.MEMGRAPH | GraphVendors.NEO4J | GraphVendors.FALKORDB:
                query = "MATCH (n:User {id: $id}) RETURN n"
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__update__vertex_on_property(self):
        vertex_id = self._get_random_vertex()

        match self._vendor:
            case GraphVendors.POSTGRESQL:
                query = "UPDATE users SET property = -1 WHERE id = %(id)s RETURNING *"
                params = {"id": vertex_id}
            case GraphVendors.MEMGRAPH | GraphVendors.NEO4J | GraphVendors.FALKORDB:
                query = "MATCH (n {id: $id}) SET n.property = -1"
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    # Basic benchmark queries

    def benchmark__basic__single_vertex_read_read(self):
        vertex_id = self._get_random_vertex()

        match self._vendor:
            case GraphVendors.POSTGRESQL:
                query = "SELECT * FROM users WHERE id = %(id)s"
                params = {"id": vertex_id}
            case GraphVendors.MEMGRAPH | GraphVendors.NEO4J | GraphVendors.FALKORDB:
                query = "MATCH (n:User {id : $id}) RETURN n"
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__basic__single_vertex_write_write(self):
        vertex_id = random.randint(1, self._num_vertices * 10)

        match self._vendor:
            case GraphVendors.POSTGRESQL:
                query = "INSERT INTO users_temp (id) VALUES (%(id)s) RETURNING *"
                params = {"id": vertex_id}
            case GraphVendors.MEMGRAPH | GraphVendors.NEO4J | GraphVendors.FALKORDB:
                query = "CREATE (n:UserTemp {id : $id}) RETURN n"
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__basic__single_vertex_property_update_update(self):
        vertex_id = self._get_random_vertex()

        match self._vendor:
            case GraphVendors.POSTGRESQL:
                query = "UPDATE users SET property = -1 WHERE id = %(id)s RETURNING *"
                params = {"id": vertex_id}
            case GraphVendors.MEMGRAPH | GraphVendors.NEO4J | GraphVendors.FALKORDB:
                query = "MATCH (n:User {id: $id}) SET n.property = -1"
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__basic__single_edge_write_write(self):
        vertex_from, vertex_to = self._get_random_from_to()

        match self._vendor:
            case GraphVendors.POSTGRESQL:
                query = "INSERT INTO friendships (user_id, friend_id) VALUES (%(from)s, %(to)s) RETURNING *"
                params = {"from": vertex_from, "to": vertex_to}
            case GraphVendors.MEMGRAPH | GraphVendors.NEO4J | GraphVendors.FALKORDB:
                query = """
                    MATCH (n:User {id: $from}), (m:User {id: $to}) WITH n, m
                    CREATE (n)-[e:Temp]->(m) RETURN e
                """
                params = {"from": vertex_from, "to": vertex_to}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__basic__aggregate_aggregate(self):
        match self._vendor:
            case GraphVendors.POSTGRESQL:
                query = "SELECT age, COUNT(*) FROM users GROUP BY age"
                params = {}
            case GraphVendors.MEMGRAPH | GraphVendors.NEO4J | GraphVendors.FALKORDB:
                query = "MATCH (n:User) RETURN n.age, COUNT(*)"
                params = {}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__basic__aggregate_count_aggregate(self):
        match self._vendor:
            case GraphVendors.POSTGRESQL:
                query = "SELECT COUNT(*), COUNT(age) FROM users"
                params = {}
            case GraphVendors.MEMGRAPH | GraphVendors.NEO4J | GraphVendors.FALKORDB:
                query = "MATCH (n) RETURN count(n), count(n.age)"
                params = {}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__basic__aggregate_with_filter_aggregate(self):
        match self._vendor:
            case GraphVendors.POSTGRESQL:
                query = "SELECT age, COUNT(*) FROM users WHERE age >= 18 GROUP BY age"
                params = {}
            case GraphVendors.MEMGRAPH | GraphVendors.NEO4J | GraphVendors.FALKORDB:
                query = "MATCH (n:User) WHERE n.age >= 18 RETURN n.age, COUNT(*)"
                params = {}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__basic__min_max_avg_aggregate(self):
        match self._vendor:
            case GraphVendors.POSTGRESQL:
                query = "SELECT MIN(age), MAX(age), AVG(age) FROM users"
                params = {}
            case GraphVendors.MEMGRAPH | GraphVendors.NEO4J | GraphVendors.FALKORDB:
                query = "MATCH (n) RETURN min(n.age), max(n.age), avg(n.age)"
                params = {}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__basic__expansion_1_analytical(self):
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

    def benchmark__basic__expansion_1_with_filter_analytical(self):
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

    def benchmark__basic__expansion_2_analytical(self):
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
            case GraphVendors.ARANGODB:
                query = """
                    FOR u IN users FILTER u.id == @id
                    FOR v, e, p IN 1..2 OUTBOUND u friendships
                    RETURN DISTINCT v.id
                """
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__basic__expansion_2_with_filter_analytical(self):
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
            case GraphVendors.ARANGODB:
                query = """
                    FOR u IN users FILTER u.id == @id
                    FOR v, e, p IN 1..2 OUTBOUND u friendships
                    FILTER v.age >= 18
                    RETURN DISTINCT v.id
                """
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__basic__expansion_3_analytical(self):
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
            case GraphVendors.ARANGODB:
                query = """
                    FOR u IN users FILTER u.id == @id
                    FOR v, e, p IN 1..3 OUTBOUND u friendships
                    RETURN DISTINCT v.id
                """
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__basic__expansion_3_with_filter_analytical(self):
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
            case GraphVendors.ARANGODB:
                query = """
                    FOR u IN users FILTER u.id == @id
                    FOR v, e, p IN 1..3 OUTBOUND u friendships
                    FILTER v.age >= 18
                    RETURN DISTINCT v.id
                """
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__basic__expansion_4_analytical(self):
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
            case GraphVendors.ARANGODB:
                query = """
                    FOR u IN users FILTER u.id == @id
                    FOR v, e, p IN 1..4 OUTBOUND u friendships
                    RETURN DISTINCT v.id
                """
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__basic__expansion_4_with_filter_analytical(self):
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
            case GraphVendors.ARANGODB:
                query = """
                    FOR u IN users FILTER u.id == @id
                    FOR v, e, p IN 1..4 OUTBOUND u friendships
                    FILTER v.age >= 18
                    RETURN DISTINCT v.id
                """
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__basic__neighbours_2_analytical(self):
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
            case GraphVendors.ARANGODB:
                query = """
                    FOR u IN users FILTER u.id == @id
                    FOR v, e, p IN 1..2 OUTBOUND u friendships
                    RETURN DISTINCT v.id
                """
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__basic__neighbours_2_with_filter_analytical(self):
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
            case GraphVendors.ARANGODB:
                query = """
                    FOR u IN users FILTER u.id == @id
                    FOR v, e, p IN 1..2 OUTBOUND u friendships
                    FILTER v.age >= 18
                    RETURN DISTINCT v.id
                """
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__basic__neighbours_2_with_data_analytical(self):
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
            case GraphVendors.ARANGODB:
                query = """
                    FOR u IN users FILTER u.id == @id
                    FOR v, e, p IN 1..2 OUTBOUND u friendships
                    RETURN DISTINCT v.id, v
                """
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__basic__neighbours_2_with_data_and_filter_analytical(self):
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
            case GraphVendors.ARANGODB:
                query = """
                    FOR u IN users FILTER u.id == @id
                    FOR v, e, p IN 1..2 OUTBOUND u friendships
                    FILTER v.age >= 18
                    RETURN DISTINCT v.id, v
                """
                params = {"id": vertex_id}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__basic__pattern_cycle_analytical(self):
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

    def benchmark__basic__pattern_long_analytical(self):
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

    def benchmark__basic__pattern_short_analytical(self):
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

    def benchmark__basic__neighbours_3_with_data_and_filter_analytical(self):
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

    def benchmark__basic__neighbours_4_with_data_and_filter_analytical(self):
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
