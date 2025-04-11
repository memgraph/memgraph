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
    SIZES = {
        "small": {"vertices": 10000, "edges": 121716},
        "medium": {"vertices": 100000, "edges": 1768515},
        "large": {"vertices": 1632803, "edges": 30622564},
    }

    URL_INDEX_FILE = {
        "memgraph": "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/pokec/benchmark/memgraph.cypher",
        "neo4j": "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/pokec/benchmark/neo4j.cypher",
        "falkordb": "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/pokec/benchmark/falkordb.cypher",
    }

    PROPERTIES_ON_EDGES = False

    def __init__(self, variant: str = None, benchmark_context: BenchmarkContext = None):
        super().__init__(variant, benchmark_context=benchmark_context)

    def custom_import(self) -> bool:
        importer = ImporterPokec(
            benchmark_context=self.benchmark_context,
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

    def benchmark__basic__shortest_path_with_filter(self):
        vertex_from, vertex_to = self._get_random_from_to()
        memgraph = (
            "MATCH (n:User {id: $from}), (m:User {id: $to}) WITH n, m "
            "MATCH p=(n)-[*bfs..15 (e, n | n.age >= 18)]->(m) "
            "RETURN extract(n in nodes(p) | n.id) AS path",
            {"from": vertex_from, "to": vertex_to},
        )

        neo4j = (
            "MATCH (n:User {id: $from}), (m:User {id: $to}) WITH n, m "
            "MATCH p=shortestPath((n)-[*..15]->(m)) "
            "WHERE all(node in nodes(p) WHERE node.age >= 18) "
            "RETURN [n in nodes(p) | n.id] AS path",
            {"from": vertex_from, "to": vertex_to},
        )
        if self._vendor == "memgraph":
            return memgraph
        elif self._vendor in ["neo4j", "falkordb"]:
            return neo4j
        raise Exception(f"Unknown vendor type {self._vendor}")

    def benchmark__basic__neighbours_3_with_data_and_filter_analytical(self):
        return (
            "MATCH (s:User {id: $id})-[*1..3]->(n:User) " "WHERE n.age >= 18 " "RETURN DISTINCT n.id, n",
            {"id": self._get_random_vertex()},
        )

    def benchmark__basic__neighbours_4_with_data_and_filter_analytical(self):
        return (
            "MATCH (s:User {id: $id})-[*1..4]->(n:User) " "WHERE n.age >= 18 " "RETURN DISTINCT n.id, n",
            {"id": self._get_random_vertex()},
        )

    def benchmark__basic__neighbours_4_with_data_and_inline_filter_analytical(self):
        return (
            "MATCH (p:User {id: $id})-->(q:User )-->(r:User)-->(s:User) "
            "WHERE p.age >= 18 AND q.age >= 18 AND r.age >= 18 AND s.age >= 18"
            "RETURN DISTINCT n.id, n",
            {"id": self._get_random_vertex()},
        )
