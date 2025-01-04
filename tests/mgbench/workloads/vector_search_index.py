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

import random

from benchmark_context import BenchmarkContext
from workloads.base import Workload


class VectorSearchIndex(Workload):
    NAME = "vector_search_index"
    NUMBER_OF_NODES = 100
    NUMBER_OF_EDGES = 100
    VECTOR_DIMENSIONS = 128

    def __init__(self, variant: str = None, benchmark_context: BenchmarkContext = None):
        super().__init__(variant, benchmark_context=benchmark_context)
        self._nodes_count = VectorSearchIndex.NUMBER_OF_NODES
        self._edges_count = VectorSearchIndex.NUMBER_OF_EDGES
        random.seed(10)

    def indexes_generator(self):
        return [
            ("CREATE INDEX ON :Node(id);", {}),
            (
                'CREATE VECTOR INDEX index ON :Node(vector) WITH CONFIG {"dimension": %i, "capacity": %i};'
                % (VectorSearchIndex.VECTOR_DIMENSIONS, VectorSearchIndex.NUMBER_OF_NODES),
                {},
            ),
        ]

    def dataset_generator(self):
        queries = []
        # Add nodes because they contain the vectors.
        for i in range(0, self._nodes_count):
            vector = self._get_random_vector()
            queries.append(("CREATE (:Node {id:%i, vector: %s});" % (i, str(vector)), {}))
        # Add edges because we also want to benchmark traversals.
        for i in range(0, self._edges_count):
            a = random.randint(0, self._nodes_count)
            b = random.randint(0, self._nodes_count)
            queries.append(
                (("MATCH (a:Node {id:$A_id}), (b:Node {id:$B_id}) CREATE (a)-[:Edge]->(b);"), {"A_id": a, "B_id": b})
            )
        return queries

    def _get_random_node(self):
        return random.randint(0, self._nodes_count)

    def _get_random_vector(self):
        return [random.random() for _ in range(0, VectorSearchIndex.VECTOR_DIMENSIONS)]

    def benchmark__vector__single_vertex_lookup(self):
        return ("MATCH (n:User {id:$id}) RETURN n;", {"id": self._get_random_node()})

    def benchmark__vector__single_vertex_create(self):
        i = self._nodes_count
        self._nodes_count += 1
        return ("CREATE (:Node {id:%i, vector: $vector});" % (i), {"vector": self._get_random_vector()})

    def benchmark__vector__running_traversals(self):
        # NOTE: Vector is there but we are not returning it, that's on purpose to avoid measuring that part.
        return ("MATCH (n:Node {id:$id})-[*bfs..4]->() RETURN n.id;", {"id": self._get_random_node()})

    def benchmark__vector__single_index_lookup(self):
        return (
            'CALL vector_search.search("index", 10, $query) YIELD * RETURN id(node), distance;',
            {"query": self._get_random_vector()},
        )
