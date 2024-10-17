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


class VectorSearchIndex(Workload):
    NAME = "vector_search_index"
    SCALE = 1000
    VECTOR_SIZE = 1500

    def __init__(self, variant: str = None, benchmark_context: BenchmarkContext = None):
        super().__init__(variant, benchmark_context=benchmark_context)
        self._vertex_count = VectorSearchIndex.SCALE
        random.seed(10)

    def indexes_generator(self):
        return [
            ("CREATE INDEX ON :Node(id);", {}),
            ("CREATE INDEX ON :Node(vector);", {}),
        ]

    def dataset_generator(self):
        queries = []
        # Add nodes because they contain the vectors.
        for i in range(0, self._vertex_count):
            vector = [random.random() for _ in range(0, VectorSearchIndex.VECTOR_SIZE)]
            queries.append(("CREATE (:Node {id:%i, vector: %s});" % (i, str(vector)), {}))
        # Add edges because we also want to benchmark traversals.
        # TODO(gitbuda): Add edges because of traversals.
        return queries

    def _get_random_vertex(self):
        return random.randint(1, VectorSearchIndex.SCALE)

    def benchmark__vector__single_vertex_lookup(self):
        return ("MATCH (n:User {id : $id}) RETURN n;", {"id": self._get_random_vertex()})

    def benchmark__vector__single_vertex_create(self):
        vector = [random.random() for _ in range(0, VectorSearchIndex.VECTOR_SIZE)]
        i = self._vertex_count
        self._vertex_count += 1
        return ("CREATE (:Node {id:%i, vector: $vector});" % (i), {"vector": vector})

    def benchmark__vector__running_traversals(self):
        return ("RETURN 1;", {})

    def benchmark__vector__single_index_lookup(self):
        return ("RETURN 1;", {})
