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

from workloads.base import Workload


class VectorSearchIndex(Workload):
    NAME = "vector_search_index"
    SCALE = 10
    VECTOR_SIZE = 1500

    def indexes_generator(self):
        return [
            # TODO(gitbuda): This is just an example.
            ("CREATE INDEX ON :Node(id);", {}),
            ("CREATE INDEX ON :Node(vector);", {}),
        ]

    def dataset_generator(self):
        queries = []
        for i in range(0, VectorSearchIndex.SCALE):
            vector = [float(i) for i in range(0, VectorSearchIndex.VECTOR_SIZE)]
            queries.append(("CREATE (:Node {id:%i, vector: %s});" % (i, str(vector)), {}))
        return queries

    def benchmark__test__match_all_nodes(self):
        return ("MATCH (n:Node) RETURN n;", {})
