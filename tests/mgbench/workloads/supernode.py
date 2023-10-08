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


class Supernode(Workload):
    NAME = "supernode"
    CARDINALITY = 50000

    def indexes_generator(self):
        return [
            ("CREATE INDEX ON :Supernode;", {}),
            ("CREATE INDEX ON :Supernode(id);", {}),
            ("CREATE INDEX ON :Node;", {}),
            ("CREATE INDEX ON :Node(id);", {}),
        ]

    def dataset_generator(self):
        queries = []
        queries.append(("CREATE (:Supernode {id: $id});", {"id": 1}))
        for i in range(0, Supernode.CARDINALITY):
            queries.append(("CREATE (:Node {id: $id});", {"id": i}))
        queries.append(("MATCH (s:Supernode), (n:Node) CREATE (s)<-[:EDGE]-(n)", {}))

        return queries

    def benchmark__test__merge_supernode_edges(self):
        return ("MATCH (s:Supernode), (n:Node) MERGE (s)<-[:EDGE]-(n);", {})

    def benchmark__test__merge_supernode_edges_other_way(self):
        return ("MATCH (s:Supernode), (n:Node) MERGE (n)-[:EDGE]->(s);", {})

    def benchmark__test__unwind_supernode_with_writes(self):
        return (f"UNWIND range(1, {Supernode.CARDINALITY}) AS x MATCH (s:Supernode) SET s.prop = x", {})
