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

from workloads.base import Workload


class Demo(Workload):
    NAME = "demo"

    def indexes_generator(self):
        indexes = []
        if "neo4j" in self.benchmark_context.vendor_name:
            indexes.extend(
                [
                    ("CREATE INDEX FOR (n:NodeA) ON (n.id);", {}),
                    ("CREATE INDEX FOR (n:NodeB) ON (n.id);", {}),
                ]
            )
        else:
            indexes.extend(
                [
                    ("CREATE INDEX ON :NodeA(id);", {}),
                    ("CREATE INDEX ON :NodeB(id);", {}),
                ]
            )
        return indexes

    def dataset_generator(self):
        queries = []
        for i in range(0, 10000):
            queries.append(("CREATE (:NodeA {id: $id});", {"id": i}))
            queries.append(("CREATE (:NodeB {id: $id});", {"id": i}))
        for i in range(0, 50000):
            a = random.randint(0, 9999)
            b = random.randint(0, 9999)
            queries.append(
                (("MATCH(a:NodeA {id: $A_id}),(b:NodeB{id: $B_id}) CREATE (a)-[:EDGE]->(b)"), {"A_id": a, "B_id": b})
            )

        return queries

    def benchmark__test__get_nodes(self):
        return ("MATCH (n) RETURN n;", {})

    def benchmark__test__get_node_by_id(self):
        return ("MATCH (n:NodeA{id: $id}) RETURN n;", {"id": random.randint(0, 9999)})
