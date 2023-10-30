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
    NAME = "cartesian"
    CARDINALITY = 1000

    def indexes_generator(self):
        return [
            ("CREATE INDEX ON :Node;", {}),
            ("CREATE INDEX ON :Node(id);", {}),
        ]

    def dataset_generator(self):
        queries = []
        for i in range(0, Supernode.CARDINALITY):
            queries.append(("CREATE (:Node {id: $id, id2: $id});", {"id": i, "id2": i}))

        return queries

    def benchmark__test__plain_cartesian_filtering(self):
        return ("MATCH (n1:Node), (n2:Node) WHERE n1.id < 100 and n2.id < 100 RETURN n1, n2;", {})

    def benchmark__test__plain_cartesian_join(self):
        return ("MATCH (n1:Node), (n2:Node) WHERE n1.id = n2.id RETURN n1, n2;", {})

    def benchmark__test__plain_cartesian_different_props_join(self):
        return ("MATCH (n1:Node), (n2:Node) WHERE n1.id = n2.id2 RETURN n1, n2;", {})
