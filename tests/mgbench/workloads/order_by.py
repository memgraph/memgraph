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


class OrderBy(Workload):
    NAME = "order_by"
    CARDINALITY = 10000

    def indexes_generator(self):
        return [
            ("CREATE INDEX ON :Node;", {}),
            ("CREATE INDEX ON :Node(prop1);", {}),
            ("CREATE INDEX ON :Node(prop2);", {}),
            ("CREATE INDEX ON :Node(prop3);", {}),
        ]

    def dataset_generator(self):
        queries = []
        for i in range(0, OrderBy.CARDINALITY):
            queries.append(
                (
                    """CREATE
(:Node {prop1: $id, prop2: $id, prop3: $id
});
""",
                    {"id": i},
                )
            )

        return queries

    def benchmark__test__order_by(self):
        return (
            "MATCH (n:Node) RETURN n ORDER BY n.prop1",
            {},
        )
