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


class HighWriteSetProperty(Workload):
    NAME = "high_write_set_property"
    CARDINALITY = 100000

    def indexes_generator(self):
        return [
            ("CREATE INDEX ON :Node;", {}),
            ("CREATE INDEX ON :Node(prop1);", {}),
            ("CREATE INDEX ON :Node(prop2);", {}),
            ("CREATE INDEX ON :Node(prop3);", {}),
            ("CREATE INDEX ON :Node(prop4);", {}),
            ("CREATE INDEX ON :Node(prop5);", {}),
            ("CREATE INDEX ON :Node(prop6);", {}),
            ("CREATE INDEX ON :Node(prop7);", {}),
            ("CREATE INDEX ON :Node(prop8);", {}),
            ("CREATE INDEX ON :Node(prop9);", {}),
            ("CREATE INDEX ON :Node(prop10);", {}),
            ("CREATE INDEX ON :Node(prop11);", {}),
            ("CREATE INDEX ON :Node(prop12);", {}),
            ("CREATE INDEX ON :Node(prop13);", {}),
            ("CREATE INDEX ON :Node(prop14);", {}),
            ("CREATE INDEX ON :Node(prop15);", {}),
            ("CREATE INDEX ON :Node(prop16);", {}),
            ("CREATE INDEX ON :Node(prop17);", {}),
            ("CREATE INDEX ON :Node(prop18);", {}),
            ("CREATE INDEX ON :Node(prop19);", {}),
            ("CREATE INDEX ON :Node(prop20);", {}),
        ]

    def dataset_generator(self):
        queries = []
        for i in range(0, HighWriteSetProperty.CARDINALITY):
            queries.append(
                (
                    """CREATE
(:Node {prop1: $id, prop2: $id, prop3: $id,
prop4: $id, prop5: $id, prop6: $id, prop7: $id, prop8: $id,
prop9: $id, prop10: $id, prop11: $id, prop12: $id, prop13: $id,
prop14: $id, prop15: $id, prop16: $id, prop17: $id, prop18: $id,
prop19: $id, prop20: $id});
""",
                    {"id": i},
                )
            )

        return queries

    def benchmark__test__write_20_attributes_set_property(self):
        return (
            "MATCH (n:Node) SET n.prop1 = n.prop2, n.prop2 = n.prop3, n.prop3 = n.prop4, n.prop4 = n.prop5, n.prop5 = n.prop6, n.prop6 = n.prop7, n.prop7 = n.prop8, n.prop8 = n.prop9, n.prop9 = n.prop10, n.prop10 = n.prop11, n.prop11 = n.prop12, n.prop12 = n.prop13, n.prop13 = n.prop14, n.prop14 = n.prop15, n.prop15 = n.prop16, n.prop16 = n.prop17, n.prop17 = n.prop18, n.prop18 = n.prop19, n.prop19 = n.prop20, n.prop20 = n.prop1;",
            {},
        )

    def benchmark__test__write_20_attributes_set_properties(self):
        return (
            "MATCH (n:Node) SET n += {prop1: n.prop2, prop2: n.prop3, prop3: n.prop4, prop4: n.prop5, prop5: n.prop6, prop6: n.prop7, prop7: n.prop8, prop8: n.prop9, prop9: n.prop10, prop10: n.prop11, prop11: n.prop12, prop12: n.prop13, prop13: n.prop14, prop14: n.prop15, prop15: n.prop16, prop16: n.prop17, prop17: n.prop18, prop18: n.prop19, prop19: n.prop20, prop20: n.prop1};",
            {},
        )
