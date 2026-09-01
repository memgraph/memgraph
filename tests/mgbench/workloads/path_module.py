# Copyright 2026 Memgraph Ltd.
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


# Measures `path.expand` and `path.subgraph_nodes` on a hub-and-spoke graph whose edges all point
# towards the hub, where a directed filter admits only the outgoing side.
#
# The `near_hub` pair is the one that guards the optimisation: it starts a hop out, so the walk
# reaches the hub at depth 1 and skips its whole incoming list while still traversing. The queries
# starting *at* the hub skip that list at depth 0 and then find an empty outgoing list, so they
# report the saving but touch no edges afterwards; they cannot detect a later regression on their
# own. The undirected variants are the controls: both directions are needed, so they run the
# unchanged code path.
class PathModule(Workload):
    NAME = "path_module"
    VARIANTS = ["small", "medium", "large"]
    DEFAULT_VARIANT = "medium"

    SIZES = {
        "small": {"vertices": 1001, "edges": 3000},
        "medium": {"vertices": 10001, "edges": 30000},
        "large": {"vertices": 100001, "edges": 300000},
    }

    def __init__(self, variant: str = None, benchmark_context: BenchmarkContext = None):
        super().__init__(variant, benchmark_context=benchmark_context)
        self._spokes = self._size["vertices"] - 1

    def indexes_generator(self):
        return [
            ("CREATE INDEX ON :Hub(id);", {}),
            ("CREATE INDEX ON :Spoke(id);", {}),
        ]

    def dataset_generator(self):
        queries = [("CREATE (:Hub {id: 0});", {})]
        for i in range(0, self._spokes):
            queries.append(("CREATE (:Spoke {id: $id});", {"id": i}))

        # All spokes point at the hub, so the hub's incoming list holds every edge and its outgoing
        # list is empty. An `INNER>` filter out of the hub must therefore reject the whole incoming
        # list, which is the case this benchmark exists to measure.
        queries.append(("MATCH (h:Hub), (s:Spoke) CREATE (s)-[:INNER]->(h);", {}))

        # A second layer of directed edges between spokes gives the traversal somewhere to go after
        # the first hop, so the deeper levels are not all dead ends.
        for _ in range(0, self._size["edges"]):
            queries.append(
                (
                    "MATCH (a:Spoke {id: $a}), (b:Spoke {id: $b}) CREATE (a)-[:OUTER]->(b);",
                    {"a": self._get_random_spoke(), "b": self._get_random_spoke()},
                )
            )
        return queries

    def _get_random_spoke(self):
        return random.randint(0, self._spokes - 1)

    def benchmark__directional__subgraph_nodes_directed(self):
        match self._vendor:
            case GraphVendors.MEMGRAPH:
                return (
                    "MATCH (h:Hub {id: 0}) CALL path.subgraph_nodes(h, "
                    '{relationshipFilter: ["INNER>", "OUTER>"], maxLevel: 3}) '
                    "YIELD nodes RETURN count(nodes);",
                    {},
                )
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

    def benchmark__directional__subgraph_nodes_undirected(self):
        match self._vendor:
            case GraphVendors.MEMGRAPH:
                return (
                    "MATCH (h:Hub {id: 0}) CALL path.subgraph_nodes(h, "
                    '{relationshipFilter: ["INNER", "OUTER"], maxLevel: 3}) '
                    "YIELD nodes RETURN count(nodes);",
                    {},
                )
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

    def benchmark__directional__expand_directed(self):
        match self._vendor:
            case GraphVendors.MEMGRAPH:
                return (
                    "MATCH (h:Hub {id: 0}) CALL path.expand(h, "
                    '["INNER>", "OUTER>"], [], 1, 3) '
                    "YIELD result RETURN count(result);",
                    {},
                )
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

    def benchmark__directional__expand_undirected(self):
        match self._vendor:
            case GraphVendors.MEMGRAPH:
                return (
                    "MATCH (h:Hub {id: 0}) CALL path.expand(h, "
                    '["INNER", "OUTER"], [], 1, 3) '
                    "YIELD result RETURN count(result);",
                    {},
                )
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

    # Starting one hop away from the hub: the first step crosses INNER to the hub, so the walk still
    # meets the supernode, just at depth 1 rather than depth 0. Unlike the queries that start at the
    # hub, this one keeps traversing afterwards, so it is the pair that a regression would move.
    def benchmark__directional__subgraph_nodes_near_hub(self):
        match self._vendor:
            case GraphVendors.MEMGRAPH:
                return (
                    "MATCH (s:Spoke {id: $id}) CALL path.subgraph_nodes(s, "
                    '{relationshipFilter: ["INNER>", "OUTER>"], maxLevel: 3}) '
                    "YIELD nodes RETURN count(nodes);",
                    {"id": self._get_random_spoke()},
                )
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

    def benchmark__directional__subgraph_nodes_near_hub_undirected(self):
        match self._vendor:
            case GraphVendors.MEMGRAPH:
                return (
                    "MATCH (s:Spoke {id: $id}) CALL path.subgraph_nodes(s, "
                    '{relationshipFilter: ["INNER", "OUTER"], maxLevel: 3}) '
                    "YIELD nodes RETURN count(nodes);",
                    {"id": self._get_random_spoke()},
                )
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")
