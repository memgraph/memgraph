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

from constants import GraphVendors
from workloads.base import Workload


class HubEdgeRewrite(Workload):
    """
    Users each linked to every one of a few hubs, so every hub has an in-degree of USERS. Rewriting a user's edges
    removes one edge from each hub's adjacency and appends a new one, which is the shape of a workload that rewrites
    per-user interest edges pointing into shared hub vertices. The dataset keeps its size, so the query can repeat.
    """

    NAME = "hub_edge_rewrite"
    HUBS = 25
    USERS = 200_000
    USERS_PER_IMPORT_QUERY = 2_000

    def indexes_generator(self):
        # The hubs are created here rather than in the dataset: the dataset is imported on several workers, so a
        # query creating them could still be running when the queries linking users to them start.
        return [
            ("CREATE INDEX ON :User(id);", {}),
            ("CREATE INDEX ON :Hub;", {}),
            ("CREATE INDEX ON :Hub(id);", {}),
            ("UNWIND range(1, $hubs) AS id CREATE (:Hub {id: id});", {"hubs": HubEdgeRewrite.HUBS}),
        ]

    def dataset_generator(self):
        queries = []
        for first in range(1, HubEdgeRewrite.USERS + 1, HubEdgeRewrite.USERS_PER_IMPORT_QUERY):
            last = min(first + HubEdgeRewrite.USERS_PER_IMPORT_QUERY - 1, HubEdgeRewrite.USERS)
            # The hubs are matched once per query: matching them for every user would re-read hubs the
            # query keeps modifying, which gets slower with every edge it adds.
            queries.append(
                (
                    "MATCH (h:Hub) WITH collect(h) AS hubs "
                    "UNWIND range($first, $last) AS id CREATE (u:User {id: id}) "
                    "WITH u, hubs UNWIND hubs AS h CREATE (u)-[:LINKED_TO]->(h);",
                    {"first": first, "last": last},
                )
            )
        return queries

    @staticmethod
    def _get_random_user():
        return random.randint(1, HubEdgeRewrite.USERS)

    def benchmark__rewrite__user_hub_edges(self):
        match self._vendor:
            case GraphVendors.MEMGRAPH:
                return (
                    "MATCH (u:User {id: $id})-[r:LINKED_TO]->(:Hub) DELETE r "
                    "WITH DISTINCT u MATCH (h:Hub) CREATE (u)-[:LINKED_TO]->(h);",
                    {"id": self._get_random_user()},
                )
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")
