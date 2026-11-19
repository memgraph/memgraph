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
from workloads.base import Workload, fixed_executions


class HubEdgeDeletion(Workload):
    """
    Users each linked to every one of a few hubs, so every hub has an in-degree of USERS. Deleting a user's edges
    removes one edge from each hub's adjacency, which is the shape of a workload that rewrites per-user interest
    edges pointing into shared hub vertices.
    """

    NAME = "hub_edge_deletion"
    HUBS = 25
    USERS = 200_000
    USERS_PER_IMPORT_QUERY = 2_000

    def __init__(self, variant: str = None, benchmark_context=None):
        super().__init__(variant, benchmark_context=benchmark_context)
        self._users_left = None

    def indexes_generator(self):
        # The hubs are created here rather than in the dataset: the dataset is imported on several workers, so a
        # query creating them could still be running when the queries linking users to them start.
        return [
            ("CREATE INDEX ON :User(id);", {}),
            ("CREATE INDEX ON :Hub(id);", {}),
            ("UNWIND range(1, $hubs) AS id CREATE (:Hub {id: id});", {"hubs": HubEdgeDeletion.HUBS}),
        ]

    def dataset_generator(self):
        queries = []
        for first in range(1, HubEdgeDeletion.USERS + 1, HubEdgeDeletion.USERS_PER_IMPORT_QUERY):
            last = min(first + HubEdgeDeletion.USERS_PER_IMPORT_QUERY - 1, HubEdgeDeletion.USERS)
            queries.append(
                (
                    "UNWIND range($first, $last) AS id CREATE (u:User {id: id}) "
                    "WITH u MATCH (h:Hub) CREATE (u)-[:LINKED_TO]->(h);",
                    {"first": first, "last": last},
                )
            )
        return queries

    def reset_generators(self) -> None:
        self._users_left = None

    def _take_random_user(self):
        # Users are drawn without replacement across a batch, so every query deletes edges that still exist.
        if self._users_left is None:
            self._users_left = list(range(1, HubEdgeDeletion.USERS + 1))
            random.shuffle(self._users_left)
        return self._users_left.pop()

    @fixed_executions(1000)
    def benchmark__delete__user_hub_edges(self):
        match self._vendor:
            case GraphVendors.MEMGRAPH:
                return (
                    "MATCH (:User {id: $id})-[r:LINKED_TO]->(:Hub) DELETE r;",
                    {"id": self._take_random_user()},
                )
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")
