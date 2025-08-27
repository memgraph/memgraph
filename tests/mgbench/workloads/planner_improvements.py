# Copyright 2025 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source
# License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

from benchmark_context import BenchmarkContext
from constants import GraphVendors
from workloads.base import Workload
from workloads.importers.importer_pokec import ImporterPokec


class PlannerImprovements(Workload):
    NAME = "planner_improvements"
    VARIANTS = ["small", "medium", "large"]
    DEFAULT_VARIANT = "small"
    FILE = None

    URL_FILE = {
        "small": "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/pokec/benchmark/pokec_small_import.cypher",
        "medium": "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/pokec/benchmark/pokec_medium_import.cypher",
        "large": "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/pokec/benchmark/pokec_large.setup.cypher.gz",
    }

    SIZES = {
        "small": {"vertices": 10000, "edges": 121716},
        "medium": {"vertices": 100000, "edges": 1768515},
        "large": {"vertices": 1632803, "edges": 30622564},
    }

    URL_INDEX_FILE = {
        GraphVendors.MEMGRAPH: "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/pokec/benchmark/memgraph.cypher",
    }

    PROPERTIES_ON_EDGES = False

    def __init__(self, variant: str = None, benchmark_context: BenchmarkContext = None):
        super().__init__(variant, benchmark_context=benchmark_context)

    def custom_import(self, client) -> bool:
        importer = ImporterPokec(
            benchmark_context=self.benchmark_context,
            client=client,
            dataset_name=self.NAME,
            index_file=self._file_index,
            dataset_file=self._file,
            variant=self._variant,
        )
        return importer.execute_import()

    def benchmark__test__indexed_order_by(self):
        # The query facilitates use of index for ORDER BY. Order
        # by in this case can be ommitted because the index itself offers
        # an ordered structure
        match self._vendor:
            case GraphVendors.MEMGRAPH:
                query = "MATCH (u:User) RETURN u.id ORDER BY u.id"
                params = {}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__test__parallel_counting(self):
        # The query needs to facilitate the use of parallel runtime for counting
        match self._vendor:
            case GraphVendors.MEMGRAPH:
                query = "MATCH (u) RETURN count(u)"
                params = {}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params

    def benchmark__test__parallel_counting(self):
        # The query needs to facilitate source expand, and not ST Shortest path
        match self._vendor:
            case GraphVendors.MEMGRAPH:
                query = f"MATCH p=(u:User {id: 1})-[*bfs]-(:User) RETURN count(p)"
                params = {}
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

        return query, params
