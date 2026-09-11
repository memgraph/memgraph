# Copyright 2024 Memgraph Ltd.
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


class Bench(Workload):
    # Write-isolated read dataset: a single :Bench label with a b-tree index on id, so a bounded
    # id-range scan is served straight from the index. Used for HA routed-read throughput where each
    # query does ~1ms of real server work (unlike a microsecond point lookup), so read load actually
    # reaches the replicas instead of being client-bound.
    NAME = "bench"
    VARIANTS = ["default"]
    DEFAULT_VARIANT = "default"
    SIZES = {"default": {"vertices": 500000, "edges": 0}}

    # ~1ms window: scanning 8000 indexed nodes (calibrated); id range is [0, TOTAL).
    TOTAL = 500000
    WINDOW = 8000

    def indexes_generator(self):
        return [("CREATE INDEX ON :Bench(id);", {})]

    def dataset_generator(self):
        # UNWIND batches so import completes in seconds rather than minutes.
        batch = 10000
        queries = []
        for start in range(0, self.TOTAL, batch):
            end = min(start + batch, self.TOTAL) - 1
            queries.append(
                (
                    "UNWIND range($a, $b) AS i CREATE (:Bench {id: i, x: i % 1000})",
                    {"a": start, "b": end},
                )
            )
        return queries

    def benchmark__scan__range_read(self):
        lo = random.randint(0, self.TOTAL - self.WINDOW)
        return (
            "MATCH (n:Bench) WHERE n.id >= $lo AND n.id < $lo + $w RETURN sum(n.x)",
            {"lo": lo, "w": self.WINDOW},
        )
