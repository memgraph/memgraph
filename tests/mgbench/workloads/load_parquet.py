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

import os
import pathlib

from constants import GraphVendors
from workloads.base import Workload


class LoadParquet(Workload):
    NAME = "load_parquet"
    CARDINALITY = 50000

    # TODO: (andi) Add if you plan to benchmark edges
    def indexes_generator(self):
        return []

    def custom_import(self, client) -> bool:
        return True

    def benchmark__test__import_nodes(self):
        workloads_dir = pathlib.Path(__file__).parent
        parquet_path = workloads_dir / "resources" / "nodes_1m.parquet"
        absolute_path = str(parquet_path.absolute())

        match self._vendor:
            case GraphVendors.MEMGRAPH:
                return (
                    f"USING PERIODIC COMMIT 1024 LOAD PARQUET FROM '{absolute_path}' AS x CREATE (p:Person {{id: x.id, name: x.name, city: x.city, age: x.age}});",
                    {},
                )
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")
