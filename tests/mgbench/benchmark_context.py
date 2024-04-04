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

from workload_mode import (
    BENCHMARK_MODE_ISOLATED,
    BENCHMARK_MODE_MIXED,
    BENCHMARK_MODE_REALISTIC,
)


class BenchmarkContext:
    """
    Class for holding information on what type of benchmark is being executed
    """

    def _get_vendor(self, name):
        if name.startswith("memgraph"):
            return "memgraph"
        if name.startswith("neo4j"):
            return "neo4j"
        return name

    def __init__(
        self,
        benchmark_target_workload: str = None,  # Workload that needs to be executed (dataset/variant/group/query)
        vendor_binary: str = None,
        vendor_name: str = None,
        client_binary: str = None,
        num_workers_for_import: int = None,
        num_workers_for_benchmark: int = None,
        single_threaded_runtime_sec: int = 0,
        query_count_lower_bound: int = 0,
        no_load_query_counts: bool = False,
        no_save_query_counts: bool = False,
        export_results: str = None,
        temporary_directory: str = None,
        workload_mixed: str = None,  # Default mode is isolated, mixed None
        workload_realistic: str = None,  # Default mode is isolated, realistic None
        time_dependent_execution: int = 0,
        warm_up: str = None,
        performance_tracking: bool = False,
        no_authorization: bool = True,
        customer_workloads: str = None,
        vendor_args: dict = {},
    ) -> None:
        self.benchmark_target_workload = benchmark_target_workload
        self.vendor_binary = vendor_binary
        self.vendor_name = vendor_name
        self.vendor = self._get_vendor(vendor_name)
        self.client_binary = client_binary
        self.num_workers_for_import = num_workers_for_import
        self.num_workers_for_benchmark = num_workers_for_benchmark
        self.single_threaded_runtime_sec = single_threaded_runtime_sec
        self.query_count_lower_bound = query_count_lower_bound
        self.no_load_query_counts = no_load_query_counts
        self.no_save_query_counts = no_save_query_counts
        self.export_results = export_results
        self.temporary_directory = temporary_directory

        assert (
            workload_mixed is None or workload_realistic is None
        ), "Cannot run both mixed and realistic workload, please select one!"

        if workload_mixed != None:
            self.mode = BENCHMARK_MODE_MIXED
            self.mode_config = workload_mixed
        elif workload_realistic != None:
            self.mode = BENCHMARK_MODE_REALISTIC
            self.mode_config = workload_realistic
        else:
            self.mode = BENCHMARK_MODE_ISOLATED
            self.mode_config = None

        self.time_dependent_execution = time_dependent_execution
        self.performance_tracking = performance_tracking
        self.warm_up = warm_up
        self.no_authorization = no_authorization
        self.customer_workloads = customer_workloads
        self.vendor_args = vendor_args
        self.active_workload = None
        self.active_variant = None

    def set_active_workload(self, workload: str) -> None:
        self.active_workload = workload

    def get_active_workload(self) -> str:
        return self.active_workload

    def set_active_variant(self, variant: str) -> None:
        self.active_variant = variant

    def get_active_variant(self) -> str:
        return self.active_variant
