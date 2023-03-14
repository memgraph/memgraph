# Describes all the information of single benchmark.py run.
class BenchmarkContext:

    """
    Class for holding information on what type of benchmark is being executed
    """

    def __init__(
        self,
        benchmark_target_workload: str = None,  # Workload that needs to be executed (dataset/variant/group/query)
        vendor_context: str = None,  # Benchmark vendor context(binary, folder, DB Runner)
        vendor_name: str = None,
        client_binary: str = None,
        num_workers_for_import: int = None,
        num_workers_for_benchmark: int = None,
        single_threaded_runtime_sec: int = 0,
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
    ) -> None:

        self.benchmark_target_workload = benchmark_target_workload
        self.vendor_context = vendor_context
        self.vendor_name = vendor_name
        self.client_binary = client_binary
        self.num_workers_for_import = num_workers_for_import
        self.num_workers_for_benchmark = num_workers_for_benchmark
        self.single_threaded_runtime_sec = single_threaded_runtime_sec
        self.no_load_query_counts = no_load_query_counts
        self.no_save_query_counts = no_save_query_counts
        self.export_results = export_results
        self.temporary_directory = temporary_directory

        if workload_mixed != None:
            self.mode = "Mixed"
            self.mode_config = workload_mixed
        elif workload_realistic != None:
            self.mode = "Realistic"
            self.mode_config = workload_realistic
        else:
            self.mode = "Isolated"
            self.mode_config = "Isolated run does not have a config."

        self.time_dependent_execution = time_dependent_execution
        self.performance_tracking = performance_tracking
        self.warm_up = warm_up
        self.no_authorization = no_authorization
