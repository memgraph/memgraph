#!/usr/bin/env python3

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

import argparse
import json
import multiprocessing
import pathlib
import platform
import random
import sys
import time
from copy import deepcopy
from typing import Dict, List

import helpers
import log
import runners
import setup
from benchmark_context import BenchmarkContext
from benchmark_results import BenchmarkResults
from constants import *
from workload_mode import BENCHMARK_MODE_MIXED, BENCHMARK_MODE_REALISTIC
from workloads import *

WARMUP_TO_HOT_QUERIES = [
    ("CREATE ();", {}),
    ("CREATE ()-[:TempEdge]->();", {}),
    ("MATCH (n) RETURN count(n.prop) LIMIT 1;", {}),
]

SETUP_AUTH_QUERIES = [
    ("CREATE USER user IDENTIFIED BY 'test';", {}),
    ("GRANT ALL PRIVILEGES TO user;", {}),
    ("GRANT CREATE_DELETE ON EDGE_TYPES * TO user;", {}),
    ("GRANT CREATE_DELETE ON LABELS * TO user;", {}),
]

CLEANUP_AUTH_QUERIES = [
    ("REVOKE LABELS * FROM user;", {}),
    ("REVOKE EDGE_TYPES * FROM user;", {}),
    ("DROP USER user;", {}),
]

SETUP_DISK_STORAGE = [
    ("STORAGE MODE ON_DISK_TRANSACTIONAL;", {}),
]

SETUP_IN_MEMORY_ANALYTICAL_STORAGE_MODE = [
    ("STORAGE MODE IN_MEMORY_ANALYTICAL;", {}),
]


def parse_args():
    parser = argparse.ArgumentParser(description="Main parser.", add_help=False)
    benchmark_parser = argparse.ArgumentParser(description="Benchmark arguments parser", add_help=False)

    benchmark_parser.add_argument(
        "benchmarks",
        nargs="*",
        default=None,
        help="descriptions of benchmarks that should be run; "
        "multiple descriptions can be specified to run multiple "
        "benchmarks; the description is specified as "
        "dataset/variant/group/query; Unix shell-style wildcards "
        "can be used in the descriptions; variant, group and query "
        "are optional and they can be left out; the default "
        "variant is '' which selects the default dataset variant; "
        "the default group is '*' which selects all groups; the"
        "default query is '*' which selects all queries",
    )

    benchmark_parser.add_argument(
        "--num-workers-for-import",
        type=int,
        default=multiprocessing.cpu_count() // 2,
        help="number of workers used to import the dataset",
    )
    benchmark_parser.add_argument(
        "--num-workers-for-benchmark",
        type=int,
        default=1,
        help="number of workers used to execute the benchmark",
    )
    benchmark_parser.add_argument(
        "--single-threaded-runtime-sec",
        type=int,
        default=10,
        help="single threaded duration of each query",
    )
    benchmark_parser.add_argument(
        "--query-count-lower-bound",
        type=int,
        default=30,
        help="Lower bound for query count, minimum number of queries that will be executed. If approximated --single-threaded-runtime-sec query count is lower than this value, lower bound is used.",
    )
    benchmark_parser.add_argument(
        "--no-load-query-counts",
        action="store_true",
        default=False,
        help="disable loading of cached query counts",
    )
    benchmark_parser.add_argument(
        "--no-save-query-counts",
        action="store_true",
        default=False,
        help="disable storing of cached query counts",
    )

    benchmark_parser.add_argument(
        "--export-results",
        default=None,
        help="file path into which results for in_memory_transactional storage mode should be exported",
    )

    benchmark_parser.add_argument(
        "--export-results-in-memory-analytical",
        default=None,
        help="File path into which results for in_memory_analytical storage mode should be exported. If set, benchmarks for analytical mode will be run.",
    )

    benchmark_parser.add_argument(
        "--export-results-on-disk-txn",
        default=None,
        help="File path into which results for on_disk_transactional storage mode should be exported. If set, benchmarks for disk storage will be run.",
    )

    benchmark_parser.add_argument(
        "--no-authorization",
        action="store_false",
        default=True,
        help="Run each query with authorization",
    )

    benchmark_parser.add_argument(
        "--warm-up",
        default="cold",
        choices=["cold", "hot", "vulcanic"],
        help="Run different warmups before benchmarks sample starts",
    )

    benchmark_parser.add_argument(
        "--workload-realistic",
        nargs="*",
        type=int,
        default=None,
        help="""Define combination that defines the realistic workload.
        Realistic workload can be run as a single configuration for all groups of queries,
        Pass the positional arguments as values of what percentage of
        write/read/update/analytical queries you want to have in your workload.
        Example:  --workload-realistic 1000 20 70 10 0 will execute 1000 queries, 20% write,
        70% read, 10% update and 0% analytical.""",
    )

    benchmark_parser.add_argument(
        "--workload-mixed",
        nargs="*",
        type=int,
        default=None,
        help="""Mixed workload can be run on each query under some defined load.
        By passing one more positional argument, you are defining what percentage of that query
        will be in mixed workload, and this is executed for each query. The rest of the queries will be
        selected from the appropriate groups
        Running --mixed-workload 1000 30 0 0 0 70, will execute each query 700 times or 70%,
        with the presence of 300 write queries from write type or 30%""",
    )

    benchmark_parser.add_argument(
        "--time-depended-execution",
        type=int,
        default=0,
        help="Execute defined number of queries (based on single-threaded-runtime-sec) for a defined duration in of wall-clock time",
    )

    benchmark_parser.add_argument(
        "--performance-tracking",
        action="store_true",
        default=False,
        help="Flag for runners performance tracking, this logs RES through time and vendor specific performance tracking.",
    )

    benchmark_parser.add_argument("--customer-workloads", default=None, help="Path to customers workloads")

    benchmark_parser.add_argument(
        "--vendor-specific",
        nargs="*",
        default=[],
        help="Vendor specific arguments that can be applied to each vendor, format: [key=value, key=value ...]",
    )

    subparsers = parser.add_subparsers(help="Subparsers", dest="run_option")

    parser_vendor_native = subparsers.add_parser(
        "vendor-native",
        help="Running database in binary native form",
        parents=[benchmark_parser],
    )
    parser_vendor_native.add_argument(
        "--vendor-name",
        default="memgraph",
        choices=["memgraph", "neo4j"],
        help="Input vendor binary name (memgraph, neo4j)",
    )
    parser_vendor_native.add_argument(
        "--vendor-binary",
        help="Vendor binary used for benchmarking, by default it is memgraph",
        default=helpers.get_binary_path("memgraph"),
    )

    parser_vendor_native.add_argument(
        "--client-binary",
        default=helpers.get_binary_path("tests/mgbench/client"),
        help="Client binary used for benchmarking",
    )

    parser_vendor_docker = subparsers.add_parser(
        "vendor-docker", help="Running database in docker", parents=[benchmark_parser]
    )
    parser_vendor_docker.add_argument(
        "--vendor-name",
        default="memgraph",
        choices=["memgraph-docker", "neo4j-docker"],
        help="Input vendor name to run in docker (memgraph-docker, neo4j-docker)",
    )

    return parser.parse_args()


def sanitize_args(args):
    assert args.benchmarks != None, helpers.list_available_workloads()
    assert args.num_workers_for_import > 0
    assert args.num_workers_for_benchmark > 0
    assert args.export_results != None, "Pass where will results be saved"
    assert args.single_threaded_runtime_sec >= 1, "Low runtime value, consider extending time for more accurate results"
    assert (
        args.workload_realistic == None or args.workload_mixed == None
    ), "Cannot run both realistic and mixed workload, only one mode run at the time"


def get_queries(gen, count):
    random.seed(gen.__name__)
    ret = []
    for _ in range(count):
        ret.append(gen())
    return ret


def validate_workload_distribution(percentage_distribution, queries_by_type):
    percentages_by_type = {
        WRITE_TYPE_QUERY: percentage_distribution[0],
        READ_TYPE_QUERY: percentage_distribution[1],
        UPDATE_TYPE_QUERY: percentage_distribution[2],
        ANALYTICAL_TYPE_QUERY: percentage_distribution[3],
    }

    for key, percentage in percentages_by_type.items():
        if percentage != 0 and len(queries_by_type[key]) == 0:
            raise Exception(
                "There is a missing query in group (write, read, update or analytical) for given workload distribution."
            )


def prepare_for_workload(benchmark_context, dataset, group, queries):
    num_of_queries = benchmark_context.mode_config[0]
    percentage_distribution = benchmark_context.mode_config[1:]
    if sum(percentage_distribution) != 100:
        raise Exception(
            "Please make sure that passed arguments % sum to 100% percent!, passed: ",
            percentage_distribution,
        )
    s = [str(i) for i in benchmark_context.mode_config]
    config_distribution = "_".join(s)

    queries_by_type = {
        WRITE_TYPE_QUERY: [],
        READ_TYPE_QUERY: [],
        UPDATE_TYPE_QUERY: [],
        ANALYTICAL_TYPE_QUERY: [],
    }

    for _, funcname in queries[group]:
        for key in queries_by_type.keys():
            if key in funcname:
                queries_by_type[key].append(funcname)

    percentages_by_type = {
        "write": percentage_distribution[0],
        "read": percentage_distribution[1],
        "update": percentage_distribution[2],
        "analytical": percentage_distribution[3],
    }

    for key, percentage in percentages_by_type.items():
        if percentage != 0 and len(queries_by_type[key]) == 0:
            raise Exception(
                "There is a missing query in group (write, read, update or analytical) for given workload distribution."
            )

    validate_workload_distribution(percentage_distribution, queries_by_type)
    random.seed(config_distribution)

    return config_distribution, queries_by_type, percentages_by_type, percentage_distribution, num_of_queries


def realistic_workload(
    vendor: runners.BaseRunner,
    client: runners.BaseClient,
    dataset,
    group,
    queries,
    benchmark_context: BenchmarkContext,
    results,
):
    log.log("Executing realistic workload...")
    config_distribution, queries_by_type, _, percentage_distribution, num_of_queries = prepare_for_workload(
        benchmark_context, dataset, group, queries
    )

    options = [WRITE_TYPE_QUERY, READ_TYPE_QUERY, UPDATE_TYPE_QUERY, ANALYTICAL_TYPE_QUERY]
    function_type = random.choices(population=options, weights=percentage_distribution, k=num_of_queries)

    prepared_queries = []
    for t in function_type:
        # Get the appropriate functions with same probability
        funcname = random.choices(queries_by_type[t], k=1)[0]
        additional_query = getattr(dataset, funcname)
        prepared_queries.append(additional_query())

    rss_db = dataset.NAME + dataset.get_variant() + "_" + "realistic" + "_" + config_distribution
    vendor.start_db(rss_db)
    warmup(benchmark_context.warm_up, client=client)

    ret = client.execute(
        queries=prepared_queries,
        num_workers=benchmark_context.num_workers_for_benchmark,
    )[0]

    usage_workload = vendor.stop_db(rss_db)

    realistic_workload_res = {
        COUNT: ret[COUNT],
        DURATION: ret[DURATION],
        RETRIES: ret[RETRIES],
        THROUGHPUT: ret[THROUGHPUT],
        NUM_WORKERS: ret[NUM_WORKERS],
        DATABASE: usage_workload,
    }
    results_key = [
        dataset.NAME,
        dataset.get_variant(),
        group,
        config_distribution,
        WITHOUT_FINE_GRAINED_AUTHORIZATION,
    ]
    results.set_value(*results_key, value=realistic_workload_res)


def mixed_workload(
    vendor: runners.BaseRunner,
    client: runners.BaseClient,
    dataset,
    group,
    queries,
    benchmark_context: BenchmarkContext,
    results,
):
    log.log("Executing mixed workload...")
    (
        config_distribution,
        queries_by_type,
        percentages_by_type,
        percentage_distribution,
        num_of_queries,
    ) = prepare_for_workload(benchmark_context, dataset, group, queries)

    options = [WRITE_TYPE_QUERY, READ_TYPE_QUERY, UPDATE_TYPE_QUERY, ANALYTICAL_TYPE_QUERY, QUERY]

    for query, funcname in queries[group]:
        log.info(
            "Running query in mixed workload: {}/{}/{}".format(
                group,
                query,
                funcname,
            ),
        )
        base_query_type = funcname.rsplit("_", 1)[1]
        if percentages_by_type.get(base_query_type, 0) > 0:
            continue

        function_type = random.choices(population=options, weights=percentage_distribution, k=num_of_queries)

        prepared_queries = []
        base_query = getattr(dataset, funcname)
        for t in function_type:
            if t == QUERY:
                prepared_queries.append(base_query())
            else:
                funcname = random.choices(queries_by_type[t], k=1)[0]
                additional_query = getattr(dataset, funcname)
                prepared_queries.append(additional_query())

        rss_db = dataset.NAME + dataset.get_variant() + "_" + "mixed" + "_" + query + "_" + config_distribution
        vendor.start_db(rss_db)
        warmup(benchmark_context.warm_up, client=client)

        ret = client.execute(
            queries=prepared_queries,
            num_workers=benchmark_context.num_workers_for_benchmark,
        )[0]

        usage_workload = vendor.stop_db(rss_db)

        ret[DATABASE] = usage_workload

        results_key = [
            dataset.NAME,
            dataset.get_variant(),
            group,
            query + "_" + config_distribution,
            WITHOUT_FINE_GRAINED_AUTHORIZATION,
        ]
        results.set_value(*results_key, value=ret)


def warmup(condition: str, client: runners.BaseRunner, queries: list = None):
    if condition == DATABASE_CONDITION_HOT:
        log.log("Execute warm-up to match condition: {} ".format(condition))
        client.execute(
            queries=WARMUP_TO_HOT_QUERIES,
            num_workers=1,
        )
    elif condition == DATABASE_CONDITION_VULCANIC:
        log.log("Execute warm-up to match condition: {} ".format(condition))
        client.execute(queries=queries)
    else:
        log.log("No warm-up on condition: {} ".format(condition))
    log.log("Finished warm-up procedure to match database condition: {} ".format(condition))


def get_query_cache_count(
    vendor: runners.BaseRunner,
    client: runners.BaseClient,
    queries: List,
    benchmark_context: BenchmarkContext,
    workload: str,
    group: str,
    query: str,
    func: str,
):
    log.init(
        f"Determining query count for benchmark based on --single-threaded-runtime argument = {benchmark_context.single_threaded_runtime_sec}s"
    )
    config_key = [workload.NAME, workload.get_variant(), group, query]
    cached_count = config.get_value(*config_key)
    if cached_count is None:
        vendor.start_db(CACHE)
        client.execute(queries=queries, num_workers=1)
        count = 1
        while True:
            ret = client.execute(queries=get_queries(func, count), num_workers=1)
            duration = ret[0][DURATION]
            should_execute = int(benchmark_context.single_threaded_runtime_sec / (duration / count))
            log.log(
                "Executed_queries={}, total_duration={}, query_duration={}, estimated_count={}".format(
                    count, duration, duration / count, should_execute
                )
            )
            if should_execute / (count * 10) < 10:
                count = should_execute
                break
            else:
                count = count * 10

        vendor.stop_db(CACHE)

        if count < benchmark_context.query_count_lower_bound:
            count = benchmark_context.query_count_lower_bound

        config.set_value(
            *config_key,
            value={
                COUNT: count,
                DURATION: benchmark_context.single_threaded_runtime_sec,
            },
        )
    else:
        log.log(
            "Using cached query count of {} queries for {} seconds of single-threaded runtime to extrapolate .".format(
                cached_count[COUNT], cached_count[DURATION]
            ),
        )
        count = int(cached_count[COUNT] * benchmark_context.single_threaded_runtime_sec / cached_count[DURATION])
    return count


def check_benchmark_requirements(benchmark_context):
    if setup.check_requirements(benchmark_context):
        log.success("Requirements for starting benchmark satisfied!")
    else:
        log.warning("Requirements for starting benchmark not satisfied!")
        sys.exit(1)


def setup_cache_config(benchmark_context, cache):
    if not benchmark_context.no_load_query_counts:
        log.log("Using previous cached query count data from cache directory.")
        return cache.load_config()
    else:
        return helpers.RecursiveDict()


def save_import_results(workload, results, import_results, rss_usage):
    log.info("Summarized importing benchmark results:")
    import_key = [workload.NAME, workload.get_variant(), IMPORT]
    if import_results != None and rss_usage != None:
        # Display import statistics.
        for row in import_results:
            log.success(
                f"Executed {row[COUNT]} queries in {row[DURATION]} seconds using {row[NUM_WORKERS]} workers with a total throughput of {row[THROUGHPUT]} Q/S."
            )

        log.success(
            f"The database used {rss_usage[CPU]} seconds of CPU time and peaked at {rss_usage[MEMORY] / (1024 * 1024)} MiB of RAM"
        )

        results.set_value(*import_key, value={CLIENT: import_results, DATABASE: rss_usage})
    else:
        results.set_value(*import_key, value={CLIENT: CUSTOM_LOAD, DATABASE: CUSTOM_LOAD})


def save_to_results(results, ret, workload, group, query, authorization_mode):
    results_key = [
        workload.NAME,
        workload.get_variant(),
        group,
        query,
        authorization_mode,
    ]
    results.set_value(*results_key, value=ret)


def run_isolated_workload_with_authorization(vendor_runner, client, queries, group, workload, results):
    log.init("Running isolated workload with authorization")

    log.info("Running preprocess AUTH queries")
    vendor_runner.start_db(VENDOR_RUNNER_AUTHORIZATION)
    client.execute(queries=SETUP_AUTH_QUERIES)
    client.set_credentials(username=USERNAME, password=PASSWORD)
    vendor_runner.stop_db(VENDOR_RUNNER_AUTHORIZATION)

    for query, funcname in queries[group]:
        log.init("Running query:" + "{}/{}/{}/{}".format(group, query, funcname, WITH_FINE_GRAINED_AUTHORIZATION))
        func = getattr(workload, funcname)
        count = get_query_cache_count(
            vendor_runner, client, get_queries(func, 1), benchmark_context, workload, group, query, func
        )

        vendor_runner.start_db(VENDOR_RUNNER_AUTHORIZATION)
        start_time = time.time()
        warmup(condition=benchmark_context.warm_up, client=client, queries=get_queries(func, count))

        ret = client.execute(
            queries=get_queries(func, count),
            num_workers=benchmark_context.num_workers_for_benchmark,
        )[0]
        usage = vendor_runner.stop_db(VENDOR_RUNNER_AUTHORIZATION)
        time_elapsed = time.time() - start_time
        log.info(f"Benchmark execution of query {funcname} finished in {time_elapsed} seconds.")

        ret[DATABASE] = usage
        log_metrics_summary(ret, usage)
        log_metadata_summary(ret)
        log.success("Throughput: {:02f} QPS".format(ret[THROUGHPUT]))
        save_to_results(results, ret, workload, group, query, WITH_FINE_GRAINED_AUTHORIZATION)

    vendor_runner.start_db(VENDOR_RUNNER_AUTHORIZATION)
    log.info("Running cleanup of auth queries")
    ret = client.execute(queries=CLEANUP_AUTH_QUERIES)
    vendor_runner.stop_db(VENDOR_RUNNER_AUTHORIZATION)


def run_isolated_workload_without_authorization(vendor_runner, client, queries, group, workload, results):
    log.init("Running isolated workload without authorization")
    for query, funcname in queries[group]:
        log.init(
            "Running query:" + "{}/{}/{}/{}".format(group, query, funcname, WITHOUT_FINE_GRAINED_AUTHORIZATION),
        )
        func = getattr(workload, funcname)
        count = get_query_cache_count(
            vendor_runner, client, get_queries(func, 1), benchmark_context, workload, group, query, func
        )

        # Benchmark run.
        sample_query = get_queries(func, 1)[0][0]
        log.info("Sample query:{}".format(sample_query))
        log.log(
            "Executing benchmark with {} queries that should yield a single-threaded runtime of {} seconds.".format(
                count, benchmark_context.single_threaded_runtime_sec
            )
        )
        log.log("Queries are executed using {} concurrent clients".format(benchmark_context.num_workers_for_benchmark))

        start_time = time.time()
        rss_db = workload.NAME + workload.get_variant() + "_" + "_" + benchmark_context.mode + "_" + query
        vendor_runner.start_db(rss_db)
        warmup(condition=benchmark_context.warm_up, client=client, queries=get_queries(func, count))
        log.init("Executing benchmark queries...")
        ret = client.execute(
            queries=get_queries(func, count),
            num_workers=benchmark_context.num_workers_for_benchmark,
            time_dependent_execution=benchmark_context.time_dependent_execution,
        )[0]

        time_elapsed = time.time() - start_time

        log.info(f"Benchmark execution of query {funcname} finished in {time_elapsed} seconds.")
        usage = vendor_runner.stop_db(rss_db)

        ret[DATABASE] = usage
        log_output_summary(benchmark_context, ret, usage, funcname, sample_query)

        save_to_results(results, ret, workload, group, query, WITHOUT_FINE_GRAINED_AUTHORIZATION)


def setup_indices_and_import_dataset(client, vendor_runner, generated_queries, workload, storage_mode):
    if benchmark_context.vendor_name == "memgraph":
        # Neo4j will get started just before import -> without this if statement it would try to start it twice
        vendor_runner.start_db_init(VENDOR_RUNNER_IMPORT)
    log.info("Executing database index setup")
    start_time = time.time()
    import_results = None
    if generated_queries:
        client.execute(queries=workload.indexes_generator(), num_workers=1)
        log.info("Finished setting up indexes.")
        log.info("Started importing dataset")
        import_results = client.execute(queries=generated_queries, num_workers=benchmark_context.num_workers_for_import)
    else:
        log.info("Using workload information for importing dataset and creating indices")
        log.info("Preparing workload: " + workload.NAME + "/" + workload.get_variant())
        workload.prepare(cache.cache_directory("datasets", workload.NAME, workload.get_variant()))
        imported = workload.custom_import()
        if not imported:
            client.execute(file_path=workload.get_index(), num_workers=1)
            log.info("Finished setting up indexes.")
            log.info("Started importing dataset")
            if storage_mode == ON_DISK_TRANSACTIONAL:
                import_results = client.execute(file_path=workload.get_node_file(), num_workers=1)
                import_results = client.execute(file_path=workload.get_edge_file(), num_workers=1)
            else:
                import_results = client.execute(
                    file_path=workload.get_file(), num_workers=benchmark_context.num_workers_for_import
                )
        else:
            log.info("Custom import executed")

    log.info(f"Finished importing dataset in {time.time() - start_time}s")
    rss_usage = vendor_runner.stop_db_init(VENDOR_RUNNER_IMPORT)

    return import_results, rss_usage


def run_target_workload(benchmark_context, workload, bench_queries, vendor_runner, client, results, storage_mode):
    generated_queries = workload.dataset_generator()
    import_results, rss_usage = setup_indices_and_import_dataset(
        client, vendor_runner, generated_queries, workload, storage_mode
    )
    save_import_results(workload, results, import_results, rss_usage)

    for group in sorted(bench_queries.keys()):
        log.init(f"\nRunning benchmark in {benchmark_context.mode} workload mode for {group} group")
        if benchmark_context.mode == BENCHMARK_MODE_MIXED:
            mixed_workload(vendor_runner, client, workload, group, bench_queries, benchmark_context, results)
        elif benchmark_context.mode == BENCHMARK_MODE_REALISTIC:
            realistic_workload(vendor_runner, client, workload, group, bench_queries, benchmark_context, results)
        else:
            run_isolated_workload_without_authorization(vendor_runner, client, bench_queries, group, workload, results)

        if benchmark_context.no_authorization:
            run_isolated_workload_with_authorization(vendor_runner, client, bench_queries, group, workload, results)


# TODO: (andi) Reorder functions in top-down notion in order to improve readibility
def run_target_workloads(benchmark_context, target_workloads, bench_results):
    for workload, bench_queries in target_workloads:
        log.info(f"Started running {str(workload.NAME)} workload")

        benchmark_context.set_active_workload(workload.NAME)
        benchmark_context.set_active_variant(workload.get_variant())

        if workload.is_disk_workload() and benchmark_context.export_results_on_disk_txn:
            run_on_disk_transactional_benchmark(benchmark_context, workload, bench_queries, bench_results.disk_results)
        else:
            run_in_memory_transactional_benchmark(
                benchmark_context, workload, bench_queries, bench_results.in_memory_txn_results
            )

            if benchmark_context.export_results_in_memory_analytical:
                run_in_memory_analytical_benchmark(
                    benchmark_context, workload, bench_queries, bench_results.in_memory_analytical_results
                )


def run_on_disk_transactional_benchmark(benchmark_context, workload, bench_queries, disk_results):
    log.info(f"Running benchmarks for {ON_DISK_TRANSACTIONAL} storage mode.")
    disk_vendor_runner, disk_client = client_runner_factory(benchmark_context)
    disk_vendor_runner.start_db(DISK_PREPARATION_RSS)
    disk_client.execute(queries=SETUP_DISK_STORAGE)
    disk_vendor_runner.stop_db(DISK_PREPARATION_RSS)
    run_target_workload(
        benchmark_context, workload, bench_queries, disk_vendor_runner, disk_client, disk_results, ON_DISK_TRANSACTIONAL
    )
    log.info(f"Finished running benchmarks for {ON_DISK_TRANSACTIONAL} storage mode.")


def run_in_memory_analytical_benchmark(benchmark_context, workload, bench_queries, in_memory_analytical_results):
    log.info(f"Running benchmarks for {IN_MEMORY_ANALYTICAL} storage mode.")
    in_memory_analytical_vendor_runner, in_memory_analytical_client = client_runner_factory(benchmark_context)
    in_memory_analytical_vendor_runner.start_db(IN_MEMORY_ANALYTICAL_RSS)
    in_memory_analytical_client.execute(queries=SETUP_IN_MEMORY_ANALYTICAL_STORAGE_MODE)
    in_memory_analytical_vendor_runner.stop_db(IN_MEMORY_ANALYTICAL_RSS)
    run_target_workload(
        benchmark_context,
        workload,
        bench_queries,
        in_memory_analytical_vendor_runner,
        in_memory_analytical_client,
        in_memory_analytical_results,
        IN_MEMORY_ANALYTICAL,
    )
    log.info(f"Finished running benchmarks for {IN_MEMORY_ANALYTICAL} storage mode.")


def run_in_memory_transactional_benchmark(benchmark_context, workload, bench_queries, in_memory_txn_results):
    log.info(f"Running benchmarks for {IN_MEMORY_TRANSACTIONAL} storage mode.")
    in_memory_txn_vendor_runner, in_memory_txn_client = client_runner_factory(benchmark_context)
    run_target_workload(
        benchmark_context,
        workload,
        bench_queries,
        in_memory_txn_vendor_runner,
        in_memory_txn_client,
        in_memory_txn_results,
        IN_MEMORY_TRANSACTIONAL,
    )
    log.info(f"Finished running benchmarks for {IN_MEMORY_TRANSACTIONAL} storage mode.")


def client_runner_factory(benchmark_context):
    vendor_runner = runners.BaseRunner.create(benchmark_context=benchmark_context)
    vendor_runner.clean_db()
    log.log("Database cleaned from any previous data")
    client = vendor_runner.fetch_client()
    return vendor_runner, client


def validate_target_workloads(benchmark_context, target_workloads):
    if len(target_workloads) == 0:
        log.error("No workloads matched the pattern: " + str(benchmark_context.benchmark_target_workload))
        log.error("Please check the pattern and workload NAME property, query group and query name.")
        log.info("Currently available workloads: ")
        log.log(helpers.list_available_workloads(benchmark_context.customer_workloads))
        sys.exit(1)


def log_benchmark_summary(results: Dict, storage_mode):
    log.log("\n")
    log.summary(f"Benchmark summary of {storage_mode} mode")
    log.log("-" * 120)
    log.summary("{:<20} {:>30} {:>30}".format("Query name", "Throughput", "Peak Memory usage"))
    for dataset, variants in results.items():
        if dataset == RUN_CONFIGURATION:
            continue
        for groups in variants.values():
            for group, queries in groups.items():
                if group == IMPORT:
                    continue
                for query, auth in queries.items():
                    for value in auth.values():
                        log.log("-" * 120)
                        log.summary(
                            "{:<20} {:>26.2f} QPS {:>27.2f} MB".format(
                                query, value[THROUGHPUT], value[DATABASE][MEMORY] / (1024.0 * 1024.0)
                            )
                        )
    log.log("-" * 90)


def log_benchmark_arguments(benchmark_context):
    log.init("Executing benchmark with following arguments: ")
    for key, value in benchmark_context.__dict__.items():
        log.log("{:<30} : {:<30}".format(str(key), str(value)))


def log_metrics_summary(ret, usage):
    log.log("Executed  {} queries in {} seconds.".format(ret[COUNT], ret[DURATION]))
    log.log("Queries have been retried {} times".format(ret[RETRIES]))
    log.log("Database used {:.3f} seconds of CPU time.".format(usage[CPU]))
    log.info("Database peaked at {:.3f} MiB of memory.".format(usage[MEMORY] / (1024.0 * 1024.0)))


def log_metadata_summary(ret):
    log.log("{:<31} {:>20} {:>20} {:>20}".format("Metadata:", "min", "avg", "max"))
    metadata = ret[METADATA]
    for key in sorted(metadata.keys()):
        log.log(
            "{name:>30}: {minimum:>20.06f} {average:>20.06f} " "{maximum:>20.06f}".format(name=key, **metadata[key])
        )


def log_output_summary(benchmark_context, ret, usage, funcname, sample_query):
    log_metrics_summary(ret, usage)
    if DOCKER not in benchmark_context.vendor_name:
        log_metadata_summary(ret)

    log.info("\nResult:")
    log.info(funcname)
    log.info(sample_query)
    log.success("Latency statistics:")
    for key, value in ret[LATENCY_STATS].items():
        if key == ITERATIONS:
            log.success("{:<10} {:>10}".format(key, value))
        else:
            log.success("{:<10} {:>10.06f} seconds".format(key, value))
    log.success("Throughput: {:02f} QPS\n\n".format(ret[THROUGHPUT]))


if __name__ == "__main__":
    args = parse_args()
    sanitize_args(args)
    vendor_specific_args = helpers.parse_kwargs(args.vendor_specific)

    temp_dir = pathlib.Path.cwd() / ".temp"
    temp_dir.mkdir(parents=True, exist_ok=True)

    benchmark_context = BenchmarkContext(
        benchmark_target_workload=args.benchmarks,
        vendor_binary=args.vendor_binary if args.run_option == "vendor-native" else None,
        vendor_name=args.vendor_name.replace("-", ""),
        client_binary=args.client_binary if args.run_option == "vendor-native" else None,
        num_workers_for_import=args.num_workers_for_import,
        num_workers_for_benchmark=args.num_workers_for_benchmark,
        single_threaded_runtime_sec=args.single_threaded_runtime_sec,
        query_count_lower_bound=args.query_count_lower_bound,
        no_load_query_counts=args.no_load_query_counts,
        export_results=args.export_results,
        export_results_in_memory_analytical=args.export_results_in_memory_analytical,
        export_results_on_disk_txn=args.export_results_on_disk_txn,
        temporary_directory=temp_dir.absolute(),
        workload_mixed=args.workload_mixed,
        workload_realistic=args.workload_realistic,
        time_dependent_execution=args.time_depended_execution,
        warm_up=args.warm_up,
        performance_tracking=args.performance_tracking,
        no_authorization=args.no_authorization,
        customer_workloads=args.customer_workloads,
        vendor_args=vendor_specific_args,
    )

    log_benchmark_arguments(benchmark_context)
    check_benchmark_requirements(benchmark_context)

    cache = helpers.Cache()
    log.log("Creating cache folder for dataset, configurations, indexes and results.")
    log.log("Cache folder in use: " + cache.get_default_cache_directory())
    config = setup_cache_config(benchmark_context, cache)

    in_memory_txn_run_config = {
        VENDOR: benchmark_context.vendor_name,
        CONDITION: benchmark_context.warm_up,
        NUM_WORKERS_FOR_BENCHMARK: benchmark_context.num_workers_for_benchmark,
        SINGLE_THREADED_RUNTIME_SEC: benchmark_context.single_threaded_runtime_sec,
        BENCHMARK_MODE: benchmark_context.mode,
        BENCHMARK_MODE_CONFIG: benchmark_context.mode_config,
        PLATFORM: platform.platform(),
        STORAGE_MODE: IN_MEMORY_TRANSACTIONAL,
    }

    in_memory_analytical_run_config = deepcopy(in_memory_txn_run_config)
    in_memory_analytical_run_config[STORAGE_MODE] = IN_MEMORY_ANALYTICAL

    on_disk_transactional_run_config = deepcopy(in_memory_txn_run_config)
    on_disk_transactional_run_config[STORAGE_MODE] = ON_DISK_TRANSACTIONAL

    bench_results = BenchmarkResults()
    bench_results.in_memory_txn_results.set_value(RUN_CONFIGURATION, value=in_memory_txn_run_config)
    bench_results.in_memory_analytical_results.set_value(RUN_CONFIGURATION, value=in_memory_analytical_run_config)
    bench_results.disk_results.set_value(RUN_CONFIGURATION, value=on_disk_transactional_run_config)

    available_workloads = helpers.get_available_workloads(benchmark_context.customer_workloads)

    # Filter out the workloads based on the pattern
    # TODO (andi) Maybe here filter workloads if working on disk storage.
    target_workloads = helpers.filter_workloads(
        available_workloads=available_workloads, benchmark_context=benchmark_context
    )

    validate_target_workloads(benchmark_context, target_workloads)
    run_target_workloads(benchmark_context, target_workloads, bench_results)

    if not benchmark_context.no_save_query_counts:
        cache.save_config(config)

    log_benchmark_summary(bench_results.in_memory_txn_results.get_data(), IN_MEMORY_TRANSACTIONAL)
    if benchmark_context.export_results:
        with open(benchmark_context.export_results, "w") as f:
            json.dump(bench_results.in_memory_txn_results.get_data(), f)

    log_benchmark_summary(bench_results.in_memory_analytical_results.get_data(), IN_MEMORY_ANALYTICAL)
    if benchmark_context.export_results_in_memory_analytical:
        with open(benchmark_context.export_results_in_memory_analytical, "w") as f:
            json.dump(bench_results.in_memory_analytical_results.get_data(), f)

    log_benchmark_summary(bench_results.disk_results.get_data(), ON_DISK_TRANSACTIONAL)
    if benchmark_context.export_results_on_disk_txn:
        with open(benchmark_context.export_results_on_disk_txn, "w") as f:
            json.dump(bench_results.disk_results.get_data(), f)
