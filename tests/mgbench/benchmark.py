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

import helpers
import log
import runners
import setup
from benchmark_context import BenchmarkContext
from workloads import *

WITH_FINE_GRAINED_AUTHORIZATION = "with_fine_grained_authorization"
WITHOUT_FINE_GRAINED_AUTHORIZATION = "without_fine_grained_authorization"


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
        help="file path into which results should be exported",
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

    # Vendor native parser starts here
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

    # Vendor docker parsers starts here
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


def get_queries(gen, count):
    # Make the generator deterministic.
    random.seed(gen.__name__)
    # Generate queries.
    ret = []
    for i in range(count):
        ret.append(gen())
    return ret


def warmup(condition: str, client: runners.BaseRunner, queries: list = None):
    log.init("Started warm-up procedure to match database condition: {} ".format(condition))
    if condition == "hot":
        log.log("Execute warm-up to match condition: {} ".format(condition))
        client.execute(
            queries=[
                ("CREATE ();", {}),
                ("CREATE ()-[:TempEdge]->();", {}),
                ("MATCH (n) RETURN count(n.prop) LIMIT 1;", {}),
            ],
            num_workers=1,
        )
    elif condition == "vulcanic":
        log.log("Execute warm-up to match condition: {} ".format(condition))
        client.execute(queries=queries)
    else:
        log.log("No warm-up on condition: {} ".format(condition))
    log.log("Finished warm-up procedure to match database condition: {} ".format(condition))


def mixed_workload(
    vendor: runners.BaseRunner, client: runners.BaseClient, dataset, group, queries, benchmark_context: BenchmarkContext
):
    num_of_queries = benchmark_context.mode_config[0]
    percentage_distribution = benchmark_context.mode_config[1:]
    if sum(percentage_distribution) != 100:
        raise Exception(
            "Please make sure that passed arguments % sum to 100% percent!, passed: ",
            percentage_distribution,
        )
    s = [str(i) for i in benchmark_context.mode_config]

    config_distribution = "_".join(s)

    log.log("Generating mixed workload...")

    percentages_by_type = {
        "write": percentage_distribution[0],
        "read": percentage_distribution[1],
        "update": percentage_distribution[2],
        "analytical": percentage_distribution[3],
    }

    queries_by_type = {
        "write": [],
        "read": [],
        "update": [],
        "analytical": [],
    }

    for _, funcname in queries[group]:
        for key in queries_by_type.keys():
            if key in funcname:
                queries_by_type[key].append(funcname)

    for key, percentage in percentages_by_type.items():
        if percentage != 0 and len(queries_by_type[key]) == 0:
            raise Exception(
                "There is a missing query in group (write, read, update or analytical) for given workload distribution."
            )

    random.seed(config_distribution)

    # Executing mixed workload for each test
    if benchmark_context.mode == "Mixed":
        for query, funcname in queries[group]:
            full_workload = []

            log.info(
                "Running query in mixed workload: {}/{}/{}".format(
                    group,
                    query,
                    funcname,
                ),
            )
            base_query = getattr(dataset, funcname)

            base_query_type = funcname.rsplit("_", 1)[1]

            if percentages_by_type.get(base_query_type, 0) > 0:
                continue

            options = ["write", "read", "update", "analytical", "query"]
            function_type = random.choices(population=options, weights=percentage_distribution, k=num_of_queries)

            for t in function_type:
                # Get the appropriate functions with same probability
                if t == "query":
                    full_workload.append(base_query())
                else:
                    funcname = random.choices(queries_by_type[t], k=1)[0]
                    additional_query = getattr(dataset, funcname)
                    full_workload.append(additional_query())

            vendor.start_db(
                dataset.NAME + dataset.get_variant() + "_" + "mixed" + "_" + query + "_" + config_distribution
            )
            warmup(benchmark_context.warm_up, client=client)
            ret = client.execute(
                queries=full_workload,
                num_workers=benchmark_context.num_workers_for_benchmark,
            )[0]
            usage_workload = vendor.stop_db(
                dataset.NAME + dataset.get_variant() + "_" + "mixed" + "_" + query + "_" + config_distribution
            )

            ret["database"] = usage_workload

            results_key = [
                dataset.NAME,
                dataset.get_variant(),
                group,
                query + "_" + config_distribution,
                WITHOUT_FINE_GRAINED_AUTHORIZATION,
            ]
            results.set_value(*results_key, value=ret)

    else:
        # Executing mixed workload from groups of queries
        full_workload = []
        options = ["write", "read", "update", "analytical"]
        function_type = random.choices(population=options, weights=percentage_distribution, k=num_of_queries)

        for t in function_type:
            # Get the appropriate functions with same probability
            funcname = random.choices(queries_by_type[t], k=1)[0]
            additional_query = getattr(dataset, funcname)
            full_workload.append(additional_query())

        vendor.start_db(dataset.NAME + dataset.get_variant() + "_" + "realistic" + "_" + config_distribution)
        warmup(benchmark_context.warm_up, client=client)
        ret = client.execute(
            queries=full_workload,
            num_workers=benchmark_context.num_workers_for_benchmark,
        )[0]
        usage_workload = vendor.stop_db(
            dataset.NAME + dataset.get_variant() + "_" + "realistic" + "_" + config_distribution
        )
        mixed_workload = {
            "count": ret["count"],
            "duration": ret["duration"],
            "retries": ret["retries"],
            "throughput": ret["throughput"],
            "num_workers": ret["num_workers"],
            "database": usage_workload,
        }
        results_key = [
            dataset.NAME,
            dataset.get_variant(),
            group,
            config_distribution,
            WITHOUT_FINE_GRAINED_AUTHORIZATION,
        ]
        results.set_value(*results_key, value=mixed_workload)

        print(mixed_workload)


def get_query_cache_count(
    vendor: runners.BaseRunner,
    client: runners.BaseClient,
    queries: list,
    config_key: list,
    benchmark_context: BenchmarkContext,
):
    cached_count = config.get_value(*config_key)
    if cached_count is None:
        log.info(
            "Determining the number of queries necessary for {} seconds of single-threaded runtime...".format(
                benchmark_context.single_threaded_runtime_sec
            )
        )
        log.log("Running query to prime the query cache...")
        # First run to prime the query caches.
        vendor.start_db("cache")
        client.execute(queries=queries, num_workers=1)
        # Get a sense of the runtime.
        count = 1
        while True:
            ret = client.execute(queries=get_queries(func, count), num_workers=1)
            duration = ret[0]["duration"]
            should_execute = int(benchmark_context.single_threaded_runtime_sec / (duration / count))
            log.log(
                "executed_queries={}, total_duration={}, query_duration={}, estimated_count={}".format(
                    count, duration, duration / count, should_execute
                )
            )
            # We don't have to execute the next iteration when
            # `should_execute` becomes the same order of magnitude as
            # `count * 10`.
            if should_execute / (count * 10) < 10:
                count = should_execute
                break
            else:
                count = count * 10
        vendor.stop_db("cache")

        if count < benchmark_context.query_count_lower_bound:
            count = benchmark_context.query_count_lower_bound

        config.set_value(
            *config_key,
            value={
                "count": count,
                "duration": benchmark_context.single_threaded_runtime_sec,
            },
        )
    else:
        log.log(
            "Using cached query count of {} queries for {} seconds of single-threaded runtime to extrapolate .".format(
                cached_count["count"], cached_count["duration"]
            ),
        )
        count = int(cached_count["count"] * benchmark_context.single_threaded_runtime_sec / cached_count["duration"])
    return count


if __name__ == "__main__":
    args = parse_args()
    vendor_specific_args = helpers.parse_kwargs(args.vendor_specific)

    assert args.benchmarks != None, helpers.list_available_workloads()
    assert args.num_workers_for_import > 0
    assert args.num_workers_for_benchmark > 0
    assert args.export_results != None, "Pass where will results be saved"
    assert (
        args.single_threaded_runtime_sec >= 10
    ), "Low runtime value, consider extending time for more accurate results"
    assert (
        args.workload_realistic == None or args.workload_mixed == None
    ), "Cannot run both realistic and mixed workload, only one mode run at the time"

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

    log.init("Executing benchmark with following arguments: ")
    for key, value in benchmark_context.__dict__.items():
        log.log("{:<30} : {:<30}".format(str(key), str(value)))

    log.init("Check requirements for running benchmark")
    if setup.check_requirements(benchmark_context=benchmark_context):
        log.success("Requirements satisfied... ")
    else:
        log.warning("Requirements not satisfied...")
        sys.exit(1)

    log.log("Creating cache folder for: dataset, configurations, indexes, results etc. ")
    cache = helpers.Cache()
    log.init("Folder in use: " + cache.get_default_cache_directory())
    if not benchmark_context.no_load_query_counts:
        log.log("Using previous cached query count data from cache directory.")
        config = cache.load_config()
    else:
        config = helpers.RecursiveDict()
    results = helpers.RecursiveDict()

    run_config = {
        "vendor": benchmark_context.vendor_name,
        "condition": benchmark_context.warm_up,
        "num_workers_for_benchmark": benchmark_context.num_workers_for_benchmark,
        "single_threaded_runtime_sec": benchmark_context.single_threaded_runtime_sec,
        "benchmark_mode": benchmark_context.mode,
        "benchmark_mode_config": benchmark_context.mode_config,
        "platform": platform.platform(),
    }

    results.set_value("__run_configuration__", value=run_config)

    available_workloads = helpers.get_available_workloads(benchmark_context.customer_workloads)

    # Filter out the workloads based on the pattern
    target_workloads = helpers.filter_workloads(
        available_workloads=available_workloads, benchmark_context=benchmark_context
    )

    if len(target_workloads) == 0:
        log.error("No workloads matched the pattern: " + str(benchmark_context.benchmark_target_workload))
        log.error("Please check the pattern and workload NAME property, query group and query name.")
        log.info("Currently available workloads: ")
        log.log(helpers.list_available_workloads(benchmark_context.customer_workloads))
        sys.exit(1)

    # Run all target workloads.
    for workload, queries in target_workloads:
        log.info("Started running following workload: " + str(workload.NAME))

        benchmark_context.set_active_workload(workload.NAME)
        benchmark_context.set_active_variant(workload.get_variant())

        log.init("Creating vendor runner for DB: " + benchmark_context.vendor_name)
        vendor_runner = runners.BaseRunner.create(
            benchmark_context=benchmark_context,
        )
        log.log("Class in use: " + str(vendor_runner.__class__.__name__))

        log.info("Cleaning the database from any previous data")
        vendor_runner.clean_db()

        client = vendor_runner.fetch_client()
        log.log("Get appropriate client for vendor " + str(client.__class__.__name__))

        ret = None
        usage = None

        generated_queries = workload.dataset_generator()
        if generated_queries:
            print("\n")
            log.info("Using workload as dataset generator...")

            vendor_runner.start_db_init("import")

            log.warning("Using following indexes...")
            log.info(workload.indexes_generator())
            log.info("Executing database index setup...")
            ret = client.execute(queries=workload.indexes_generator(), num_workers=1)
            log.log("Finished setting up indexes...")
            for row in ret:
                log.success(
                    "Executed {} queries in {} seconds using {} workers with a total throughput of {} Q/S.".format(
                        row["count"], row["duration"], row["num_workers"], row["throughput"]
                    )
                )

            log.info("Importing dataset...")
            ret = client.execute(queries=generated_queries, num_workers=benchmark_context.num_workers_for_import)
            log.log("Finished importing dataset...")
            usage = vendor_runner.stop_db_init("import")
        else:
            log.init("Preparing workload: " + workload.NAME + "/" + workload.get_variant())
            workload.prepare(cache.cache_directory("datasets", workload.NAME, workload.get_variant()))
            log.info("Using workload dataset information for import...")
            imported = workload.custom_import()
            if not imported:
                log.log("Basic import execution")
                vendor_runner.start_db_init("import")
                log.log("Executing database index setup...")
                client.execute(file_path=workload.get_index(), num_workers=1)
                log.log("Importing dataset...")
                ret = client.execute(
                    file_path=workload.get_file(), num_workers=benchmark_context.num_workers_for_import
                )
                usage = vendor_runner.stop_db_init("import")
            else:
                log.info("Custom import executed...")

        # Save import results.
        import_key = [workload.NAME, workload.get_variant(), "__import__"]
        if ret != None and usage != None:
            # Display import statistics.
            for row in ret:
                log.success(
                    "Executed {} queries in {} seconds using {} workers with a total throughput of {} Q/S.".format(
                        row["count"], row["duration"], row["num_workers"], row["throughput"]
                    )
                )

            log.success(
                "The database used {} seconds of CPU time and peaked at {} MiB of RAM".format(
                    usage["cpu"], usage["memory"] / 1024 / 1024
                )
            )

            results.set_value(*import_key, value={"client": ret, "database": usage})
        else:
            results.set_value(*import_key, value={"client": "custom_load", "database": "custom_load"})

        # Run all benchmarks in all available groups.
        for group in sorted(queries.keys()):
            print("\n")
            log.init("Running benchmark in " + benchmark_context.mode)
            if benchmark_context.mode == "Mixed":
                mixed_workload(vendor_runner, client, workload, group, queries, benchmark_context)
            elif benchmark_context.mode == "Realistic":
                mixed_workload(vendor_runner, client, workload, group, queries, benchmark_context)
            else:
                for query, funcname in queries[group]:
                    log.init(
                        "Running query:"
                        + "{}/{}/{}/{}".format(group, query, funcname, WITHOUT_FINE_GRAINED_AUTHORIZATION),
                    )
                    func = getattr(workload, funcname)

                    # Query count
                    config_key = [
                        workload.NAME,
                        workload.get_variant(),
                        group,
                        query,
                    ]
                    log.init("Determining query count for benchmark based on --single-threaded-runtime argument")
                    count = get_query_cache_count(
                        vendor_runner, client, get_queries(func, 1), config_key, benchmark_context
                    )
                    # Benchmark run.
                    sample_query = get_queries(func, 1)[0][0]
                    log.info("Sample query:{}".format(sample_query))
                    log.log(
                        "Executing benchmark with {} queries that should yield a single-threaded runtime of {} seconds.".format(
                            count, benchmark_context.single_threaded_runtime_sec
                        )
                    )
                    log.log(
                        "Queries are executed using {} concurrent clients".format(
                            benchmark_context.num_workers_for_benchmark
                        )
                    )
                    vendor_runner.start_db(
                        workload.NAME + workload.get_variant() + "_" + "_" + benchmark_context.mode + "_" + query
                    )
                    warmup(condition=benchmark_context.warm_up, client=client, queries=get_queries(func, count))
                    log.init("Executing benchmark queries...")
                    if benchmark_context.time_dependent_execution != 0:
                        ret = client.execute(
                            queries=get_queries(func, count),
                            num_workers=benchmark_context.num_workers_for_benchmark,
                            time_dependent_execution=benchmark_context.time_dependent_execution,
                        )[0]
                    else:
                        ret = client.execute(
                            queries=get_queries(func, count),
                            num_workers=benchmark_context.num_workers_for_benchmark,
                        )[0]
                    log.info("Benchmark execution finished...")
                    usage = vendor_runner.stop_db(
                        workload.NAME + workload.get_variant() + "_" + benchmark_context.mode + "_" + query
                    )
                    ret["database"] = usage
                    # Output summary.

                    log.log("Executed  {} queries in {} seconds.".format(ret["count"], ret["duration"]))
                    log.log("Queries have been retried {} times".format(ret["retries"]))
                    log.log("Database used {:.3f} seconds of CPU time.".format(usage["cpu"]))
                    log.info("Database peaked at {:.3f} MiB of memory.".format(usage["memory"] / 1024.0 / 1024.0))
                    if "docker" not in benchmark_context.vendor_name:
                        log.log("{:<31} {:>20} {:>20} {:>20}".format("Metadata:", "min", "avg", "max"))
                        metadata = ret["metadata"]
                        for key in sorted(metadata.keys()):
                            log.log(
                                "{name:>30}: {minimum:>20.06f} {average:>20.06f} "
                                "{maximum:>20.06f}".format(name=key, **metadata[key])
                            )
                    print("\n")
                    log.info("Result:")
                    log.info(funcname)
                    log.info(sample_query)
                    log.success("Latency statistics:")
                    for key, value in ret["latency_stats"].items():
                        if key == "iterations":
                            log.success("{:<10} {:>10}".format(key, value))
                        else:
                            log.success("{:<10} {:>10.06f} seconds".format(key, value))

                    log.success("Throughput: {:02f} QPS".format(ret["throughput"]))
                    print("\n\n")

                    # Save results.
                    results_key = [
                        workload.NAME,
                        workload.get_variant(),
                        group,
                        query,
                        WITHOUT_FINE_GRAINED_AUTHORIZATION,
                    ]
                    results.set_value(*results_key, value=ret)

            # If there is need for authorization testing.
            if benchmark_context.no_authorization:
                log.init("Running queries with authorization...")
                log.info("Setting USER and PRIVILEGES...")
                vendor_runner.start_db("authorization")
                client.execute(
                    queries=[
                        ("CREATE USER user IDENTIFIED BY 'test';", {}),
                        ("GRANT ALL PRIVILEGES TO user;", {}),
                        ("GRANT CREATE_DELETE ON EDGE_TYPES * TO user;", {}),
                        ("GRANT CREATE_DELETE ON LABELS * TO user;", {}),
                    ]
                )

                client.set_credentials(username="user", password="test")
                vendor_runner.stop_db("authorization")

                for query, funcname in queries[group]:
                    log.init(
                        "Running query:" + "{}/{}/{}/{}".format(group, query, funcname, WITH_FINE_GRAINED_AUTHORIZATION)
                    )
                    func = getattr(workload, funcname)

                    config_key = [
                        workload.NAME,
                        workload.get_variant(),
                        group,
                        query,
                    ]
                    count = get_query_cache_count(
                        vendor_runner, client, get_queries(func, 1), config_key, benchmark_context
                    )

                    vendor_runner.start_db("authorization")
                    warmup(condition=benchmark_context.warm_up, client=client, queries=get_queries(func, count))

                    ret = client.execute(
                        queries=get_queries(func, count),
                        num_workers=benchmark_context.num_workers_for_benchmark,
                    )[0]
                    usage = vendor_runner.stop_db("authorization")
                    ret["database"] = usage
                    # Output summary.
                    log.log("Executed  {} queries in {} seconds.".format(ret["count"], ret["duration"]))
                    log.log("Queries have been retried {} times".format(ret["retries"]))
                    log.log("Database used {:.3f} seconds of CPU time.".format(usage["cpu"]))
                    log.log("Database peaked at {:.3f} MiB of memory.".format(usage["memory"] / 1024.0 / 1024.0))
                    log.log("{:<31} {:>20} {:>20} {:>20}".format("Metadata:", "min", "avg", "max"))
                    metadata = ret["metadata"]
                    for key in sorted(metadata.keys()):
                        log.log(
                            "{name:>30}: {minimum:>20.06f} {average:>20.06f} "
                            "{maximum:>20.06f}".format(name=key, **metadata[key])
                        )
                    log.success("Throughput: {:02f} QPS".format(ret["throughput"]))
                    # Save results.
                    results_key = [
                        workload.NAME,
                        workload.get_variant(),
                        group,
                        query,
                        WITH_FINE_GRAINED_AUTHORIZATION,
                    ]
                    results.set_value(*results_key, value=ret)

                log.info("Deleting USER and PRIVILEGES...")
                vendor_runner.start_db("authorizations")
                ret = client.execute(
                    queries=[
                        ("REVOKE LABELS * FROM user;", {}),
                        ("REVOKE EDGE_TYPES * FROM user;", {}),
                        ("DROP USER user;", {}),
                    ]
                )
                vendor_runner.stop_db("authorization")

    # Save configuration.
    if not benchmark_context.no_save_query_counts:
        cache.save_config(config)

    # Export results.
    if benchmark_context.export_results:
        with open(benchmark_context.export_results, "w") as f:
            json.dump(results.get_data(), f)

    # Results summary.
    log.init("~" * 45)
    log.info("Benchmark finished.")
    log.init("~" * 45)
    log.log("\n")
    log.summary("Benchmark summary")
    log.log("-" * 90)
    log.summary("{:<20} {:>30} {:>30}".format("Query name", "Throughput", "Peak Memory usage"))
    with open(benchmark_context.export_results, "r") as f:
        results = json.load(f)
        for dataset, variants in results.items():
            if dataset == "__run_configuration__":
                continue
            for variant, groups in variants.items():
                for group, queries in groups.items():
                    if group == "__import__":
                        continue
                    for query, auth in queries.items():
                        for key, value in auth.items():
                            log.log("-" * 90)
                            log.summary(
                                "{:<20} {:>26.2f} QPS {:>27.2f} MB".format(
                                    query, value["throughput"], value["database"]["memory"] / 1024.0 / 1024.0
                                )
                            )
    log.log("-" * 90)
