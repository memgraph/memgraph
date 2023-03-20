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
import collections
import fnmatch
import json
import multiprocessing
import platform
import random

import helpers
import log
import runners
import workloads.base
from benchmark_context import BenchmarkContext
from workloads import *

WITH_FINE_GRAINED_AUTHORIZATION = "with_fine_grained_authorization"
WITHOUT_FINE_GRAINED_AUTHORIZATION = "without_fine_grained_authorization"


def parse_args():

    parser = argparse.ArgumentParser(
        description="Memgraph benchmark executor.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
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
    parser.add_argument(
        "--vendor-binary",
        help="Vendor binary used for benchmarking, by default it is memgraph",
        default=helpers.get_binary_path("memgraph"),
    )

    parser.add_argument(
        "--vendor-name",
        default="memgraph",
        choices=["memgraph", "neo4j"],
        help="Input vendor binary name (memgraph, neo4j)",
    )
    parser.add_argument(
        "--client-binary",
        default=helpers.get_binary_path("tests/mgbench/client"),
        help="Client binary used for benchmarking",
    )
    parser.add_argument(
        "--num-workers-for-import",
        type=int,
        default=multiprocessing.cpu_count() // 2,
        help="number of workers used to import the dataset",
    )
    parser.add_argument(
        "--num-workers-for-benchmark",
        type=int,
        default=1,
        help="number of workers used to execute the benchmark",
    )
    parser.add_argument(
        "--single-threaded-runtime-sec",
        type=int,
        default=10,
        help="single threaded duration of each query",
    )
    parser.add_argument(
        "--no-load-query-counts",
        action="store_true",
        default=False,
        help="disable loading of cached query counts",
    )
    parser.add_argument(
        "--no-save-query-counts",
        action="store_true",
        default=False,
        help="disable storing of cached query counts",
    )

    parser.add_argument(
        "--export-results",
        default=None,
        help="file path into which results should be exported",
    )
    parser.add_argument(
        "--temporary-directory",
        default="/tmp",
        help="directory path where temporary data should be stored",
    )

    parser.add_argument(
        "--no-authorization",
        action="store_false",
        default=True,
        help="Run each query with authorization",
    )

    parser.add_argument(
        "--warm-up",
        default="cold",
        choices=["cold", "hot", "vulcanic"],
        help="Run different warmups before benchmarks sample starts",
    )

    parser.add_argument(
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

    parser.add_argument(
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

    parser.add_argument(
        "--time-depended-execution",
        type=int,
        default=0,
        help="Execute defined number of queries (based on single-threaded-runtime-sec) for a defined duration in of wall-clock time",
    )

    parser.add_argument(
        "--performance-tracking",
        action="store_true",
        default=False,
        help="Flag for runners performance tracking, this logs RES through time and vendor specific performance tracking.",
    )

    parser.add_argument(
        "--vendor-specific",
        nargs="*",
        default=["bolt-port=7687", "no-properties-on-edges=False"],
        help="Vendor specific arguments that can be applied to each vendor, format: key=value",
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
    if condition == "hot":
        print("Executing hot warm-up queries")
        client.execute(
            queries=[
                ("CREATE ();", {}),
                ("CREATE ()-[:TempEdge]->();", {}),
                ("MATCH (n) RETURN n LIMIT 1;", {}),
            ],
            num_workers=1,
        )
    elif condition == "vulcanic":
        print("Executing vulcanic warm-up queries")
        client.execute(queries=queries)
    else:
        print("Cold run")


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

    print("Generating mixed workload.")

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

    for (_, funcname) in queries[group]:
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
                "Running query in mixed workload:",
                "{}/{}/{}".format(
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
                # Get the apropropriate functions with same probabilty
                if t == "query":
                    full_workload.append(base_query())
                else:
                    funcname = random.choices(queries_by_type[t], k=1)[0]
                    additional_query = getattr(dataset, funcname)
                    full_workload.append(additional_query())

            vendor.start_benchmark(
                dataset.NAME + dataset.get_variant() + "_" + "mixed" + "_" + query + "_" + config_distribution
            )
            warmup(benchmark_context.warm_up, client=client)
            ret = client.execute(
                queries=full_workload,
                num_workers=benchmark_context.num_workers_for_benchmark,
            )[0]
            usage_workload = vendor.stop(
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

        vendor.start_benchmark(dataset.NAME + dataset.get_variant() + "_" + "realistic" + "_" + config_distribution)
        warmup(benchmark_context.warm_up, client=client)
        ret = client.execute(
            queries=full_workload,
            num_workers=benchmark_context.num_workers_for_benchmark,
        )[0]
        usage_workload = vendor.stop(
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
        print(
            "Determining the number of queries necessary for",
            benchmark_context.single_threaded_runtime_sec,
            "seconds of single-threaded runtime...",
        )
        # First run to prime the query caches.
        vendor.start_benchmark("cache")
        client.execute(queries=queries, num_workers=1)
        # Get a sense of the runtime.
        count = 1
        while True:
            ret = client.execute(queries=get_queries(func, count), num_workers=1)
            duration = ret[0]["duration"]
            should_execute = int(benchmark_context.single_threaded_runtime_sec / (duration / count))
            print(
                "executed_queries={}, total_duration={}, "
                "query_duration={}, estimated_count={}".format(count, duration, duration / count, should_execute)
            )
            # We don't have to execute the next iteration when
            # `should_execute` becomes the same order of magnitude as
            # `count * 10`.
            if should_execute / (count * 10) < 10:
                count = should_execute
                break
            else:
                count = count * 10
        vendor.stop("cache")

        count_lower_bound = 30
        if count < count_lower_bound:
            count = count_lower_bound

        config.set_value(
            *config_key,
            value={
                "count": count,
                "duration": benchmark_context.single_threaded_runtime_sec,
            },
        )
    else:
        print(
            "Using cached query count of",
            cached_count["count"],
            "queries for",
            cached_count["duration"],
            "seconds of single-threaded runtime.",
        )
        count = int(cached_count["count"] * benchmark_context.single_threaded_runtime_sec / cached_count["duration"])
    return count


if __name__ == "__main__":

    args = parse_args()
    vendor_specific_args = helpers.parse_kwargs(args.vendor_specific)

    assert args.benchmarks != None, helpers.list_available_workloads()
    assert args.vendor_name == "memgraph" or args.vendor_name == "neo4j", "Unsupported vendors"
    assert args.vendor_binary != None, "Pass database binary for runner"
    assert args.client_binary != None, "Pass client binary for benchmark client "
    assert args.num_workers_for_import > 0
    assert args.num_workers_for_benchmark > 0
    assert args.export_results != None, "Pass where will results be saved"
    assert (
        args.single_threaded_runtime_sec >= 10
    ), "Low runtime value, consider extending time for more accurate results"
    assert (
        args.workload_realistic == None or args.workload_mixed == None
    ), "Cannot run both realistic and mixed workload, only one mode run at the time"

    benchmark_context = BenchmarkContext(
        benchmark_target_workload=args.benchmarks,
        vendor_binary=args.vendor_binary,
        vendor_name=args.vendor_name,
        client_binary=args.client_binary,
        num_workers_for_import=args.num_workers_for_import,
        num_workers_for_benchmark=args.num_workers_for_benchmark,
        single_threaded_runtime_sec=args.single_threaded_runtime_sec,
        no_load_query_counts=args.no_load_query_counts,
        export_results=args.export_results,
        temporary_directory=args.temporary_directory,
        workload_mixed=args.workload_mixed,
        workload_realistic=args.workload_realistic,
        time_dependent_execution=args.time_depended_execution,
        warm_up=args.warm_up,
        performance_tracking=args.performance_tracking,
        no_authorization=args.no_authorization,
        vendor_args=vendor_specific_args,
    )

    # Create cache, config and results objects.
    cache = helpers.Cache()
    if not benchmark_context.no_load_query_counts:
        config = cache.load_config()
    else:
        config = helpers.RecursiveDict()
    results = helpers.RecursiveDict()

    vendor_runner = runners.BaseRunner.create(
        benchmark_context=benchmark_context,
    )

    run_config = {
        "vendor": benchmark_context.vendor_name,
        "condition": benchmark_context.warm_up,
        "benchmark_mode": benchmark_context.mode,
        "benchmark_mode_config": benchmark_context.mode_config,
        "platform": platform.platform(),
    }

    results.set_value("__run_configuration__", value=run_config)

    available_workloads = helpers.get_available_workloads()

    print(helpers.list_available_workloads())

    # Filter out the workloads based on the pattern
    target_workloads = helpers.filter_workloads(
        available_workloads=available_workloads, benchmark_context=benchmark_context
    )

    # Run all target workloads.
    for workload, queries in target_workloads:

        client = vendor_runner.fetch_client()

        ret = None
        usage = None

        vendor_runner.clean_db()

        generated_queries = workload.dataset_generator()
        if generated_queries:
            vendor_runner.start_preparation("import")
            client.execute(queries=generated_queries, num_workers=benchmark_context.num_workers_for_import)
            vendor_runner.stop("import")
        else:
            log.init("Preparing workload: ", workload.NAME + "/" + workload.get_variant())
            workload.prepare(cache.cache_directory("datasets", workload.NAME, workload.get_variant()))
            imported = workload.custom_import()
            if not imported:
                vendor_runner.start_preparation("import")
                print("Executing database cleanup and index setup...")
                client.execute(file_path=workload.get_index(), num_workers=benchmark_context.num_workers_for_import)
                print("Importing dataset...")
                ret = client.execute(
                    file_path=workload.get_file(), num_workers=benchmark_context.num_workers_for_import
                )
                usage = vendor_runner.stop("import")

        # Save import results.
        import_key = [workload.NAME, workload.get_variant(), "__import__"]
        if ret != None and usage != None:
            # Display import statistics.
            print()
            for row in ret:
                print(
                    "Executed",
                    row["count"],
                    "queries in",
                    row["duration"],
                    "seconds using",
                    row["num_workers"],
                    "workers with a total throughput of",
                    row["throughput"],
                    "queries/second.",
                )
            print()
            print(
                "The database used",
                usage["cpu"],
                "seconds of CPU time and peaked at",
                usage["memory"] / 1024 / 1024,
                "MiB of RAM.",
            )

            results.set_value(*import_key, value={"client": ret, "database": usage})
        else:
            results.set_value(*import_key, value={"client": "custom_load", "database": "custom_load"})

        # Run all benchmarks in all available groups.
        for group in sorted(queries.keys()):

            if benchmark_context.mode == "Mixed":
                mixed_workload(vendor_runner, client, workload, group, queries, benchmark_context)
            elif benchmark_context.mode == "Realistic":
                mixed_workload(vendor_runner, client, workload, group, queries, benchmark_context)
            else:
                for query, funcname in queries[group]:
                    log.info(
                        "Running query:",
                        "{}/{}/{}/{}".format(group, query, funcname, WITHOUT_FINE_GRAINED_AUTHORIZATION),
                    )
                    func = getattr(workload, funcname)

                    # Query count
                    config_key = [
                        workload.NAME,
                        workload.get_variant(),
                        group,
                        query,
                    ]
                    count = get_query_cache_count(
                        vendor_runner, client, get_queries(func, 1), config_key, benchmark_context
                    )

                    # Benchmark run.
                    print("Sample query:", get_queries(func, 1)[0][0])
                    print(
                        "Executing benchmark with",
                        count,
                        "queries that should " "yield a single-threaded runtime of",
                        benchmark_context.single_threaded_runtime_sec,
                        "seconds.",
                    )
                    print(
                        "Queries are executed using",
                        benchmark_context.num_workers_for_benchmark,
                        "concurrent clients.",
                    )
                    vendor_runner.start_benchmark(
                        workload.NAME + workload.get_variant() + "_" + "_" + benchmark_context.mode + "_" + query
                    )

                    warmup(condition=benchmark_context.warm_up, client=client, queries=get_queries(func, count))
                    if benchmark_context.time_dependent_execution != 0:
                        ret = client.execute(
                            queries=get_queries(func, count),
                            num_workers=benchmark_context.num_workers_for_benchmark,
                            time_dependent_execution=benchmark_context.time_depended_execution,
                        )[0]
                    else:
                        ret = client.execute(
                            queries=get_queries(func, count),
                            num_workers=benchmark_context.num_workers_for_benchmark,
                        )[0]

                    usage = vendor_runner.stop(
                        workload.NAME + workload.get_variant() + "_" + benchmark_context.mode + "_" + query
                    )
                    ret["database"] = usage
                    # Output summary.
                    print()
                    print("Executed", ret["count"], "queries in", ret["duration"], "seconds.")
                    print("Queries have been retried", ret["retries"], "times.")
                    print("Database used {:.3f} seconds of CPU time.".format(usage["cpu"]))
                    print("Database peaked at {:.3f} MiB of memory.".format(usage["memory"] / 1024.0 / 1024.0))
                    print("{:<31} {:>20} {:>20} {:>20}".format("Metadata:", "min", "avg", "max"))
                    metadata = ret["metadata"]
                    for key in sorted(metadata.keys()):
                        print(
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
                        WITHOUT_FINE_GRAINED_AUTHORIZATION,
                    ]
                    results.set_value(*results_key, value=ret)

            # If there is need for authorization testing.
            if benchmark_context.no_authorization:
                print("Running query with authorization")
                vendor_runner.start_benchmark("authorization")
                client.execute(
                    queries=[
                        ("CREATE USER user IDENTIFIED BY 'test';", {}),
                        ("GRANT ALL PRIVILEGES TO user;", {}),
                        ("GRANT CREATE_DELETE ON EDGE_TYPES * TO user;", {}),
                        ("GRANT CREATE_DELETE ON LABELS * TO user;", {}),
                    ]
                )

                client.set_credentials(username="user", password="test")
                vendor_runner.stop("authorization")

                for query, funcname in queries[group]:

                    log.info(
                        "Running query:",
                        "{}/{}/{}/{}".format(group, query, funcname, WITH_FINE_GRAINED_AUTHORIZATION),
                    )
                    func = getattr(workload, funcname)

                    config_key = [
                        workload.NAME,
                        workload.get_variant(),
                        group,
                        query,
                    ]
                    count = get_query_cache_count(
                        vendor_runner,
                        client,
                        get_queries(func, 1),
                        config_key,
                        benchmark_context.single_threaded_runtime_sec,
                        benchmark_context.warm_up,
                    )

                    vendor_runner.start_benchmark("authorization")
                    warmup(type=benchmark_context.warm_up, client=client, queries=get_queries(func, count))
                    ret = client.execute(
                        queries=get_queries(func, count),
                        num_workers=benchmark_context.num_workers_for_benchmark,
                    )[0]
                    usage = vendor_runner.stop("authorization")
                    ret["database"] = usage
                    # Output summary.
                    print()
                    print(
                        "Executed",
                        ret["count"],
                        "queries in",
                        ret["duration"],
                        "seconds.",
                    )
                    print("Queries have been retried", ret["retries"], "times.")
                    print("Database used {:.3f} seconds of CPU time.".format(usage["cpu"]))
                    print("Database peaked at {:.3f} MiB of memory.".format(usage["memory"] / 1024.0 / 1024.0))
                    print("{:<31} {:>20} {:>20} {:>20}".format("Metadata:", "min", "avg", "max"))
                    metadata = ret["metadata"]
                    for key in sorted(metadata.keys()):
                        print(
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

                # Clean up database from any roles and users job
                vendor_runner.start_benchmark("authorizations")
                ret = client.execute(
                    queries=[
                        ("REVOKE LABELS * FROM user;", {}),
                        ("REVOKE EDGE_TYPES * FROM user;", {}),
                        ("DROP USER user;", {}),
                    ]
                )
                vendor_runner.stop("authorization")

    # Save configuration.
    if not benchmark_context.no_save_query_counts:
        cache.save_config(config)

    # Export results.
    if benchmark_context.export_results:
        with open(benchmark_context.export_results, "w") as f:
            json.dump(results.get_data(), f)
