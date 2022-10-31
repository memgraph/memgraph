#!/usr/bin/env python3

# Copyright 2022 Memgraph Ltd.
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
import copy
import fnmatch
import inspect
import json
import multiprocessing
import random
import sys
import statistics

import datasets
import log
import helpers
import runners


# Parse options.
parser = argparse.ArgumentParser(
    description="Memgraph benchmark executor.",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument(
    "benchmarks",
    nargs="*",
    default="",
    help="descriptions of benchmarks that should be run; "
    "multiple descriptions can be specified to run multiple "
    "benchmarks; the description is specified as "
    "dataset/variant/workload/group/test; Unix shell-style wildcards "
    "can be used in the descriptions; variant, workload, group and test "
    "are optional and they can be left out; the default "
    "variant is '' which selects the default dataset variant; "
    "the default workload is '*' which selects all workloads; the "
    "default group is '*' which selects all groups; the"
    "default test is '*' which selects all tests",
)
parser.add_argument(
    "--vendor-binary",
    help="Vendor binary used for benchmarking",
)

parser.add_argument(
    "--vendor-name",
    default="memgraph",
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
    help="single threaded duration of each test",
)
parser.add_argument(
    "--no-load-query-counts",
    action="store_true",
    help="disable loading of cached query counts",
)
parser.add_argument(
    "--no-save-query-counts",
    action="store_true",
    help="disable storing of cached query counts",
)
parser.add_argument(
    "--export-results",
    default="",
    help="file path into which results should be exported",
)
parser.add_argument(
    "--temporary-directory",
    default="/tmp",
    help="directory path where temporary data should " "be stored",
)
parser.add_argument(
    "--no-properties-on-edges", action="store_true", help="disable properties on edges"
)

parser.add_argument("--bolt-port", default=7687, help="memgraph bolt port")

parser.add_argument(
    "--with-authorization",
    action="store_true",
    default=False,
    help="Run each test with authorization",
)

parser.add_argument(
    "--warmup-run",
    action="store_true",
    default=False,
    help="Run warmup before benchmarks",
)

parser.add_argument(
    "--mixed-workload",
    nargs=5,
    type=int,
    default=[0, 0, 0, 0, 0],
    help="Run combination that defines mixed workload;"
    "Pass the positional arguments as values of what percentage of"
    "write/read/update/analytical queries you want;"
    "Running --mixed-workload 1000 20 70 10 0"
    "The command above will execute 1000 queries, 20% write, "
    "70% read, 10% update and 0% analytical",
)

args = parser.parse_args()


def get_queries(gen, count):
    # Make the generator deterministic.
    random.seed(gen.__name__)
    # Generate queries.
    ret = []
    for i in range(count):
        ret.append(gen())
    return ret


def match_patterns(
    dataset, variant, workload, group, test, is_default_variant, patterns
):
    for pattern in patterns:
        verdict = [fnmatch.fnmatchcase(dataset, pattern[0])]
        if pattern[1] != "":
            verdict.append(fnmatch.fnmatchcase(variant, pattern[1]))
        else:
            verdict.append(is_default_variant)
        verdict.append(fnmatch.fnmatchcase(workload, pattern[2]))
        verdict.append(fnmatch.fnmatchcase(group, pattern[3]))
        verdict.append(fnmatch.fnmatchcase(test, pattern[4]))
        if all(verdict):
            return True
    return False


def filter_benchmarks(generators, patterns):
    patterns = copy.deepcopy(patterns)
    for i in range(len(patterns)):
        pattern = patterns[i].split("/")
        if len(pattern) > 5 or len(pattern) == 0:
            raise Exception("Invalid benchmark description '" + pattern + "'!")
        pattern.extend(["", "*", "*"][len(pattern) - 1 :])
        patterns[i] = pattern
    filtered = []
    for dataset in sorted(generators.keys()):
        generator, tests = generators[dataset]
        for variant in generator.VARIANTS:
            is_default_variant = variant == generator.DEFAULT_VARIANT
            current = collections.defaultdict(list)
            for workload in tests:
                for test_name, test_func, test_group in tests[workload]:
                    if match_patterns(
                        dataset,
                        variant,
                        workload,
                        test_group,
                        test_name,
                        is_default_variant,
                        patterns,
                    ):
                        current[workload].append((test_name, test_func, test_group))
            if len(current) > 0:
                filtered.append((generator(variant), dict(current)))
    return filtered


def warmup(client):
    print("Running warm-up process.")
    client.execute(
        queries=[
            ("CREATE ();", {}),
            ("CREATE ()-[:TempEdge]->();", {}),
            ("MATCH (n) RETURN n LIMIT 1;", {}),
        ],
        num_workers=1,
    )


def tail_latency(vendor, client, func):
    vendor.start_benchmark()
    if args.warmup_run:
        warmup(client)
    latency = []
    iteration = 100
    query_list = get_queries(func, 100)
    for i in range(0, iteration):
        ret = client.execute(queries=[query_list[i]], num_workers=1)
        latency.append(ret[0]["duration"])
    latency.sort()
    query_stats = {
        "iterations": iteration,
        "min": latency[0],
        "max": latency[99],
        "mean": statistics.fmean(latency),
        "p99": latency[98],
        "p95": latency[94],
        "p90": latency[89],
        "p50": latency[49],
    }
    print("Query statistics for tail latency: ")
    print(query_stats)
    vendor.stop()
    return query_stats


def mixed_workload(vendor, client, dataset, workload, tests):
    num_of_queries = args.mixed_workload[0]
    percentage_distribution = args.mixed_workload[1:]
    if sum(percentage_distribution) != 100:
        raise Exception(
            "Please make sure that passed arguments % sum to 100% percent!, passed: ",
            percentage_distribution,
        )
    s = [str(i) for i in args.mixed_workload]

    config_distribution = "_".join(s)

    mixed_config_key = [
        dataset.NAME,
        dataset.get_variant(),
        workload,
        "mixed_workload",
        config_distribution,
    ]
    cached_mixed_workload = config.get_value(*mixed_config_key)
    full_workload = []
    if cached_mixed_workload is None:

        print("Generating mixed workload")
        write = list()
        read = list()
        update = list()
        analytical = list()

        for test, funcname, group in tests[workload]:
            if group == "write":
                write.append(funcname)
            elif group == "read":
                read.append(funcname)
            elif group == "update":
                update.append(funcname)
            elif group == "analytical":
                analytical.append(funcname)

        if (
            len(write) == 0
            and percentage_distribution[0] != 0
            or len(read) == 0
            and percentage_distribution[1] != 0
            or len(update) == 0
            and percentage_distribution[2] != 0
            or len(analytical) == 0
            and percentage_distribution[3] != 0
        ):
            raise Exception(
                "There is a missing query in group (write, read, update or analytical) for given workload distribution."
            )

        options = ["w", "r", "u", "a"]
        function_type = random.choices(
            population=options, weights=percentage_distribution, k=num_of_queries
        )

        for t in function_type:
            # Get the apropropriate functions with same probabilty
            if t == "w":
                funcname = random.choices(write, k=1)[0]
                query = getattr(dataset, funcname)
                full_workload.append(query())
            elif t == "r":
                funcname = random.choices(read, k=1)[0]
                query = getattr(dataset, funcname)
                full_workload.append(query())
            elif t == "u":
                funcname = random.choices(update, k=1)[0]
                query = getattr(dataset, funcname)
                full_workload.append(query())
            elif t == "a":
                funcname = random.choices(analytical, k=1)[0]
                query = getattr(dataset, funcname)
                full_workload.append(query())
        query_mix = {}
        query_mix["queries"] = full_workload
        config.set_value(*mixed_config_key, value=query_mix)

        cache.save_config(config)

    else:
        print("Using cached queries for mixed workload")
        full_workload = cached_mixed_workload["queries"]

    vendor.start_benchmark()
    if args.warmup_run:
        warmup(client)
    ret = client.execute(
        queries=full_workload,
        num_workers=args.num_workers_for_benchmark,
    )[0]
    usage_workload = vendor.stop()

    mixed_workload = {
        "count": ret["count"],
        "duration": ret["duration"],
        "retires": ret["retries"],
        "throughput": ret["throughput"],
        "memory": usage_workload["memory"],
        "cpu": usage_workload["cpu"],
    }
    results_key = [
        dataset.NAME,
        dataset.get_variant(),
        workload,
        config_distribution,
    ]
    results.set_value(*results_key, value=mixed_workload)

    print(mixed_workload)


def get_test_cache_count(vendor, client, func, config_key):
    cached_count = config.get_value(*config_key)

    if cached_count is None:
        print(
            "Determining the number of queries necessary for",
            args.single_threaded_runtime_sec,
            "seconds of single-threaded runtime...",
        )
        # First run to prime the query caches.
        vendor.start_benchmark()
        if args.warmup_run:
            warmup(client)
        client.execute(queries=get_queries(func, 1), num_workers=1)
        # Get a sense of the runtime.
        count = 1
        while True:
            ret = client.execute(queries=get_queries(func, count), num_workers=1)
            duration = ret[0]["duration"]
            should_execute = int(args.single_threaded_runtime_sec / (duration / count))
            print(
                "executed_queries={}, total_duration={}, "
                "query_duration={}, estimated_count={}".format(
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
        vendor.stop()
        config.set_value(
            *config_key,
            value={
                "count": count,
                "duration": args.single_threaded_runtime_sec,
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
        count = int(
            cached_count["count"]
            * args.single_threaded_runtime_sec
            / cached_count["duration"]
        )
    return count


# Detect available datasets.
generators = {}
for key in dir(datasets):
    if key.startswith("_"):
        continue
    dataset = getattr(datasets, key)
    if (
        not inspect.isclass(dataset)
        or dataset == datasets.Dataset
        or not issubclass(dataset, datasets.Dataset)
    ):
        continue
    tests = collections.defaultdict(list)
    for funcname in dir(dataset):
        if not funcname.startswith("benchmark__"):
            continue
        workload, group, test = funcname.split("__")[1:]
        tests[workload].append((test, funcname, group))
    generators[dataset.NAME] = (dataset, dict(tests))
    if dataset.PROPERTIES_ON_EDGES and args.no_properties_on_edges:
        raise Exception(
            'The "{}" dataset requires properties on edges, '
            "but you have disabled them!".format(dataset.NAME)
        )

# List datasets if there is no specified dataset.
if len(args.benchmarks) == 0:
    log.init("Available tests")
    for name in sorted(generators.keys()):
        print("Dataset:", name)
        dataset, tests = generators[name]
        print(
            "    Variants:",
            ", ".join(dataset.VARIANTS),
            "(default: " + dataset.DEFAULT_VARIANT + ")",
        )
        for workload in sorted(tests.keys()):
            print("    workload:", workload)
            for test_name, test_func, group in tests[workload]:
                print("        Test:", test_name)
                print("        Group:", group)
    sys.exit(0)

# Create cache, config and results objects.
cache = helpers.Cache()
if not args.no_load_query_counts:
    config = cache.load_config()
else:
    config = helpers.RecursiveDict()
results = helpers.RecursiveDict()

# Filter out the generators.
benchmarks = filter_benchmarks(generators, args.benchmarks)
# Run all specified benchmarks.
for dataset, tests in benchmarks:
    log.init("Preparing", dataset.NAME + "/" + dataset.get_variant(), "dataset")
    dataset.prepare(
        cache.cache_directory("datasets", dataset.NAME, dataset.get_variant())
    )

    # TODO: Create some apstract class for vendors, that will hold this data
    if args.vendor_name == "neo4j":
        vendor = runners.Neo4j(
            args.vendor_binary,
            args.temporary_directory,
            args.bolt_port,
        )
    else:
        vendor = runners.Memgraph(
            args.vendor_binary,
            args.temporary_directory,
            not args.no_properties_on_edges,
            args.bolt_port,
        )

    client = runners.Client(
        args.client_binary, args.temporary_directory, args.bolt_port
    )
    vendor.start_preparation()
    # TODO:Neo4j doesn't like duplicated triggers, add this do dataset files.
    client.execute(
        queries=[
            ("DROP INDEX ON:User(id)", {}),
        ]
    )
    ret = client.execute(
        file_path=dataset.get_file(), num_workers=args.num_workers_for_import
    )
    usage = vendor.stop()
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

    # Save import results.
    import_key = [dataset.NAME, dataset.get_variant(), "__import__"]
    results.set_value(*import_key, value={"client": ret, "database": usage})

    # TODO: cache import data

    # Run all benchmarks in all available groups.
    for workload in sorted(tests.keys()):

        if args.mixed_workload[0] > 0:
            mixed_workload(vendor, client, dataset, workload, tests)

        for test, funcname, group in tests[workload]:
            log.info("Running test:", "{}/{}/{}".format(group, test, funcname))
            func = getattr(dataset, funcname)

            query_statistics = tail_latency(vendor, client, func)

            config_key = [
                dataset.NAME,
                dataset.get_variant(),
                args.vendor_name,
                workload,
                group,
                test,
            ]
            count = get_test_cache_count(vendor, client, func, config_key)

            # Benchmark run.
            print("Sample query:", get_queries(func, 1)[0][0])
            print(
                "Executing benchmark with",
                count,
                "queries that should " "yield a single-threaded runtime of",
                args.single_threaded_runtime_sec,
                "seconds.",
            )
            print(
                "Queries are executed using",
                args.num_workers_for_benchmark,
                "concurrent clients.",
            )
            vendor.start_benchmark()
            if args.warmup_run:
                warmup(client)
            ret = client.execute(
                queries=get_queries(func, count),
                num_workers=args.num_workers_for_benchmark,
            )[0]
            usage = vendor.stop()
            ret["database"] = usage
            ret["query_statistics"] = query_statistics

            # Output summary.
            print()
            print("Executed", ret["count"], "queries in", ret["duration"], "seconds.")
            print("Queries have been retried", ret["retries"], "times.")
            print("Database used {:.3f} seconds of CPU time.".format(usage["cpu"]))
            print(
                "Database peaked at {:.3f} MiB of memory.".format(
                    usage["memory"] / 1024.0 / 1024.0
                )
            )
            print(
                "{:<31} {:>20} {:>20} {:>20}".format("Metadata:", "min", "avg", "max")
            )
            metadata = ret["metadata"]
            for key in sorted(metadata.keys()):
                print(
                    "{name:>30}: {minimum:>20.06f} {average:>20.06f} "
                    "{maximum:>20.06f}".format(name=key, **metadata[key])
                )
            log.success("Throughput: {:02f} QPS".format(ret["throughput"]))

            # Save results.
            results_key = [
                dataset.NAME,
                dataset.get_variant(),
                workload,
                group,
                test,
            ]
            results.set_value(*results_key, value=ret)

    ## If there is need for authorization testing.
    if args.with_authorization:
        print("Running test with authorization")
        vendor.start_benchmark()
        client.execute(
            queries=[
                ("CREATE USER user IDENTIFIED BY 'test';", {}),
                ("GRANT ALL PRIVILEGES TO user;", {}),
                ("GRANT CREATE_DELETE ON EDGE_TYPES * TO user;", {}),
                ("GRANT CREATE_DELETE ON LABELS * TO user;", {}),
            ]
        )
        client = runners.Client(
            args.client_binary,
            args.temporary_directory,
            args.bolt_port,
            username="user",
            password="test",
        )
        vendor.stop()

        for workload in sorted(tests.keys()):
            for test, funcname, group in tests[workload]:

                log.info("Running test:", "{}/{}/{}".format(group, test, funcname))
                func = getattr(dataset, funcname)

                query_statistics = tail_latency(vendor, client, func)

                # Get number of queries to execute.
                # TODO: implement minimum number of queries, `max(10, num_workers)`
                config_key = [
                    dataset.NAME,
                    dataset.get_variant(),
                    args.vendor_name,
                    workload,
                    group,
                    test,
                ]
                count = get_test_cache_count(vendor, client, func, config_key)

                vendor.start_benchmark()
                if args.warmup_run:
                    warmup(client)
                ret = client.execute(
                    queries=get_queries(func, count),
                    num_workers=args.num_workers_for_benchmark,
                )[0]
                usage = vendor.stop()
                ret["database"] = usage
                ret["query_statistics"] = query_statistics

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
                print(
                    "Database peaked at {:.3f} MiB of memory.".format(
                        usage["memory"] / 1024.0 / 1024.0
                    )
                )
                print(
                    "{:<31} {:>20} {:>20} {:>20}".format(
                        "Metadata:", "min", "avg", "max"
                    )
                )
                metadata = ret["metadata"]
                for key in sorted(metadata.keys()):
                    print(
                        "{name:>30}: {minimum:>20.06f} {average:>20.06f} "
                        "{maximum:>20.06f}".format(name=key, **metadata[key])
                    )
                log.success("Throughput: {:02f} QPS".format(ret["throughput"]))
                # Save results.
                results_key = [
                    dataset.NAME,
                    dataset.get_variant(),
                    workload,
                    group,
                    test,
                    "authorization",
                ]
                results.set_value(*results_key, value=ret)

        # Clean up database from any roles and users job
        vendor.start_benchmark()
        ret = client.execute(
            queries=[
                ("REVOKE LABELS * FROM user;", {}),
                ("REVOKE EDGE_TYPES * FROM user;", {}),
                ("DROP USER user;", {}),
            ]
        )
        print("Cleanup status: ")
        print(ret)
        vendor.stop()


# Save configuration.
if not args.no_save_query_counts:
    cache.save_config(config)

# Export results.
if args.export_results:
    with open(args.export_results, "w") as f:
        json.dump(results.get_data(), f)
