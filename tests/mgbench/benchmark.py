#!/usr/bin/env python3

# Copyright 2021 Memgraph Ltd.
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

import datasets
import log
import helpers
import runners


def get_queries(gen, count):
    # Make the generator deterministic.
    random.seed(gen.__name__)
    # Generate queries.
    ret = []
    for i in range(count):
        ret.append(gen())
    return ret


def match_patterns(dataset, variant, group, test, is_default_variant,
                   patterns):
    for pattern in patterns:
        verdict = [fnmatch.fnmatchcase(dataset, pattern[0])]
        if pattern[1] != "":
            verdict.append(fnmatch.fnmatchcase(variant, pattern[1]))
        else:
            verdict.append(is_default_variant)
        verdict.append(fnmatch.fnmatchcase(group, pattern[2]))
        verdict.append(fnmatch.fnmatchcase(test, pattern[3]))
        if all(verdict):
            return True
    return False


def filter_benchmarks(generators, patterns):
    patterns = copy.deepcopy(patterns)
    for i in range(len(patterns)):
        pattern = patterns[i].split("/")
        if len(pattern) > 4 or len(pattern) == 0:
            raise Exception("Invalid benchmark description '" + pattern + "'!")
        pattern.extend(["", "*", "*"][len(pattern) - 1:])
        patterns[i] = pattern
    filtered = []
    for dataset in sorted(generators.keys()):
        generator, tests = generators[dataset]
        for variant in generator.VARIANTS:
            is_default_variant = variant == generator.DEFAULT_VARIANT
            current = collections.defaultdict(list)
            for group in tests:
                for test_name, test_func in tests[group]:
                    if match_patterns(dataset, variant, group, test_name,
                                      is_default_variant, patterns):
                        current[group].append((test_name, test_func))
            if len(current) > 0:
                filtered.append((generator(variant), dict(current)))
    return filtered


# Parse options.
parser = argparse.ArgumentParser(
    description="Memgraph benchmark executor.",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("benchmarks", nargs="*", default="",
                    help="descriptions of benchmarks that should be run; "
                    "multiple descriptions can be specified to run multiple "
                    "benchmarks; the description is specified as "
                    "dataset/variant/group/test; Unix shell-style wildcards "
                    "can be used in the descriptions; variant, group and test "
                    "are optional and they can be left out; the default "
                    "variant is '' which selects the default dataset variant; "
                    "the default group is '*' which selects all groups; the "
                    "default test is '*' which selects all tests")
parser.add_argument("--memgraph-binary",
                    default=helpers.get_binary_path("memgraph"),
                    help="Memgraph binary used for benchmarking")
parser.add_argument("--client-binary",
                    default=helpers.get_binary_path("tests/mgbench/client"),
                    help="client binary used for benchmarking")
parser.add_argument("--num-workers-for-import", type=int,
                    default=multiprocessing.cpu_count() // 2,
                    help="number of workers used to import the dataset")
parser.add_argument("--num-workers-for-benchmark", type=int,
                    default=1,
                    help="number of workers used to execute the benchmark")
parser.add_argument("--single-threaded-runtime-sec", type=int,
                    default=10,
                    help="single threaded duration of each test")
parser.add_argument("--no-load-query-counts", action="store_true",
                    help="disable loading of cached query counts")
parser.add_argument("--no-save-query-counts", action="store_true",
                    help="disable storing of cached query counts")
parser.add_argument("--export-results", default="",
                    help="file path into which results should be exported")
parser.add_argument("--temporary-directory", default="/tmp",
                    help="directory path where temporary data should "
                    "be stored")
parser.add_argument("--no-properties-on-edges", action="store_true",
                    help="disable properties on edges")
args = parser.parse_args()

# Detect available datasets.
generators = {}
for key in dir(datasets):
    if key.startswith("_"):
        continue
    dataset = getattr(datasets, key)
    if not inspect.isclass(dataset) or dataset == datasets.Dataset or \
            not issubclass(dataset, datasets.Dataset):
        continue
    tests = collections.defaultdict(list)
    for funcname in dir(dataset):
        if not funcname.startswith("benchmark__"):
            continue
        group, test = funcname.split("__")[1:]
        tests[group].append((test, funcname))
    generators[dataset.NAME] = (dataset, dict(tests))
    if dataset.PROPERTIES_ON_EDGES and args.no_properties_on_edges:
        raise Exception("The \"{}\" dataset requires properties on edges, "
                        "but you have disabled them!".format(dataset.NAME))

# List datasets if there is no specified dataset.
if len(args.benchmarks) == 0:
    log.init("Available tests")
    for name in sorted(generators.keys()):
        print("Dataset:", name)
        dataset, tests = generators[name]
        print("    Variants:", ", ".join(dataset.VARIANTS),
              "(default: " + dataset.DEFAULT_VARIANT + ")")
        for group in sorted(tests.keys()):
            print("    Group:", group)
            for test_name, test_func in tests[group]:
                print("        Test:", test_name)
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
    log.init("Preparing", dataset.NAME + "/" + dataset.get_variant(),
             "dataset")
    dataset.prepare(cache.cache_directory("datasets", dataset.NAME,
                                          dataset.get_variant()))

    # Prepare runners and import the dataset.
    memgraph = runners.Memgraph(args.memgraph_binary, args.temporary_directory,
                                not args.no_properties_on_edges)
    client = runners.Client(args.client_binary, args.temporary_directory)
    memgraph.start_preparation()
    ret = client.execute(file_path=dataset.get_file(),
                         num_workers=args.num_workers_for_import)
    usage = memgraph.stop()

    # Display import statistics.
    print()
    for row in ret:
        print("Executed", row["count"], "queries in", row["duration"],
              "seconds using", row["num_workers"],
              "workers with a total throughput of", row["throughput"],
              "queries/second.")
    print()
    print("The database used", usage["cpu"],
          "seconds of CPU time and peaked at",
          usage["memory"] / 1024 / 1024, "MiB of RAM.")

    # Save import results.
    import_key = [dataset.NAME, dataset.get_variant(), "__import__"]
    results.set_value(*import_key, value={"client": ret, "database": usage})

    # TODO: cache import data

    # Run all benchmarks in all available groups.
    for group in sorted(tests.keys()):
        for test, funcname in tests[group]:
            log.info("Running test:", "{}/{}".format(group, test))
            func = getattr(dataset, funcname)

            # Get number of queries to execute.
            # TODO: implement minimum number of queries, `max(10, num_workers)`
            config_key = [dataset.NAME, dataset.get_variant(), group, test]
            cached_count = config.get_value(*config_key)
            if cached_count is None:
                print("Determining the number of queries necessary for",
                      args.single_threaded_runtime_sec,
                      "seconds of single-threaded runtime...")
                # First run to prime the query caches.
                memgraph.start_benchmark()
                client.execute(queries=get_queries(func, 1), num_workers=1)
                # Get a sense of the runtime.
                count = 1
                while True:
                    ret = client.execute(queries=get_queries(func, count),
                                         num_workers=1)
                    duration = ret[0]["duration"]
                    should_execute = int(args.single_threaded_runtime_sec /
                                         (duration / count))
                    print("executed_queries={}, total_duration={}, "
                          "query_duration={}, estimated_count={}".format(
                              count, duration, duration / count,
                              should_execute))
                    # We don't have to execute the next iteration when
                    # `should_execute` becomes the same order of magnitude as
                    # `count * 10`.
                    if should_execute / (count * 10) < 10:
                        count = should_execute
                        break
                    else:
                        count = count * 10
                memgraph.stop()
                config.set_value(*config_key, value={
                    "count": count,
                    "duration": args.single_threaded_runtime_sec})
            else:
                print("Using cached query count of", cached_count["count"],
                      "queries for", cached_count["duration"],
                      "seconds of single-threaded runtime.")
                count = int(cached_count["count"] *
                            args.single_threaded_runtime_sec /
                            cached_count["duration"])

            # Benchmark run.
            print("Sample query:", get_queries(func, 1)[0][0])
            print("Executing benchmark with", count, "queries that should "
                  "yield a single-threaded runtime of",
                  args.single_threaded_runtime_sec, "seconds.")
            print("Queries are executed using", args.num_workers_for_benchmark,
                  "concurrent clients.")
            memgraph.start_benchmark()
            ret = client.execute(queries=get_queries(func, count),
                                 num_workers=args.num_workers_for_benchmark)[0]
            usage = memgraph.stop()
            ret["database"] = usage

            # Output summary.
            print()
            print("Executed", ret["count"], "queries in",
                  ret["duration"], "seconds.")
            print("Queries have been retried", ret["retries"], "times.")
            print("Database used {:.3f} seconds of CPU time.".format(
                  usage["cpu"]))
            print("Database peaked at {:.3f} MiB of memory.".format(
                  usage["memory"] / 1024.0 / 1024.0))
            print("{:<31} {:>20} {:>20} {:>20}".format("Metadata:", "min",
                                                       "avg", "max"))
            metadata = ret["metadata"]
            for key in sorted(metadata.keys()):
                print("{name:>30}: {minimum:>20.06f} {average:>20.06f} "
                      "{maximum:>20.06f}".format(name=key, **metadata[key]))
            log.success("Throughput: {:02f} QPS".format(ret["throughput"]))

            # Save results.
            results_key = [dataset.NAME, dataset.get_variant(), group, test]
            results.set_value(*results_key, value=ret)

# Save configuration.
if not args.no_save_query_counts:
    cache.save_config(config)

# Export results.
if args.export_results:
    with open(args.export_results, "w") as f:
        json.dump(results.get_data(), f)
