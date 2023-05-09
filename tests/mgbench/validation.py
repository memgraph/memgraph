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
import copy
import multiprocessing
import random

import helpers
import runners
import workloads
from benchmark_context import BenchmarkContext
from workloads import base


def pars_args():
    parser = argparse.ArgumentParser(
        prog="Validator for individual query checking",
        description="""Validates that query is running, and validates output between different vendors""",
    )
    parser.add_argument(
        "benchmarks",
        nargs="*",
        default="",
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
        "--vendor-binary-1",
        help="Vendor binary used for benchmarking, by default it is memgraph",
        default=helpers.get_binary_path("memgraph"),
    )

    parser.add_argument(
        "--vendor-name-1",
        default="memgraph",
        choices=["memgraph", "neo4j"],
        help="Input vendor binary name (memgraph, neo4j)",
    )

    parser.add_argument(
        "--vendor-binary-2",
        help="Vendor binary used for benchmarking, by default it is memgraph",
        default=helpers.get_binary_path("memgraph"),
    )

    parser.add_argument(
        "--vendor-name-2",
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
        "--temporary-directory",
        default="/tmp",
        help="directory path where temporary data should " "be stored",
    )

    parser.add_argument(
        "--num-workers-for-import",
        type=int,
        default=multiprocessing.cpu_count() // 2,
        help="number of workers used to import the dataset",
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


if __name__ == "__main__":
    args = pars_args()

    benchmark_context_db_1 = BenchmarkContext(
        vendor_name=args.vendor_name_1,
        vendor_binary=args.vendor_binary_1,
        benchmark_target_workload=copy.copy(args.benchmarks),
        client_binary=args.client_binary,
        num_workers_for_import=args.num_workers_for_import,
        temporary_directory=args.temporary_directory,
    )

    available_workloads = helpers.get_available_workloads()

    print(helpers.list_available_workloads())

    vendor_runner = runners.BaseRunner.create(
        benchmark_context=benchmark_context_db_1,
    )

    cache = helpers.Cache()
    client = vendor_runner.fetch_client()

    workloads = helpers.filter_workloads(
        available_workloads=available_workloads, benchmark_context=benchmark_context_db_1
    )

    results_db_1 = {}

    for workload, queries in workloads:
        vendor_runner.clean_db()

        generated_queries = workload.dataset_generator()
        if generated_queries:
            vendor_runner.start_db_init("import")
            client.execute(queries=generated_queries, num_workers=benchmark_context_db_1.num_workers_for_import)
            vendor_runner.stop_db_init("import")
        else:
            workload.prepare(cache.cache_directory("datasets", workload.NAME, workload.get_variant()))
            imported = workload.custom_import()
            if not imported:
                vendor_runner.start_db_init("import")
                print("Executing database cleanup and index setup...")
                client.execute(
                    file_path=workload.get_index(), num_workers=benchmark_context_db_1.num_workers_for_import
                )
                print("Importing dataset...")
                ret = client.execute(
                    file_path=workload.get_file(), num_workers=benchmark_context_db_1.num_workers_for_import
                )
                usage = vendor_runner.stop_db_init("import")

        for group in sorted(queries.keys()):
            for query, funcname in queries[group]:
                print("Running query:{}/{}/{}".format(group, query, funcname))
                func = getattr(workload, funcname)
                count = 1
                vendor_runner.start_db("validation")
                try:
                    ret = client.execute(queries=get_queries(func, count), num_workers=1, validation=True)[0]
                    results_db_1[funcname] = ret["results"].items()
                except Exception as e:
                    print("Issue running the query" + funcname)
                    print(e)
                    results_db_1[funcname] = "Query not executed properly"
                finally:
                    usage = vendor_runner.stop_db("validation")
                    print("Database used {:.3f} seconds of CPU time.".format(usage["cpu"]))
                    print("Database peaked at {:.3f} MiB of memory.".format(usage["memory"] / 1024.0 / 1024.0))

    benchmark_context_db_2 = BenchmarkContext(
        vendor_name=args.vendor_name_2,
        vendor_binary=args.vendor_binary_2,
        benchmark_target_workload=copy.copy(args.benchmarks),
        client_binary=args.client_binary,
        num_workers_for_import=args.num_workers_for_import,
        temporary_directory=args.temporary_directory,
    )

    vendor_runner = runners.BaseRunner.create(
        benchmark_context=benchmark_context_db_2,
    )
    available_workloads = helpers.get_available_workloads()

    workloads = helpers.filter_workloads(available_workloads, benchmark_context=benchmark_context_db_2)

    client = vendor_runner.fetch_client()

    results_db_2 = {}

    for workload, queries in workloads:
        vendor_runner.clean_db()

        generated_queries = workload.dataset_generator()
        if generated_queries:
            vendor_runner.start_db_init("import")
            client.execute(queries=generated_queries, num_workers=benchmark_context_db_2.num_workers_for_import)
            vendor_runner.stop("import")
        else:
            workload.prepare(cache.cache_directory("datasets", workload.NAME, workload.get_variant()))
            imported = workload.custom_import()
            if not imported:
                vendor_runner.start_db_init("import")
                print("Executing database cleanup and index setup...")
                client.execute(
                    file_path=workload.get_index(), num_workers=benchmark_context_db_2.num_workers_for_import
                )
                print("Importing dataset...")
                ret = client.execute(
                    file_path=workload.get_file(), num_workers=benchmark_context_db_2.num_workers_for_import
                )
                usage = vendor_runner.stop_db_init("import")

        for group in sorted(queries.keys()):
            for query, funcname in queries[group]:
                print("Running query:{}/{}/{}".format(group, query, funcname))
                func = getattr(workload, funcname)
                count = 1
                vendor_runner.start_db("validation")
                try:
                    ret = client.execute(queries=get_queries(func, count), num_workers=1, validation=True)[0]
                    results_db_2[funcname] = ret["results"].items()
                except Exception as e:
                    print("Issue running the query" + funcname)
                    print(e)
                    results_db_2[funcname] = "Query not executed properly"
                finally:
                    usage = vendor_runner.stop_db("validation")
                    print("Database used {:.3f} seconds of CPU time.".format(usage["cpu"]))
                    print("Database peaked at {:.3f} MiB of memory.".format(usage["memory"] / 1024.0 / 1024.0))

    validation = {}
    for key in results_db_1.keys():
        if type(results_db_1[key]) is str:
            validation[key] = "Query not executed properly."
        else:
            db_1_values = set()
            for index, value in results_db_1[key]:
                db_1_values.add(value)
            neo4j_values = set()
            for index, value in results_db_2[key]:
                neo4j_values.add(value)

            if db_1_values == neo4j_values:
                validation[key] = "Identical results"
            else:
                validation[key] = "Different results, check manually."

    for key, value in validation.items():
        print(key + " " + value)
