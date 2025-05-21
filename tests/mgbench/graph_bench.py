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
import subprocess
from pathlib import Path

from constants import BenchmarkClientLanguage, BenchmarkInstallationType, GraphVendors


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Run graph database benchmarks on supported databases(Memgraph and Neo4j)",
    )

    parser.add_argument(
        "--vendor",
        type=str,
        nargs="+",  # Accept multiple if you want to benchmark multiple vendors
        default=GraphVendors.MEMGRAPH,
        choices=GraphVendors.get_all_vendors(),
        required=True,
        help="Choose one or more database vendors: memgraph, neo4j, or falkordb",
    )

    parser.add_argument(
        "--installation-type",
        type=str,
        default=BenchmarkInstallationType.NATIVE,
        choices=BenchmarkInstallationType.get_all_installation_types(),
        required=True,
        help="Specify installation type: native or docker",
    )

    parser.add_argument(
        "--client-language",
        type=str,
        default=BenchmarkClientLanguage.CPP,
        choices=BenchmarkClientLanguage.get_all_client_languages(),
        required=True,
        help="Specify client language implementation: cpp or docker",
    )

    parser.add_argument(
        "--vendor-binary",
        type=str,
        nargs="*",
        help="Path to vendor binary (required if installation type is 'native'). Accept multiple binaries if you want to benchmark multiple vendors",
    )

    parser.add_argument(
        "--dataset-name",
        default="",
        help="Dataset name you wish to execute",
    )

    parser.add_argument(
        "--dataset-size",
        default="",
        help="Pick a dataset variant you wish to execute",
    )

    parser.add_argument("--dataset-group", default="", help="Select a group of queries")

    parser.add_argument(
        "--realistic",
        nargs=5,
        action="append",
        metavar=("num_of_queries", "write", "read", "update", "analytical"),
        help="Forward config for group run",
    )

    parser.add_argument(
        "--mixed",
        nargs=6,
        action="append",
        metavar=(
            "num_of_queries",
            "write",
            "read",
            "update",
            "analytical",
            "query_percentage",
        ),
        help="Forward config for query",
    )

    parser.add_argument(
        "--num-workers-for-benchmark",
        type=int,
        default=12,
        help="number of workers used to execute the benchmark",
    )

    parser.add_argument(
        "--query-count-lower-bound",
        type=int,
        default=300,
        help="number of workers used to execute the benchmark (works only for isolated run)",
    )

    parser.add_argument(
        "--single-threaded-runtime-sec",
        type=int,
        default=30,
        help="Duration of single threaded benchmark per query (works only for isolated run)",
    )

    parser.add_argument(
        "--skip-isolated-cold",
        action="store_true",
        default=False,
        help="Skip isolated cold benchmarks",
    )

    parser.add_argument(
        "--skip-isolated-hot",
        action="store_true",
        default=False,
        help="Skip isolated hot benchmarks",
    )

    parser.add_argument(
        "--skip-isolated-vulcanic",
        action="store_true",
        default=False,
        help="Skip isolated vulcanic benchmarks",
    )

    args = parser.parse_args()

    # Ensure vendor-binary is provided and matches vendor count when installation-type is 'native'
    if args.installation_type == BenchmarkInstallationType.NATIVE:
        if not args.vendor_binary or len(args.vendor_binary) != len(args.vendor):
            parser.error(
                f"--vendor-binary is required when --installation-type is '{BenchmarkInstallationType.NATIVE}'. "
                "Provide one binary path per vendor."
            )

    return args


def run_full_benchmarks(
    vendor,
    installation_type,
    binary,
    client_language,
    dataset,
    dataset_size,
    dataset_group,
    realistic,
    mixed,
    workers,
    query_count_lower_bound,
    single_threaded_runtime_sec,
    skip_isolated_cold,
    skip_isolated_hot,
    skip_isolated_vulcanic,
):
    configurations = []
    if not skip_isolated_cold:
        # Basic isolated test cold
        configurations.append(
            [
                "--export-results",
                f"{vendor}_{str(workers)}_{dataset}_{dataset_size}_cold_isolated.json",
            ]
        )
    if not skip_isolated_hot:
        # Basic isolated test hot
        configurations.append(
            [
                "--export-results",
                f"{vendor}_{str(workers)}_{dataset}_{dataset_size}_hot_isolated.json",
                "--warm-up",
                "hot",
            ]
        )
    if not skip_isolated_vulcanic:
        # Basic isolated test vulcanic
        configurations.append(
            [
                "--export-results",
                f"{vendor}_{str(workers)}_{dataset}_{dataset_size}_vulcanic_isolated.json",
                "--warm-up",
                "vulcanic",
            ]
        )

    if realistic:
        # Configurations for full workload
        for count, write, read, update, analytical in realistic:
            cold = [
                "--export-results",
                f"{vendor}_{str(workers)}_{dataset}_{dataset_size}_cold_realistic_{count}_{write}_{read}_{update}_{analytical}.json",
                "--workload-realistic",
                count,
                write,
                read,
                update,
                analytical,
            ]

            hot = [
                "--export-results",
                f"{vendor}_{str(workers)}_{dataset}_{dataset_size}_hot_realistic_{count}_{write}_{read}_{update}_{analytical}.json",
                "--warm-up",
                "hot",
                "--workload-realistic",
                count,
                write,
                read,
                update,
                analytical,
            ]

            configurations.append(cold)
            configurations.append(hot)

    if mixed:
        # Configurations for workload per query
        for count, write, read, update, analytical, query in mixed:
            cold = [
                "--export-results",
                f"{vendor}_{str(workers)}_{dataset}_{dataset_size}_cold_mixed_{count}_{write}_{read}_{update}_{analytical}_{query}.json",
                "--workload-mixed",
                count,
                write,
                read,
                update,
                analytical,
                query,
            ]
            hot = [
                "--export-results",
                f"{vendor}_{str(workers)}_{dataset}_{dataset_size}_hot_mixed_{count}_{write}_{read}_{update}_{analytical}_{query}.json",
                "--warm-up",
                "hot",
                "--workload-mixed",
                count,
                write,
                read,
                update,
                analytical,
                query,
            ]

            configurations.append(cold)
            configurations.append(hot)

    installation_type_specific_args = (
        [] if installation_type == BenchmarkInstallationType.DOCKER else ["--vendor-binary", binary]
    )

    default_args = (
        ["python3", "benchmark.py"]
        + installation_type_specific_args
        + [
            "--vendor-name",
            vendor,
            "--installation-type",
            installation_type,
            "--client-language",
            client_language,
            "--num-workers-for-benchmark",
            str(workers),
            "--single-threaded-runtime-sec",
            str(single_threaded_runtime_sec),
            "--query-count-lower-bound",
            str(query_count_lower_bound),
            "--no-authorization",
            dataset + "/" + dataset_size + "/" + dataset_group + "/*",
        ]
    )

    for config in configurations:
        full_config = default_args + config
        print(full_config)
        subprocess.run(args=full_config, check=True)


def collect_all_results(vendor_name, dataset, dataset_size, dataset_group, workers):
    working_directory = Path().absolute()
    print(working_directory)
    results = sorted(
        working_directory.glob(vendor_name + "_" + str(workers) + "_" + dataset + "_" + dataset_size + "_*.json")
    )
    summary = {dataset: {dataset_size: {dataset_group: {}}}}

    for file in results:
        if "summary" in file.name:
            continue
        f = file.open()
        data = json.loads(f.read())
        if data["__run_configuration__"]["condition"] == "hot":
            for key, value in data[dataset][dataset_size][dataset_group].items():
                key_condition = key + "_hot"
                summary[dataset][dataset_size][dataset_group][key_condition] = value
        elif data["__run_configuration__"]["condition"] == "cold":
            for key, value in data[dataset][dataset_size][dataset_group].items():
                key_condition = key + "_cold"
                summary[dataset][dataset_size][dataset_group][key_condition] = value
        elif data["__run_configuration__"]["condition"] == "vulcanic":
            for key, value in data[dataset][dataset_size][dataset_group].items():
                key_condition = key + "_vulcanic"
                summary[dataset][dataset_size][dataset_group][key_condition] = value
    print(summary)

    json_object = json.dumps(summary, indent=4)
    print(json_object)
    with open(vendor_name + "_" + str(workers) + "_" + dataset + "_" + dataset_size + "_summary.json", "w") as f:
        json.dump(summary, f)


if __name__ == "__main__":
    args = parse_arguments()

    realistic = args.realistic
    mixed = args.mixed

    vendors = GraphVendors.get_all_vendors()

    # Pre-check if all the vendors are supported for the benchmark
    for vendor in args.vendor:
        if vendor.lower() not in vendors:
            raise Exception(f"Check that vendor: {vendor} is supported!")

    for i in range(len(args.vendor)):
        vendor_name = args.vendor[i]
        vendor_binary = (
            None if args.installation_type == BenchmarkInstallationType.DOCKER else Path(args.vendor_binary[i])
        )

        if vendor_binary is not None and not vendor_binary.is_file():
            raise Exception(f"Binary path provided: {vendor_binary} seems not to be a file!")

        run_full_benchmarks(
            vendor_name,
            args.installation_type,
            vendor_binary,
            args.client_language,
            args.dataset_name,
            args.dataset_size,
            args.dataset_group,
            realistic,
            mixed,
            args.num_workers_for_benchmark,
            args.query_count_lower_bound,
            args.single_threaded_runtime_sec,
            args.skip_isolated_cold,
            args.skip_isolated_hot,
            args.skip_isolated_vulcanic,
        )
        collect_all_results(
            vendor_name, args.dataset_name, args.dataset_size, args.dataset_group, args.num_workers_for_benchmark
        )
