import argparse
import json
import subprocess
from pathlib import Path


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Run graph database benchmarks on supported databases(Memgraph and Neo4j)",
    )
    parser.add_argument(
        "--vendor",
        nargs=2,
        action="append",
        metavar=("vendor_name", "vendor_binary"),
        help="Forward name and paths to vendors binary"
        "Example: --vendor memgraph /path/to/binary --vendor neo4j /path/to/binary",
    )
    parser.add_argument(
        "--dataset-size",
        default="small",
        choices=["small", "medium", "large"],
        help="Pick a dataset size (small, medium, large)",
    )

    parser.add_argument("--dataset-group", default="basic", help="Select a group of queries")

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

    args = parser.parse_args()

    return args


def run_full_benchmarks(vendor, binary, dataset_size, dataset_group, realistic, mixed):

    configurations = [
        # Basic full group test cold
        [
            "--export-results",
            vendor + "_" + dataset_size + "_cold_isolated.json",
        ],
        # Basic full group test hot
        [
            "--export-results",
            vendor + "_" + dataset_size + "_hot_isolated.json",
            "--warmup-run",
        ],
    ]

    # Configurations for full workload
    for count, write, read, update, analytical in realistic:
        cold = [
            "--export-results",
            vendor
            + "_"
            + dataset_size
            + "_cold_realistic_{}_{}_{}_{}_{}.json".format(count, write, read, update, analytical),
            "--mixed-workload",
            count,
            write,
            read,
            update,
            analytical,
        ]

        hot = [
            "--export-results",
            vendor
            + "_"
            + dataset_size
            + "_hot_realistic_{}_{}_{}_{}_{}.json".format(count, write, read, update, analytical),
            "--warmup-run",
            "--mixed-workload",
            count,
            write,
            read,
            update,
            analytical,
        ]
        configurations.append(cold)
        configurations.append(hot)

    # Configurations for workload per query
    for count, write, read, update, analytical, query in mixed:
        cold = [
            "--export-results",
            vendor
            + "_"
            + dataset_size
            + "_cold_mixed_{}_{}_{}_{}_{}_{}.json".format(count, write, read, update, analytical, query),
            "--mixed-workload",
            count,
            write,
            read,
            update,
            analytical,
            query,
        ]
        hot = [
            "--export-results",
            vendor
            + "_"
            + dataset_size
            + "_hot_mixed_{}_{}_{}_{}_{}_{}.json".format(count, write, read, update, analytical, query),
            "--warmup-run",
            "--mixed-workload",
            count,
            write,
            read,
            update,
            analytical,
            query,
        ]
        configurations.append(cold)
        configurations.append(hot)

    default_args = [
        "python3",
        "benchmark.py",
        "--vendor-binary",
        binary,
        "--vendor-name",
        vendor,
        "--num-workers-for-benchmark",
        "12",
        "--no-authorization",
        "pokec/" + dataset_size + "/" + dataset_group + "/*",
        "--tail-latency",
        "100",
    ]

    for config in configurations:
        full_config = default_args + config
        print(full_config)
        subprocess.run(args=full_config, check=True)


def collect_all_results(vendor_name, dataset_size, dataset_group):
    working_directory = Path().absolute()
    print(working_directory)
    results = sorted(working_directory.glob(vendor_name + "_" + dataset_size + "_*.json"))
    summary = {"pokec": {dataset_size: {dataset_group: {}}}}

    for file in results:
        if "summary" in file.name:
            continue
        f = file.open()
        data = json.loads(f.read())
        if data["__run_configuration__"]["condition"] == "hot":
            for key, value in data["pokec"][dataset_size][dataset_group].items():
                key_condition = key + "_hot"
                summary["pokec"][dataset_size][dataset_group][key_condition] = value
        elif data["__run_configuration__"]["condition"] == "cold":
            for key, value in data["pokec"][dataset_size][dataset_group].items():
                key_condition = key + "_cold"
                summary["pokec"][dataset_size][dataset_group][key_condition] = value

    print(summary)

    json_object = json.dumps(summary, indent=4)
    print(json_object)
    with open(vendor_name + "_" + dataset_size + "_summary.json", "w") as f:
        json.dump(summary, f)


if __name__ == "__main__":
    args = parse_arguments()

    realistic = args.realistic
    mixed = args.mixed

    vendor_names = {"memgraph", "neo4j"}
    for vendor_name, vendor_binary in args.vendor:
        path = Path(vendor_binary)
        if vendor_name.lower() in vendor_names and (path.is_file() or path.is_dir()):
            run_full_benchmarks(
                vendor_name,
                vendor_binary,
                args.dataset_size,
                args.dataset_group,
                realistic,
                mixed,
            )
            collect_all_results(vendor_name, args.dataset_size, args.dataset_group)
        else:
            raise Exception(
                "Check that vendor: {} is supported and you are passing right path: {} to binary.".format(
                    vendor_name, path
                )
            )
