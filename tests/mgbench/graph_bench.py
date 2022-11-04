import subprocess
import argparse
import json
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

    parser.add_argument(
        "--mixed-group",
        nargs=5,
        action="append",
        metavar=("num_of_queries", "write", "read", "update", "analytical"),
        help="Forward config for group run",
    )

    parser.add_argument(
        "--mixed-per-query",
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


def run_full_benchmarks(vendor, binary, dataset_size, group_configs, per_query_configs):

    configurations = [
        # Basic full group test cold
        [
            "--export-results",
            vendor + "_cold.json",
        ],
        # Basic full group test warm
        [
            "--export-results",
            vendor + "_warm.json",
            "--warmup-run",
        ],
    ]

    # Configurations for full workload
    for count, write, read, update, analytical in group_configs:
        cold = [
            "--export-results",
            vendor
            + "_cold_mixed_group_{}_{}_{}_{}_{}.json".format(
                count, write, read, update, analytical
            ),
            "--mixed-workload",
            count,
            write,
            read,
            update,
            analytical,
        ]

        warm = [
            "--export-results",
            vendor
            + "_warm_mixed_group_{}_{}_{}_{}_{}.json".format(
                count, write, read, update, analytical
            ),
            "--mixed-workload",
            count,
            write,
            read,
            update,
            analytical,
            "--warmup-run",
        ]
        configurations.append(cold)
        configurations.append(warm)

    # Configurations for workload per query
    for count, write, read, update, analytical, query in per_query_configs:
        cold = [
            "--export-results",
            vendor
            + "_cold_mixed_query_{}_{}_{}_{}_{}_{}.json".format(
                count, write, read, update, analytical, query
            ),
            "--mixed-workload",
            count,
            write,
            read,
            update,
            analytical,
            query,
        ]
        warm = [
            "--export-results",
            vendor
            + "_warm_mixed_query_{}_{}_{}_{}_{}_{}.json".format(
                count, write, read, update, analytical, query
            ),
            "--mixed-workload",
            count,
            write,
            read,
            update,
            analytical,
            query,
            "--warmup-run",
        ]
        configurations.append(cold)
        configurations.append(warm)

    default_args = [
        "python3",
        "benchmark.py",
        "--vendor-binary",
        binary,
        "--vendor-name",
        vendor,
        "--num-workers-for-benchmark",
        "12",
        "pokec/" + dataset_size + "/mixed/*/*",
    ]

    for config in configurations:
        print(config)
        default_args.extend(config)
        print(default_args)
        subprocess.run(args=default_args, check=True)


def collect_all_results(vendor_name, dataset_size):
    working_directory = Path().absolute()
    print(working_directory)
    results = sorted(working_directory.glob(vendor_name + "_*.json"))
    summary = {
        "pokec": {
            dataset_size: {
                "cold": [],
                "warm": [],
            }
        }
    }

    for file in results:
        f = file.open()
        data = json.loads(f.read())
        if "cold" in file.name:
            summary["pokec"][dataset_size]["cold"].append(data["pokec"][dataset_size])
        elif "warm" in file.name:
            summary["pokec"][dataset_size]["warm"].append(data["pokec"][dataset_size])

    print(summary)

    json_object = json.dumps(summary, indent=4)
    print(json_object)
    with open(vendor_name + "_summary.json", "w") as f:
        json.dump(summary, f)


if __name__ == "__main__":
    args = parse_arguments()

    group_run_mixed = args.mixed_group
    per_query_mixed = args.mixed_per_query

    vendor_names = {"memgraph", "neo4j"}
    for vendor_name, vendor_binary in args.vendor:
        path = Path(vendor_binary)
        if vendor_name.lower() in vendor_names and path.is_file():
            # run_full_benchmarks(
            #     vendor_name,
            #     vendor_binary,
            #     args.dataset_size,
            #     group_run_mixed,
            #     per_query_mixed,
            # )
            collect_all_results(vendor_name, args.dataset_size)
        else:
            raise Exception(
                "Check that vendor: {} is supported and you are passing right path: {} to binary.".format(
                    vendor_name, path
                )
            )
