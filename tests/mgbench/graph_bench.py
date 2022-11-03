import subprocess
import argparse
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
    args = parser.parse_args()

    return args


def run_full_benchmarks(vendor, binary, dataset_size):
    # All run configurations
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
        # Mixed workload per query, 30% write cold
        [
            "--export-results",
            vendor + "_cold_mixed_per_query.json",
            "pokec/" + dataset_size + "/mixed/*/*",
            "--mixed-workload",
            "100 30 0 0 0 70",
        ],
        # Mixed workload per query, 30% write warm
        [
            "--export-results",
            vendor + "_warm_mixed_per_query.json",
            "pokec/" + dataset_size + "/mixed/*/*",
            "--warmup-run",
            "--mixed-workload",
            "100 30 0 0 0 70",
        ],
        # Mixed workload per full group 30% write, 70% read cold
        [
            "--export-results",
            vendor + "_cold_mixed_group.json",
            "pokec/" + dataset_size + "/mixed/*/*",
            "--mixed-workload",
            "100 30 70 0 0",
        ],
        # Mixed workload per full group 30% write, 70% read warm
        [
            "--export-results",
            vendor + "_warm_mixed_group.json",
            "pokec/" + dataset_size + "/mixed/*/*",
            "--warmup-run",
            "--mixed-workload",
            "100 30 70 0 0",
        ],
        # Mixed workload per full group 50% write, 50% read cold
        [
            "--export-results",
            vendor + "_warm_mixed_group.json",
            "pokec/" + dataset_size + "/mixed/*/*",
            "--mixed-workload",
            "100 50 50 0 0",
        ],
        # Mixed workload per full group 50% write, 50% read warm
        [
            "--export-results",
            vendor + "_warm_mixed_group.json",
            "pokec/" + dataset_size + "/mixed/*/*",
            "--warmup-run",
            "--mixed-workload",
            "100 50 50 0 0",
        ],
        # Mixed workload per full group 70% write, 30% read cold
        [
            "--export-results",
            vendor + "_warm_mixed_group.json",
            "pokec/" + dataset_size + "/mixed/*/*",
            "--mixed-workload",
            "100 70 30 0 0",
        ],
        # Mixed workload per full group 70% write, 30% read cold
        [
            "--export-results",
            vendor + "_warm_mixed_group.json",
            "pokec/" + dataset_size + "/mixed/*/*",
            "--warmup-run",
            "--mixed-workload",
            "100 70 30 0 0",
        ],
    ]

    default_args = [
        "python3",
        "benchmark.py",
        "--vendor-binary",
        binary,
        "--vendor-name",
        vendor,
        "--num-workers-for-benchmark",
        "12",
        "pokec/small/mixed/*/*",
    ]

    for config in configurations:
        default_args.extend(config)
        subprocess.run(args=default_args, check=True)


if __name__ == "__main__":
    args = parse_arguments()
    vendor_names = {"memgraph", "neo4j"}
    for vendor_name, vendor_binary in args.vendor:
        path = Path(vendor_binary)
        if vendor_name.lower() in vendor_names and path.is_file():
            run_full_benchmarks(vendor_name, vendor_binary, args.dataset_size)
        else:
            raise Exception(
                "Check that vendor: {} is supported and you are passing right path: {} to binary.".format(
                    vendor_name, path
                )
            )
