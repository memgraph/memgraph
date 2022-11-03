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
    args = parser.parse_args()

    return args


def run_full_benchmarks(vendor, binary):
    subprocess.run(
        args=[
            "python3",
            "benchmark.py",
            "--vendor-binary",
            binary,
            "--vendor-name",
            vendor,
            "--num-workers-for-benchmark",
            "12",
            "--export-results",
            "out.json",
            "pokec/small/mixed/*/*",
            "--warmup-run",
            "--mixed-workload",
            "100",
            "30",
            "0",
            "0",
            "0",
            "70",
        ],
        check=True,
    )


if __name__ == "__main__":
    args = parse_arguments()
    vendor_names = {"memgraph", "neo4j"}
    for vendor_name, vendor_binary in args.vendor:
        path = Path(vendor_binary)
        if vendor_name.lower() in vendor_names and path.is_file():
            run_full_benchmarks(vendor_name, vendor_binary)
        else:
            raise Exception(
                "Check that vendor: {} is supported and you are passing right path: {} to binary.".format(
                    vendor_name, path
                )
            )
