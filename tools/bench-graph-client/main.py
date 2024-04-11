#!/usr/bin/env python3

"""
Bench Graph client responsible for sending benchmarking data in JSON format to
the Bench Graph server.
"""

import json
import logging
import os
import subprocess
from argparse import ArgumentParser
from datetime import datetime

import requests

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

GITHUB_REPOSITORY = os.getenv("GITHUB_REPOSITORY", "")
GITHUB_SHA = os.getenv("GITHUB_SHA", "")
GITHUB_REF = os.getenv("GITHUB_REF", "")

BENCH_GRAPH_SERVER_ENDPOINT = f"http://{os.getenv('BENCH_GRAPH_SERVER_ENDPOINT', 'bench-graph-api:9001')}"

log = logging.getLogger(__name__)


def parse_args():
    argp = ArgumentParser(description=__doc__)
    argp.add_argument("--benchmark-name", type=str, required=True)
    argp.add_argument("--benchmark-results", type=str, required=True)
    argp.add_argument("--benchmark-results-in-memory-analytical-path", type=str, required=False)
    argp.add_argument("--benchmark-results-on-disk-txn-path", type=str, required=False)
    argp.add_argument("--github-run-id", type=int, required=True)
    argp.add_argument("--github-run-number", type=int, required=True)
    argp.add_argument("--head-branch-name", type=str, required=True)
    return argp.parse_args()


def post_measurement(args):
    timestamp = datetime.now().timestamp()
    with open(args.benchmark_results, "r") as in_memory_txn_file:
        in_memory_txn_data = json.load(in_memory_txn_file)

    in_memory_analytical_data = None
    if args.benchmark_results_in_memory_analytical_path is not None:
        try:
            with open(args.benchmark_results_in_memory_analytical_path, "r") as in_memory_analytical_file:
                in_memory_analytical_data = json.load(in_memory_analytical_file)
        except IOError:
            log.error(f"Failed to load {args.benchmark_results_in_memory_analytical_path}.")

    on_disk_txn_data = None
    if args.benchmark_results_on_disk_txn_path is not None:
        try:
            with open(args.benchmark_results_on_disk_txn_path, "r") as on_disk_txn_file:
                on_disk_txn_data = json.load(on_disk_txn_file)
        except IOError:
            log.error(f"Failed to load {args.benchmark_results_on_disk_txn_path}.")

    req = requests.post(
        f"{BENCH_GRAPH_SERVER_ENDPOINT}/measurements",
        json={
            "name": args.benchmark_name,
            "timestamp": timestamp,
            "git_repo": GITHUB_REPOSITORY,
            "git_ref": GITHUB_REF,
            "git_sha": GITHUB_SHA,
            "github_run_id": args.github_run_id,
            "github_run_number": args.github_run_number,
            "results": in_memory_txn_data,
            "in_memory_analytical_results": in_memory_analytical_data,
            "on_disk_txn_results": on_disk_txn_data,
            "git_branch": args.head_branch_name,
        },
        timeout=1,
    )
    assert req.status_code == 200, f"Uploading {args.benchmark_name} data failed."
    log.info(f"{args.benchmark_name} data sent to " f"{BENCH_GRAPH_SERVER_ENDPOINT}")


if __name__ == "__main__":
    args = parse_args()
    logging.basicConfig(level=logging.INFO)
    post_measurement(args)
