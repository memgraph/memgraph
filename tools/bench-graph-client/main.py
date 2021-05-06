#!/usr/bin/env python3

import json
import os
import requests
from datetime import datetime
from argparse import ArgumentParser

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

GITHUB_REPOSITORY = os.getenv("GITHUB_REPOSITORY", "")
GITHUB_SHA = os.getenv("GITHUB_SHA", "")
GITHUB_REF = os.getenv("GITHUB_REF", "")

BENCH_GRAPH_SERVER_ENDPOINT = os.getenv(
    "BENCH_GRAPH_SERVER_ENDPOINT",
    "http://mgdeps-cache:9000")
BENCH_GRAPH_BENCHMARKS = {
    "macro_benchmark": {
        "json_data_path": os.path.join(
            SCRIPT_DIR,
            "../../tests/macro_benchmark/.harness_summary")}}


def parse_args():
    argp = ArgumentParser(description=__doc__)
    argp.add_argument("--benchmark-name", type=str, required=True)
    argp.add_argument("--github-run-id", type=int, required=True)
    argp.add_argument("--github-run-number", type=int, required=True)
    return argp.parse_args()


def post_measurement(args):
    with open(BENCH_GRAPH_BENCHMARKS[args.benchmark_name]["json_data_path"],
              "r") as f:
        data = json.load(f)
        timestamp = datetime.now().timestamp()
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
                "results": data
            },
            timeout=1)
        assert req.status_code == 200, \
            f"Uploading {args.benchmark_name} data failed."


if __name__ == "__main__":
    args = parse_args()
    post_measurement(args)
