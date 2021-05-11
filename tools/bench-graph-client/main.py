#!/usr/bin/env python3

"""
Bench Graph client responsible for sending benchmarking data in JSON format to
the Bench Graph server.
"""

import json
import logging
import os
import requests
import subprocess
from datetime import datetime
from argparse import ArgumentParser

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

GITHUB_REPOSITORY = os.getenv("GITHUB_REPOSITORY", "")
GITHUB_SHA = os.getenv("GITHUB_SHA", "")
GITHUB_REF = os.getenv("GITHUB_REF", "")

BENCH_GRAPH_SERVER_ENDPOINT = os.getenv(
    "BENCH_GRAPH_SERVER_ENDPOINT",
    "http://bench-graph-api:9001")

log = logging.getLogger(__name__)


def parse_args():
    argp = ArgumentParser(description=__doc__)
    argp.add_argument("--benchmark-name", type=str, required=True)
    argp.add_argument("--benchmark-results-path", type=str, required=True)
    argp.add_argument("--github-run-id", type=int, required=True)
    argp.add_argument("--github-run-number", type=int, required=True)
    return argp.parse_args()


def post_measurement(args):
    with open(args.benchmark_results_path, "r") as f:
        data = json.load(f)
        timestamp = datetime.now().timestamp()
        branch = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            stdout=subprocess.PIPE,
            check=True).stdout.decode("utf-8").strip()
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
                "results": data,
                "git_branch": branch},
            timeout=1)
        assert req.status_code == 200, \
            f"Uploading {args.benchmark_name} data failed."
        log.info(f"{args.benchmark_name} data sent to "
                 f"{BENCH_GRAPH_SERVER_ENDPOINT}")


if __name__ == "__main__":
    args = parse_args()
    logging.basicConfig(level=logging.INFO)
    post_measurement(args)
