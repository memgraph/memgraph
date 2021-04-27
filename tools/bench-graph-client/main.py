import json
import os
import requests
from datetime import datetime
from argparse import ArgumentParser

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
MACRO_BENCH_SUMMARY_PATH = os.path.join(
    SCRIPT_DIR, "../../tests/macro_benchmark/.harness_summary")
GITHUB_REPOSITORY = os.getenv("GITHUB_REPOSITORY", "")
GITHUB_SHA = os.getenv("GITHUB_SHA", "")
GITHUB_REF = os.getenv("GITHUB_REF", "")


def parse_args():
    argp = ArgumentParser(description=__doc__)
    argp.add_argument("--github-run-id", type=int, required=True)
    argp.add_argument("--github-run-number", type=int, required=True)
    return argp.parse_args()


if __name__ == "__main__":
    args = parse_args()

    with open(MACRO_BENCH_SUMMARY_PATH, "r") as f:
        data = json.load(f)
        timestamp = datetime.now().timestamp()
        for result in data["results"]:
            req = requests.post(
                "http://mgdeps-cache:9000/measurements",
                json={
                    "name": result["group_name"] + result["scenario_name"],
                    "timestamp": timestamp,
                    "git_repo": GITHUB_REPOSITORY,
                    "git_ref": GITHUB_REF,
                    "git_sha": GITHUB_SHA,
                    "github_run_id": args.github_run_id,
                    "github_run_number": args.github_run_number,
                    "results": {
                        "wall_median": result["wall_time"]["median"]}},
                timeout=1)
            print(req.status_code)
