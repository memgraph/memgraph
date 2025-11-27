#!/usr/bin/env python3

# Copyright 2021 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import logging
import os
import shutil
import subprocess
import sys
import time
from argparse import ArgumentParser
from pathlib import Path

import interactive_mg_runner
import yaml

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", ".."))
BUILD_DIR = os.path.join(PROJECT_DIR, "build")

log = logging.getLogger("memgraph.tests.e2e")

DISABLE_NODE = os.getenv("DISABLE_NODE", "false") == "true"
if DISABLE_NODE:
    log.info("Skipping node setup because DISABLE_NODE is set to true")


def load_args():
    parser = ArgumentParser()
    parser.add_argument("--workloads-root-directory", required=True)
    parser.add_argument("--workload-name", default=None, required=False)
    parser.add_argument(
        "--workload-name-list",
        default=False,
        required=False,
        action="store_true",
        help="List all available workload names and exit",
    )
    parser.add_argument("--debug", default=False, required=False)
    parser.add_argument("--save-data-dir", default=False, required=False, action="store_true")
    parser.add_argument("--clean-logs-dir", default=False, required=False, action="store_true")
    return parser.parse_args()


def load_workloads(root_directory):
    workloads = []
    # Always search relative to the build directory
    build_e2e_dir = os.path.join(BUILD_DIR, "tests", "e2e")
    if root_directory == ".":
        search_path = Path(build_e2e_dir)
    else:
        search_path = Path(os.path.join(build_e2e_dir, root_directory))

    for file in search_path.rglob("workloads.yaml"):
        # 8.03.2024. - Skip streams e2e tests
        if str(file).endswith("/streams/workloads.yaml"):
            continue

        if str(file).endswith("/graphql/workloads.yaml") and DISABLE_NODE:
            continue
        with open(file, "r") as f:
            workloads.extend(yaml.load(f, Loader=yaml.FullLoader)["workloads"])
    return workloads


def list_workload_names(root_directory):
    """List all available workload names from the given root directory."""
    workloads = load_workloads(root_directory)
    workload_names = sorted(set(w["name"] for w in workloads))

    print("Available workload names:")
    print("-" * 30)
    for name in workload_names:
        print(f"  {name}")
    print("-" * 30)
    print(f"Total: {len(workload_names)} workloads")


def cleanup(workload, keep_directories=True):
    # If we use cluster keyword in workloads.yaml, we will stop directories and keep them based on args.save_data_dir
    # If we manually control instances using interactive_mg_runner in tests, then we specify our cleanup function
    if "cluster" in workload:
        interactive_mg_runner.stop_all(keep_directories)


def run(args):
    workloads = load_workloads(args.workloads_root_directory)
    for workload in workloads:
        workload_name = workload["name"]
        if args.workload_name is not None and args.workload_name != workload_name:
            continue
        log.info("%s STARTED.", workload_name)

        try:
            if "pre_set_workload" in workload:
                binary = os.path.join(BUILD_DIR, workload["pre_set_workload"])
                subprocess.run([binary], check=True, stderr=subprocess.STDOUT)

            if "cluster" in workload:
                procdir = ""
                if "proc" in workload:
                    procdir = os.path.join(BUILD_DIR, workload["proc"])
                interactive_mg_runner.start_all(workload["cluster"], procdir, keep_directories=False)

            if args.debug:
                hosts = subprocess.check_output("pgrep memgraph", shell=True)
                print(f"PID: {hosts}")
                time.sleep(10)

            # Test.
            mg_test_binary = os.path.join(BUILD_DIR, workload["binary"])
            subprocess.run([mg_test_binary] + workload["args"], check=True, stderr=subprocess.STDOUT)

            # Validation.
            if "cluster" in workload:
                for name, config in workload["cluster"].items():
                    mg_instance = interactive_mg_runner.MEMGRAPH_INSTANCES[name]
                    # Explicitely check if there are validation queries and skip if
                    # nothing is to validate. If setup queries are dealing with
                    # users, any new connection requires auth details.
                    validation_queries = config.get("validation_queries", [])
                    if not validation_queries:
                        continue

                    # NOTE: If the setup quries create users AND there are some
                    # validation queries, the connection here has to get the right
                    # username/password.
                    conn = mg_instance.get_connection()
                    for validation in validation_queries:
                        data = mg_instance.query(validation["query"], conn)[0][0]
                        assert (
                            data == validation["expected"]
                        ), f"Assertion failed: got {data}, expected {validation['expected']} from query `{validation['query']}`"
                    conn.close()

            log.info("%s PASSED.", workload_name)

        except Exception as e:
            cleanup(workload, keep_directories=args.save_data_dir)
            log.error("%s FAILED. %s", workload_name, e)
            raise e

        cleanup(workload, keep_directories=args.save_data_dir)


if __name__ == "__main__":
    args = load_args()

    # If --workload-name-list is specified, list workload names and exit
    if args.workload_name_list:
        list_workload_names(args.workloads_root_directory)
        sys.exit(0)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(asctime)s %(name)s] %(message)s")
    run(args)
    if not args.save_data_dir:
        try:
            shutil.rmtree(os.path.join(BUILD_DIR, "e2e", "data"))
        except Exception:
            pass
    if args.clean_logs_dir:
        try:
            shutil.rmtree(os.path.join(BUILD_DIR, "e2e", "logs"))
        except Exception:
            pass
