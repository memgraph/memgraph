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

import atexit
import logging
import os
import subprocess
from argparse import ArgumentParser
from pathlib import Path

import yaml

import interactive_mg_runner

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", ".."))
BUILD_DIR = os.path.join(PROJECT_DIR, "build")

log = logging.getLogger("memgraph.tests.e2e")


def load_args():
    parser = ArgumentParser()
    parser.add_argument("--workloads-root-directory", required=True)
    parser.add_argument("--workload-name", default=None, required=False)
    return parser.parse_args()


def load_workloads(root_directory):
    workloads = []
    for file in Path(root_directory).rglob("*.yaml"):
        with open(file, "r") as f:
            workloads.extend(yaml.load(f, Loader=yaml.FullLoader)["workloads"])
    return workloads


def run(args):
    workloads = load_workloads(args.workloads_root_directory)
    for workload in workloads:
        workload_name = workload["name"]
        if args.workload_name is not None and args.workload_name != workload_name:
            continue
        log.info("%s STARTED.", workload_name)
        # Setup.
        @atexit.register
        def cleanup():
            interactive_mg_runner.stop_all()

        if "cluster" in workload:
            procdir = ""
            if "proc" in workload:
                procdir = os.path.join(BUILD_DIR, workload["proc"])
            interactive_mg_runner.start_all(workload["cluster"], procdir)

        # Test.
        mg_test_binary = os.path.join(BUILD_DIR, workload["binary"])
        subprocess.run([mg_test_binary] + workload["args"], check=True, stderr=subprocess.STDOUT)
        # Validation.
        if "cluster" in workload:
            for name, config in workload["cluster"].items():
                for validation in config.get("validation_queries", []):
                    mg_instance = interactive_mg_runner.MEMGRAPH_INSTANCES[name]
                    data = mg_instance.query(validation["query"])[0][0]
                    assert data == validation["expected"]
        cleanup()
        log.info("%s PASSED.", workload_name)


if __name__ == "__main__":
    args = load_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(asctime)s %(name)s] %(message)s")
    run(args)
