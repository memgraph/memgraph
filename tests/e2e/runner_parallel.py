#!/usr/bin/env python3

# Copyright 2026 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import copy
import io
import logging
import os
import re
import shutil
import subprocess
import sys
import time
import traceback
from argparse import ArgumentParser
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from contextlib import redirect_stderr, redirect_stdout
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

# Keep port ranges for each worker well-separated to avoid collisions.
DEFAULT_PORT_OFFSET_STEP = 1000
PORT_AFTER_COLON_RE = re.compile(r"(?<=:)(\d{2,5})(?=(?:['\"\s]|$))")
PORT_KEYWORD_RE = re.compile(r"(?i)(\bPORT\s+)(\d{2,5})")


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
    parser.add_argument(
        "--nprocesses",
        type=int,
        required=True,
        help="Number of parallel worker processes",
    )
    parser.add_argument(
        "--port-offset-step",
        type=int,
        required=False,
        default=DEFAULT_PORT_OFFSET_STEP,
        help="Per-worker port offset step (default: 1000)",
    )
    parser.add_argument(
        "--gdb",
        default=False,
        required=False,
        action="store_true",
        help="Start Memgraph under gdbserver for debugging",
    )
    parser.add_argument(
        "--gdb-port",
        default=1234,
        type=int,
        required=False,
        help="Port for gdbserver (default: 1234)",
    )
    return parser.parse_args()


def load_workloads(root_directory):
    workloads = []
    # Always search relative to the build directory.
    build_e2e_dir = os.path.join(BUILD_DIR, "tests", "e2e")
    if root_directory == ".":
        search_path = Path(build_e2e_dir)
    else:
        search_path = Path(os.path.join(build_e2e_dir, root_directory))

    for file in search_path.rglob("workloads.yaml"):
        # 8.03.2024. - Skip streams e2e tests.
        if str(file).endswith("/streams/workloads.yaml"):
            continue

        if str(file).endswith("/graphql/workloads.yaml") and DISABLE_NODE:
            continue
        with open(file, "r") as f:
            workloads.extend(yaml.load(f, Loader=yaml.FullLoader)["workloads"])
    return workloads


def list_workload_names(root_directory):
    workloads = load_workloads(root_directory)
    workload_names = sorted(set(w["name"] for w in workloads))

    print("Available workload names:")
    print("-" * 30)
    for name in workload_names:
        print(f"  {name}")
    print("-" * 30)
    print(f"Total: {len(workload_names)} workloads")


def cleanup(workload, keep_directories=True):
    # If we use cluster keyword in workloads.yaml, we will stop directories and keep them based on args.save_data_dir.
    # If we manually control instances using interactive_mg_runner in tests, then we specify our cleanup function.
    if "cluster" in workload:
        interactive_mg_runner.stop_all(keep_directories)


def _append_suffix(filename, suffix):
    stem, ext = os.path.splitext(filename)
    return f"{stem}{suffix}{ext}" if ext else f"{filename}{suffix}"


def _offset_port_flags(args, port_offset):
    if not args:
        return args

    offsettable_flags = {
        "--bolt-port",
        "--management-port",
        "--coordinator-port",
        "--replication-port",
        "--rpc-port",
        "--monitoring-port",
    }

    updated = list(args)
    for i, arg in enumerate(updated):
        if arg in offsettable_flags and i + 1 < len(updated) and str(updated[i + 1]).isdigit():
            updated[i + 1] = str(int(updated[i + 1]) + port_offset)
            continue

        for flag in offsettable_flags:
            prefix = f"{flag}="
            if isinstance(arg, str) and arg.startswith(prefix):
                value = arg[len(prefix) :]
                if value.isdigit():
                    updated[i] = f"{prefix}{int(value) + port_offset}"
                break
    return updated


def _offset_ports_in_query(query, port_offset):
    if not isinstance(query, str) or port_offset == 0:
        return query

    def replace_colon_port(match):
        value = int(match.group(1))
        return str(value + port_offset)

    def replace_port_keyword(match):
        value = int(match.group(2))
        return f"{match.group(1)}{value + port_offset}"

    updated = PORT_AFTER_COLON_RE.sub(replace_colon_port, query)
    updated = PORT_KEYWORD_RE.sub(replace_port_keyword, updated)
    return updated


def _offset_query_collection(queries, port_offset):
    if not isinstance(queries, list):
        return queries
    updated = []
    for query in queries:
        if isinstance(query, list):
            updated.append([_offset_ports_in_query(inner, port_offset) for inner in query])
        else:
            updated.append(_offset_ports_in_query(query, port_offset))
    return updated


def _prepare_workload_for_worker(workload, worker_slot, port_offset_step):
    prepared = copy.deepcopy(workload)
    port_offset = worker_slot * port_offset_step
    suffix = f"-w{worker_slot}"

    if "cluster" in prepared:
        for _, config in prepared["cluster"].items():
            config["args"] = _offset_port_flags(config.get("args", []), port_offset)
            if "setup_queries" in config:
                config["setup_queries"] = _offset_query_collection(config["setup_queries"], port_offset)
            if "validation_queries" in config:
                for validation in config["validation_queries"]:
                    if isinstance(validation, dict) and "query" in validation:
                        validation["query"] = _offset_ports_in_query(validation["query"], port_offset)

            if "log_file" in config and isinstance(config["log_file"], str):
                config["log_file"] = _append_suffix(config["log_file"], suffix)
            if "data_directory" in config and isinstance(config["data_directory"], str):
                config["data_directory"] = _append_suffix(config["data_directory"], suffix)

    prepared["args"] = _offset_port_flags(prepared.get("args", []), port_offset)
    return prepared


def _run_and_capture(command, env):
    proc = subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env)
    if proc.stdout:
        print(proc.stdout, end="" if proc.stdout.endswith("\n") else "\n")
    if proc.stderr:
        print(proc.stderr, end="" if proc.stderr.endswith("\n") else "\n", file=sys.stderr)


def run_single_workload(workload, worker_slot, args_dict):
    workload_name = workload["name"]
    start_time = time.monotonic()
    output_stdout = io.StringIO()
    output_stderr = io.StringIO()
    args_save_data_dir = args_dict["save_data_dir"]
    args_debug = args_dict["debug"]
    args_gdb = args_dict["gdb"]
    args_gdb_port = args_dict["gdb_port"]
    port_offset = worker_slot * args_dict["port_offset_step"]

    # Keep logging visible in the captured stream for each isolated workload.
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(asctime)s %(name)s] %(message)s")

    env = os.environ.copy()
    env["MEMGRAPH_PARALLEL_PROCESS_INDEX"] = str(worker_slot)
    env["MEMGRAPH_PORT_OFFSET"] = str(port_offset)

    prepared = _prepare_workload_for_worker(workload, worker_slot, args_dict["port_offset_step"])

    try:
        with redirect_stdout(output_stdout), redirect_stderr(output_stderr):
            log.info("%s STARTED (worker=%d, port_offset=%d).", workload_name, worker_slot, port_offset)

            if "pre_set_workload" in prepared:
                binary = os.path.join(BUILD_DIR, prepared["pre_set_workload"])
                _run_and_capture([binary], env=env)

            if "cluster" in prepared:
                procdir = ""
                if "proc" in prepared:
                    procdir = os.path.join(BUILD_DIR, prepared["proc"])
                gdb_port = (args_gdb_port + port_offset) if args_gdb else None
                interactive_mg_runner.start_all(prepared["cluster"], procdir, keep_directories=False, gdb_port=gdb_port)

            if args_debug:
                hosts = subprocess.check_output("pgrep memgraph", shell=True, env=env, text=True)
                print(f"PID: {hosts}")
                time.sleep(10)

            mg_test_binary = os.path.join(BUILD_DIR, prepared["binary"])
            _run_and_capture([mg_test_binary] + prepared["args"], env=env)

            if "cluster" in prepared:
                for name, config in prepared["cluster"].items():
                    mg_instance = interactive_mg_runner.MEMGRAPH_INSTANCES[name]
                    validation_queries = config.get("validation_queries", [])
                    if not validation_queries:
                        continue

                    conn = mg_instance.get_connection()
                    for validation in validation_queries:
                        data = mg_instance.query(validation["query"], conn)[0][0]
                        assert (
                            data == validation["expected"]
                        ), f"Assertion failed: got {data}, expected {validation['expected']} from query `{validation['query']}`"
                    conn.close()

            log.info("%s PASSED (worker=%d).", workload_name, worker_slot)
            success = True
            error = ""
    except Exception as e:
        with redirect_stderr(output_stderr):
            traceback.print_exc()
        cleanup(prepared, keep_directories=args_save_data_dir)
        success = False
        error = str(e)
    finally:
        cleanup(prepared, keep_directories=args_save_data_dir)

    elapsed = time.monotonic() - start_time
    return {
        "name": workload_name,
        "success": success,
        "error": error,
        "elapsed": elapsed,
        "worker_slot": worker_slot,
        "port_offset": port_offset,
        "stdout": output_stdout.getvalue(),
        "stderr": output_stderr.getvalue(),
    }


def _print_result(result):
    header = (
        f"\n===== [{result['name']}] worker={result['worker_slot']} "
        f"offset={result['port_offset']} elapsed={result['elapsed']:.2f}s "
        f"status={'PASSED' if result['success'] else 'FAILED'} ====="
    )
    print(header)
    if result["stdout"]:
        print(result["stdout"], end="" if result["stdout"].endswith("\n") else "\n")
    if result["stderr"]:
        print(result["stderr"], end="" if result["stderr"].endswith("\n") else "\n", file=sys.stderr)
    print("=" * len(header.lstrip("\n")))


def run(args):
    workloads = load_workloads(args.workloads_root_directory)
    if args.workload_name is not None:
        workloads = [w for w in workloads if w["name"] == args.workload_name]

    if not workloads:
        raise RuntimeError("No workloads selected to run.")

    if args.nprocesses < 1:
        raise ValueError("--nprocesses must be at least 1")
    if args.port_offset_step < 1:
        raise ValueError("--port-offset-step must be at least 1")

    worker_count = min(args.nprocesses, len(workloads))
    args_dict = {
        "save_data_dir": args.save_data_dir,
        "debug": args.debug,
        "gdb": args.gdb,
        "gdb_port": args.gdb_port,
        "port_offset_step": args.port_offset_step,
    }

    pending_workloads = list(workloads)
    failed = []
    futures = {}

    with ProcessPoolExecutor(max_workers=worker_count) as pool:
        for worker_slot in range(worker_count):
            workload = pending_workloads.pop(0)
            future = pool.submit(run_single_workload, workload, worker_slot, args_dict)
            futures[future] = worker_slot

        while futures:
            done, _ = wait(list(futures.keys()), return_when=FIRST_COMPLETED)
            for finished in done:
                worker_slot = futures.pop(finished)
                result = finished.result()
                _print_result(result)
                if not result["success"]:
                    failed.append(result)
                if pending_workloads:
                    workload = pending_workloads.pop(0)
                    future = pool.submit(run_single_workload, workload, worker_slot, args_dict)
                    futures[future] = worker_slot

    if failed:
        failed_names = ", ".join(result["name"] for result in failed)
        raise RuntimeError(f"{len(failed)} workload(s) failed: {failed_names}")


if __name__ == "__main__":
    args = load_args()

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
