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
import json
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
PORT_NAMESPACE_BASE = 10000
DEFAULT_BOLT_PORT = 7687
DEFAULT_MONITORING_PORT = 7444
DEFAULT_METRICS_PORT = 9091
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
        "--metrics-port",
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


def _extract_ports_from_args(args):
    if not args:
        return set()

    offsettable_flags = {
        "--bolt-port",
        "--management-port",
        "--coordinator-port",
        "--replication-port",
        "--rpc-port",
        "--monitoring-port",
        "--metrics-port",
    }

    ports = set()
    for i, arg in enumerate(args):
        if arg in offsettable_flags and i + 1 < len(args) and str(args[i + 1]).isdigit():
            ports.add(int(args[i + 1]))
            continue

        for flag in offsettable_flags:
            prefix = f"{flag}="
            if isinstance(arg, str) and arg.startswith(prefix):
                value = arg[len(prefix) :]
                if value.isdigit():
                    ports.add(int(value))
                break
    return ports


def _extract_ports_from_query(query):
    if not isinstance(query, str):
        return set()

    ports = set()
    for match in PORT_AFTER_COLON_RE.finditer(query):
        ports.add(int(match.group(1)))
    for match in PORT_KEYWORD_RE.finditer(query):
        ports.add(int(match.group(2)))
    return ports


def _replace_port_flags_with_map(args, port_map):
    if not args:
        return args

    offsettable_flags = {
        "--bolt-port",
        "--management-port",
        "--coordinator-port",
        "--replication-port",
        "--rpc-port",
        "--monitoring-port",
        "--metrics-port",
    }

    updated = list(args)
    for i, arg in enumerate(updated):
        if arg in offsettable_flags and i + 1 < len(updated) and str(updated[i + 1]).isdigit():
            original = int(updated[i + 1])
            updated[i + 1] = str(port_map.get(original, original))
            continue

        for flag in offsettable_flags:
            prefix = f"{flag}="
            if isinstance(arg, str) and arg.startswith(prefix):
                value = arg[len(prefix) :]
                if value.isdigit():
                    original = int(value)
                    updated[i] = f"{prefix}{port_map.get(original, original)}"
                break
    return updated


def _offset_ports_in_query(query, port_map):
    if not isinstance(query, str) or not port_map:
        return query

    def replace_colon_port(match):
        original = int(match.group(1))
        return str(port_map.get(original, original))

    def replace_port_keyword(match):
        original = int(match.group(2))
        return f"{match.group(1)}{port_map.get(original, original)}"

    updated = PORT_AFTER_COLON_RE.sub(replace_colon_port, query)
    updated = PORT_KEYWORD_RE.sub(replace_port_keyword, updated)
    return updated


def _offset_query_collection(queries, port_map):
    if not isinstance(queries, list):
        return queries
    updated = []
    for query in queries:
        if isinstance(query, list):
            updated.append([_offset_ports_in_query(inner, port_map) for inner in query])
        else:
            updated.append(_offset_ports_in_query(query, port_map))
    return updated


def _has_port_flag(args, flag):
    if not args:
        return False
    for arg in args:
        if arg == flag:
            return True
        if isinstance(arg, str) and arg.startswith(f"{flag}="):
            return True
    return False


def _ensure_default_listener_ports(args):
    normalized = list(args or [])
    if not _has_port_flag(normalized, "--bolt-port"):
        normalized += ["--bolt-port", str(DEFAULT_BOLT_PORT)]
    if not _has_port_flag(normalized, "--monitoring-port"):
        normalized += ["--monitoring-port", str(DEFAULT_MONITORING_PORT)]
    if not _has_port_flag(normalized, "--metrics-port"):
        normalized += ["--metrics-port", str(DEFAULT_METRICS_PORT)]
    return normalized


def _extract_bolt_port_from_args(args):
    if not args:
        return DEFAULT_BOLT_PORT
    for i, arg in enumerate(args):
        if arg == "--bolt-port" and i + 1 < len(args) and str(args[i + 1]).isdigit():
            return int(args[i + 1])
        if isinstance(arg, str) and arg.startswith("--bolt-port="):
            value = arg.split("=", 1)[1]
            if value.isdigit():
                return int(value)
    return DEFAULT_BOLT_PORT


def _build_port_map(workload, worker_slot, port_offset_step):
    namespace_start = PORT_NAMESPACE_BASE + worker_slot * port_offset_step
    namespace_end = namespace_start + port_offset_step - 1
    if namespace_end > 65535:
        raise RuntimeError(
            f"Port namespace exhausted for worker {worker_slot}: "
            f"{namespace_start}-{namespace_end} exceeds 65535. "
            f"Use fewer workers or a smaller --port-offset-step."
        )

    discovered_ports = set()
    discovered_ports.update(_extract_ports_from_args(workload.get("args", [])))

    if "cluster" in workload:
        for config in workload["cluster"].values():
            discovered_ports.update(_extract_ports_from_args(config.get("args", [])))
            for setup_query in config.get("setup_queries", []):
                if isinstance(setup_query, list):
                    for nested_query in setup_query:
                        discovered_ports.update(_extract_ports_from_query(nested_query))
                else:
                    discovered_ports.update(_extract_ports_from_query(setup_query))
            for validation in config.get("validation_queries", []):
                if isinstance(validation, dict) and "query" in validation:
                    discovered_ports.update(_extract_ports_from_query(validation["query"]))

    sorted_ports = sorted(discovered_ports)
    if len(sorted_ports) > port_offset_step:
        raise RuntimeError(
            f"Worker {worker_slot} needs {len(sorted_ports)} unique ports, "
            f"but --port-offset-step is {port_offset_step}. Increase --port-offset-step."
        )

    port_map = {}
    for index, original in enumerate(sorted_ports):
        port_map[original] = namespace_start + index
    return port_map, namespace_start


def _prepare_workload_for_worker(workload, worker_slot, port_offset_step):
    prepared = copy.deepcopy(workload)
    if "cluster" in prepared:
        for _, config in prepared["cluster"].items():
            config["args"] = _ensure_default_listener_ports(config.get("args", []))

    port_map, namespace_start = _build_port_map(prepared, worker_slot, port_offset_step)
    suffix = f"-w{worker_slot}"

    if "cluster" in prepared:
        for _, config in prepared["cluster"].items():
            config["args"] = _replace_port_flags_with_map(config.get("args", []), port_map)
            if "setup_queries" in config:
                config["setup_queries"] = _offset_query_collection(config["setup_queries"], port_map)
            if "validation_queries" in config:
                for validation in config["validation_queries"]:
                    if isinstance(validation, dict) and "query" in validation:
                        validation["query"] = _offset_ports_in_query(validation["query"], port_map)

            if "log_file" in config and isinstance(config["log_file"], str):
                config["log_file"] = _append_suffix(config["log_file"], suffix)
            if "data_directory" in config and isinstance(config["data_directory"], str):
                config["data_directory"] = _append_suffix(config["data_directory"], suffix)

    prepared["args"] = _replace_port_flags_with_map(prepared.get("args", []), port_map)
    return prepared, namespace_start, port_map


def _extend_pythonpath(env, path):
    existing = env.get("PYTHONPATH", "")
    if not existing:
        env["PYTHONPATH"] = path
    else:
        env["PYTHONPATH"] = f"{path}{os.pathsep}{existing}"


def _run_and_capture(command, env):
    proc = subprocess.run(command, check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env)
    if proc.stdout:
        print(proc.stdout, end="" if proc.stdout.endswith("\n") else "\n")
    if proc.stderr:
        print(proc.stderr, end="" if proc.stderr.endswith("\n") else "\n", file=sys.stderr)
    if proc.returncode != 0:
        raise subprocess.CalledProcessError(proc.returncode, command, output=proc.stdout, stderr=proc.stderr)


def run_single_workload(workload, worker_slot, args_dict):
    workload_name = workload["name"]
    start_time = time.monotonic()
    output_stdout = io.StringIO()
    output_stderr = io.StringIO()
    args_save_data_dir = args_dict["save_data_dir"]
    args_debug = args_dict["debug"]
    args_gdb = args_dict["gdb"]
    args_gdb_port = args_dict["gdb_port"]

    # Keep logging visible in the captured stream for each isolated workload.
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(asctime)s %(name)s] %(message)s")

    env = os.environ.copy()
    prepared, port_namespace_start, port_map = _prepare_workload_for_worker(
        workload, worker_slot, args_dict["port_offset_step"]
    )
    env["MEMGRAPH_PARALLEL_PROCESS_INDEX"] = str(worker_slot)
    env["MEMGRAPH_PORT_OFFSET"] = str(port_namespace_start)
    env["MEMGRAPH_PORT_NAMESPACE_START"] = str(port_namespace_start)
    env["MEMGRAPH_E2E_PORT_MAP"] = json.dumps(port_map)
    _extend_pythonpath(env, SCRIPT_DIR)
    if "cluster" in prepared and prepared["cluster"]:
        first_instance_name, first_instance_config = next(iter(prepared["cluster"].items()))
        first_instance_bolt_port = _extract_bolt_port_from_args(first_instance_config.get("args", []))
        env["MEMGRAPH_BOLT_PORT"] = str(first_instance_bolt_port)
        env["MEMGRAPH_HOST"] = "127.0.0.1"
        env["MEMGRAPH_PORT"] = str(first_instance_bolt_port)
        env["MG_HOST"] = "127.0.0.1"
        env["MG_PORT"] = str(first_instance_bolt_port)
        env["MEMGRAPH_INSTANCE_NAME"] = first_instance_name

    gdb_port = None
    if args_gdb:
        gdb_port = 50000 + worker_slot
        if gdb_port > 65535:
            raise RuntimeError(f"Computed gdb port {gdb_port} exceeds 65535")

    try:
        with redirect_stdout(output_stdout), redirect_stderr(output_stderr):
            log.info(
                "%s STARTED (worker=%d, port_namespace_start=%d).",
                workload_name,
                worker_slot,
                port_namespace_start,
            )

            if "pre_set_workload" in prepared:
                binary = os.path.join(BUILD_DIR, prepared["pre_set_workload"])
                _run_and_capture([binary], env=env)

            if "cluster" in prepared:
                procdir = ""
                if "proc" in prepared:
                    procdir = os.path.join(BUILD_DIR, prepared["proc"])
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
        "port_offset": port_namespace_start,
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


def _terminate_pool(pool):
    processes = getattr(pool, "_processes", {})
    for process in processes.values():
        try:
            process.terminate()
        except Exception:
            pass


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
    stop_due_to_failure = False

    with ProcessPoolExecutor(max_workers=worker_count) as pool:
        for worker_slot in range(worker_count):
            workload = pending_workloads.pop(0)
            future = pool.submit(run_single_workload, workload, worker_slot, args_dict)
            futures[future] = worker_slot

        while futures:
            done, _ = wait(list(futures.keys()), return_when=FIRST_COMPLETED)
            for finished in done:
                worker_slot = futures.pop(finished)
                try:
                    result = finished.result()
                except Exception as exc:
                    result = {
                        "name": f"<worker {worker_slot}>",
                        "success": False,
                        "error": str(exc),
                        "elapsed": 0.0,
                        "worker_slot": worker_slot,
                        "port_offset": worker_slot * args.port_offset_step,
                        "stdout": "",
                        "stderr": traceback.format_exc(),
                    }
                _print_result(result)
                if not result["success"]:
                    failed.append(result)
                    stop_due_to_failure = True
                    pending_workloads = []
                    for future in list(futures.keys()):
                        future.cancel()
                    _terminate_pool(pool)
                    break
                if pending_workloads and not stop_due_to_failure:
                    workload = pending_workloads.pop(0)
                    future = pool.submit(run_single_workload, workload, worker_slot, args_dict)
                    futures[future] = worker_slot
            if stop_due_to_failure:
                break

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
