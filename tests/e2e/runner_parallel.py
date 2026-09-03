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

"""
Runs e2e workloads in parallel.

Workloads are split into two lanes:
  * parallel lane: workloads whose whole cluster is declared in workloads.yaml. Their ports are remapped into a
    per-worker window and their data directories / log files get a per-worker suffix. Test processes see the mapping
    through MEMGRAPH_E2E_PORT_MAP, which sitecustomize.py uses to redirect local connections. Workloads from the same
    workloads.yaml never run at the same time, so shared files (query module dirs, csv fixtures...) cannot collide.
  * exclusive lane: workloads that start their own Memgraph instances from the test (interactive_mg_runner), spawn
    helper processes on fixed ports, or assert on endpoint strings. Those keep their hardcoded ports and data
    directories, so they run one at a time, unmodified, exactly like runner.py would run them.
"""

import copy
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import traceback
from argparse import ArgumentParser
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path

import interactive_mg_runner
import yaml

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", ".."))
BUILD_DIR = os.path.join(PROJECT_DIR, "build")
BUILD_E2E_DIR = os.path.join(BUILD_DIR, "tests", "e2e")

log = logging.getLogger("memgraph.tests.e2e")

DISABLE_NODE = os.getenv("DISABLE_NODE", "false") == "true"

# Remapped ports live in [PORT_NAMESPACE_BASE, ephemeral range start). Hardcoded test ports (7687.., 10001.., 13011..)
# all sit below the base, so the exclusive lane never collides with the parallel lane.
PORT_NAMESPACE_BASE = 20000
DEFAULT_PORT_OFFSET_STEP = 100
DEFAULT_BOLT_PORT = 7687
DEFAULT_MONITORING_PORT = 7444
DEFAULT_METRICS_PORT = 9091
PORT_AFTER_COLON_RE = re.compile(r"(?<=:)(\d{2,5})(?=(?:['\"\s,]|$))")
PORT_KEYWORD_RE = re.compile(r"(?i)(\bPORT\s+)(\d{2,5})")

OFFSETTABLE_FLAGS = {
    "--bolt-port",
    "--bolt_port",
    "--management-port",
    "--management_port",
    "--coordinator-port",
    "--coordinator_port",
    "--replication-port",
    "--replication_port",
    "--rpc-port",
    "--rpc_port",
    "--monitoring-port",
    "--monitoring_port",
    "--metrics-port",
    "--metrics_port",
}

# Test directories whose helpers use fixed ports outside of Memgraph (graphql starts a node server on :4000 that
# connects to bolt://localhost:7687 from JS).
EXCLUSIVE_TEST_DIRS = {"graphql"}
# Tests that compare SHOW REPLICAS / SHOW CONFIG output against hardcoded ports, which remapping would break.
EXCLUSIVE_TEST_FILES = {
    "configuration/configuration_check.py",
    "replication/show.py",
    "replication/edge_delete.py",
    "replication/replicate_periodic_commit.py",
}
# A test that manages instances itself, or shells out, cannot have its ports remapped from the outside.
EXCLUSIVE_SOURCE_RE = re.compile(r"\binteractive_mg_runner\b|\bsubprocess\b")


@dataclass
class Entry:
    workload: dict
    group: str  # workloads.yaml the workload came from
    exclusive: bool


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
        help="Number of parallel worker processes (one of them is reserved for the exclusive lane)",
    )
    parser.add_argument(
        "--port-offset-step",
        type=int,
        required=False,
        default=DEFAULT_PORT_OFFSET_STEP,
        help=f"Number of ports reserved per parallel worker (default: {DEFAULT_PORT_OFFSET_STEP})",
    )
    parser.add_argument(
        "--keep-going",
        default=False,
        required=False,
        action="store_true",
        help="Run all workloads even after a failure (default: stop scheduling new workloads on first failure)",
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


def _read_test_sources(workload):
    """Returns the source of a pytest workload's test file and the shared helpers next to it."""
    if not workload["binary"].endswith("pytest_runner.sh") or not workload.get("args"):
        return ""
    test_rel = workload["args"][0]
    sources = []
    for base in (BUILD_E2E_DIR, SCRIPT_DIR):
        test_path = os.path.join(base, test_rel)
        if not os.path.exists(test_path):
            continue
        test_dir = os.path.dirname(test_path)
        candidates = [test_path] + [os.path.join(test_dir, name) for name in ("conftest.py", "common.py")]
        for path in candidates:
            if os.path.exists(path):
                with open(path, "r", errors="replace") as f:
                    sources.append(f.read())
        break
    return "\n".join(sources)


def is_exclusive(workload):
    if "cluster" not in workload:
        return True
    if not workload["binary"].endswith("pytest_runner.sh"):
        return False
    test_rel = workload["args"][0] if workload.get("args") else ""
    if test_rel in EXCLUSIVE_TEST_FILES or test_rel.split("/")[0] in EXCLUSIVE_TEST_DIRS:
        return True
    return bool(EXCLUSIVE_SOURCE_RE.search(_read_test_sources(workload)))


def load_entries(root_directory):
    entries = []
    if root_directory == ".":
        search_path = Path(BUILD_E2E_DIR)
    else:
        search_path = Path(os.path.join(BUILD_E2E_DIR, root_directory))

    for file in sorted(search_path.rglob("workloads.yaml")):
        # 8.03.2024. - Skip streams e2e tests.
        if str(file).endswith("/streams/workloads.yaml"):
            continue
        if str(file).endswith("/graphql/workloads.yaml") and DISABLE_NODE:
            continue
        group = os.path.relpath(str(file), BUILD_E2E_DIR)
        with open(file, "r") as f:
            for workload in yaml.load(f, Loader=yaml.FullLoader)["workloads"]:
                entries.append(Entry(workload=workload, group=group, exclusive=is_exclusive(workload)))
    return entries


def list_workload_names(root_directory):
    entries = load_entries(root_directory)
    print("Available workload names:")
    print("-" * 30)
    for name in sorted(set(e.workload["name"] for e in entries)):
        print(f"  {name}")
    print("-" * 30)
    print(f"Total: {len(entries)} workloads")


def cleanup(workload, keep_directories=True):
    # If we use cluster keyword in workloads.yaml, we will stop directories and keep them based on args.save_data_dir.
    # If we manually control instances using interactive_mg_runner in tests, then we specify our cleanup function.
    if "cluster" in workload:
        interactive_mg_runner.stop_all(keep_directories)


def _append_suffix(filename, suffix):
    stem, ext = os.path.splitext(filename)
    return f"{stem}{suffix}{ext}" if ext else f"{filename}{suffix}"


def _ephemeral_port_range_start():
    try:
        with open("/proc/sys/net/ipv4/ip_local_port_range") as f:
            return int(f.read().split()[0])
    except Exception:
        return 32768


def _extract_ports_from_args(args):
    ports = set()
    for i, arg in enumerate(args or []):
        if arg in OFFSETTABLE_FLAGS and i + 1 < len(args) and str(args[i + 1]).isdigit():
            ports.add(int(args[i + 1]))
            continue
        if not isinstance(arg, str):
            continue
        for flag in OFFSETTABLE_FLAGS:
            prefix = f"{flag}="
            if arg.startswith(prefix) and arg[len(prefix) :].isdigit():
                ports.add(int(arg[len(prefix) :]))
                break
        else:
            # e.g. --database_endpoints=127.0.0.1:7687,127.0.0.1:7688
            ports.update(_extract_ports_from_query(arg))
    return ports


def _extract_ports_from_query(query):
    if not isinstance(query, str):
        return set()
    ports = set(int(m.group(1)) for m in PORT_AFTER_COLON_RE.finditer(query))
    ports.update(int(m.group(2)) for m in PORT_KEYWORD_RE.finditer(query))
    return ports


def _remap_ports_in_query(query, port_map):
    if not isinstance(query, str) or not port_map:
        return query

    def replace_colon_port(match):
        original = int(match.group(1))
        return str(port_map.get(original, original))

    def replace_port_keyword(match):
        original = int(match.group(2))
        return f"{match.group(1)}{port_map.get(original, original)}"

    updated = PORT_AFTER_COLON_RE.sub(replace_colon_port, query)
    return PORT_KEYWORD_RE.sub(replace_port_keyword, updated)


def _remap_ports_in_args(args, port_map):
    updated = list(args or [])
    for i, arg in enumerate(updated):
        if arg in OFFSETTABLE_FLAGS and i + 1 < len(updated) and str(updated[i + 1]).isdigit():
            original = int(updated[i + 1])
            updated[i + 1] = str(port_map.get(original, original))
            continue
        if not isinstance(arg, str):
            continue
        for flag in OFFSETTABLE_FLAGS:
            prefix = f"{flag}="
            if arg.startswith(prefix) and arg[len(prefix) :].isdigit():
                original = int(arg[len(prefix) :])
                updated[i] = f"{prefix}{port_map.get(original, original)}"
                break
        else:
            updated[i] = _remap_ports_in_query(arg, port_map)
    return updated


def _remap_query_collection(queries, port_map):
    if not isinstance(queries, list):
        return queries
    updated = []
    for query in queries:
        if isinstance(query, list):
            updated.append([_remap_ports_in_query(inner, port_map) for inner in query])
        else:
            updated.append(_remap_ports_in_query(query, port_map))
    return updated


def _has_port_flag(args, *flags):
    for arg in args or []:
        for flag in flags:
            if arg == flag or (isinstance(arg, str) and arg.startswith(f"{flag}=")):
                return True
    return False


def _ensure_default_listener_ports(args):
    """Memgraph listens on these even when not asked to, so they need remapping too."""
    normalized = list(args or [])
    if not _has_port_flag(normalized, "--bolt-port", "--bolt_port"):
        normalized += ["--bolt-port", str(DEFAULT_BOLT_PORT)]
    if not _has_port_flag(normalized, "--monitoring-port", "--monitoring_port"):
        normalized += ["--monitoring-port", str(DEFAULT_MONITORING_PORT)]
    if not _has_port_flag(normalized, "--metrics-port", "--metrics_port"):
        normalized += ["--metrics-port", str(DEFAULT_METRICS_PORT)]
    return normalized


def _extract_bolt_port_from_args(args):
    for i, arg in enumerate(args or []):
        if arg in ("--bolt-port", "--bolt_port") and i + 1 < len(args) and str(args[i + 1]).isdigit():
            return int(args[i + 1])
        if isinstance(arg, str) and (arg.startswith("--bolt-port=") or arg.startswith("--bolt_port=")):
            value = arg.split("=", 1)[1]
            if value.isdigit():
                return int(value)
    return DEFAULT_BOLT_PORT


def _build_port_map(workload, worker_slot, port_offset_step):
    namespace_start = PORT_NAMESPACE_BASE + worker_slot * port_offset_step
    namespace_end = namespace_start + port_offset_step - 1
    ephemeral_start = _ephemeral_port_range_start()
    if namespace_end >= ephemeral_start:
        raise RuntimeError(
            f"Port namespace exhausted for worker {worker_slot}: {namespace_start}-{namespace_end} reaches the "
            f"ephemeral port range starting at {ephemeral_start}. Use fewer workers or a smaller --port-offset-step."
        )

    discovered_ports = set(_extract_ports_from_args(workload.get("args", [])))
    for config in workload["cluster"].values():
        discovered_ports.update(_extract_ports_from_args(config.get("args", [])))
        for setup_query in config.get("setup_queries", []):
            for query in setup_query if isinstance(setup_query, list) else [setup_query]:
                discovered_ports.update(_extract_ports_from_query(query))
        for validation in config.get("validation_queries", []):
            if isinstance(validation, dict) and "query" in validation:
                discovered_ports.update(_extract_ports_from_query(validation["query"]))

    sorted_ports = sorted(discovered_ports)
    if len(sorted_ports) > port_offset_step:
        raise RuntimeError(
            f"Worker {worker_slot} needs {len(sorted_ports)} unique ports, "
            f"but --port-offset-step is {port_offset_step}. Increase --port-offset-step."
        )

    port_map = {original: namespace_start + index for index, original in enumerate(sorted_ports)}
    return port_map, namespace_start


def prepare_workload_for_worker(workload, worker_slot, port_offset_step):
    prepared = copy.deepcopy(workload)
    for config in prepared["cluster"].values():
        config["args"] = _ensure_default_listener_ports(config.get("args", []))

    port_map, namespace_start = _build_port_map(prepared, worker_slot, port_offset_step)
    suffix = f"-w{worker_slot}"

    for config in prepared["cluster"].values():
        config["args"] = _remap_ports_in_args(config.get("args", []), port_map)
        if "setup_queries" in config:
            config["setup_queries"] = _remap_query_collection(config["setup_queries"], port_map)
        for validation in config.get("validation_queries", []) or []:
            if isinstance(validation, dict) and "query" in validation:
                validation["query"] = _remap_ports_in_query(validation["query"], port_map)
        if isinstance(config.get("log_file"), str):
            config["log_file"] = _append_suffix(config["log_file"], suffix)
        if isinstance(config.get("data_directory"), str):
            config["data_directory"] = _append_suffix(config["data_directory"], suffix)

    prepared["args"] = _remap_ports_in_args(prepared.get("args", []), port_map)
    # C++ e2e binaries default to --bolt-port 7687 when the workload passes no port at all.
    is_cpp_binary = not prepared["binary"].endswith(".sh")
    if is_cpp_binary and not _extract_ports_from_args(workload.get("args", [])) and DEFAULT_BOLT_PORT in port_map:
        prepared["args"] = prepared["args"] + ["--bolt-port", str(port_map[DEFAULT_BOLT_PORT])]
    return prepared, namespace_start, port_map


def _extend_pythonpath(env, path):
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = path if not existing else f"{path}{os.pathsep}{existing}"


class _CaptureFds:
    """Redirects fd 1 and 2 (so also the output of every child process) into a temporary file."""

    def __enter__(self):
        self.file = tempfile.TemporaryFile(mode="w+b")
        sys.stdout.flush()
        sys.stderr.flush()
        self.saved = [os.dup(1), os.dup(2)]
        os.dup2(self.file.fileno(), 1)
        os.dup2(self.file.fileno(), 2)
        return self

    def __exit__(self, *exc):
        sys.stdout.flush()
        sys.stderr.flush()
        os.dup2(self.saved[0], 1)
        os.dup2(self.saved[1], 2)
        for fd in self.saved:
            os.close(fd)
        self.file.seek(0)
        self.output = self.file.read().decode(errors="replace")
        self.file.close()
        return False


def run_single_workload(workload, worker_slot, exclusive, args_dict):
    workload_name = workload["name"]
    start_time = time.monotonic()
    args_save_data_dir = args_dict["save_data_dir"]

    env = os.environ.copy()
    _extend_pythonpath(env, SCRIPT_DIR)
    port_namespace_start = None
    if exclusive:
        prepared = copy.deepcopy(workload)
    else:
        prepared, port_namespace_start, port_map = prepare_workload_for_worker(
            workload, worker_slot, args_dict["port_offset_step"]
        )
        env["MEMGRAPH_PARALLEL_PROCESS_INDEX"] = str(worker_slot)
        env["MEMGRAPH_PORT_NAMESPACE_START"] = str(port_namespace_start)
        env["MEMGRAPH_E2E_PORT_MAP"] = json.dumps(port_map)
        first_instance_config = next(iter(prepared["cluster"].values()))
        env["MEMGRAPH_BOLT_PORT"] = str(_extract_bolt_port_from_args(first_instance_config.get("args", [])))

    gdb_port = None
    if args_dict["gdb"]:
        gdb_port = args_dict["gdb_port"] if exclusive else 50000 + worker_slot

    success = False
    error = ""
    with _CaptureFds() as capture:
        # The handler created by the parent's basicConfig points at the original stderr, rebind it to the capture.
        logging.basicConfig(
            level=logging.INFO, format="%(levelname)s %(asctime)s %(name)s] %(message)s", stream=sys.stderr, force=True
        )
        try:
            lane = "exclusive" if exclusive else f"parallel, ports from {port_namespace_start}"
            log.info("%s STARTED (worker=%d, %s).", workload_name, worker_slot, lane)

            if "pre_set_workload" in prepared:
                subprocess.run([os.path.join(BUILD_DIR, prepared["pre_set_workload"])], check=True, env=env)

            if "cluster" in prepared:
                procdir = os.path.join(BUILD_DIR, prepared["proc"]) if "proc" in prepared else ""
                interactive_mg_runner.start_all(prepared["cluster"], procdir, keep_directories=False, gdb_port=gdb_port)

            if args_dict["debug"]:
                hosts = subprocess.check_output("pgrep memgraph", shell=True, text=True)
                print(f"PID: {hosts}")
                time.sleep(10)

            mg_test_binary = os.path.join(BUILD_DIR, prepared["binary"])
            subprocess.run([mg_test_binary] + prepared["args"], check=True, env=env)

            if "cluster" in prepared:
                for name, config in prepared["cluster"].items():
                    validation_queries = config.get("validation_queries", [])
                    if not validation_queries:
                        continue
                    mg_instance = interactive_mg_runner.MEMGRAPH_INSTANCES[name]
                    conn = mg_instance.get_connection()
                    for validation in validation_queries:
                        data = mg_instance.query(validation["query"], conn)[0][0]
                        assert (
                            data == validation["expected"]
                        ), f"Assertion failed: got {data}, expected {validation['expected']} from query `{validation['query']}`"
                    conn.close()

            log.info("%s PASSED (worker=%d).", workload_name, worker_slot)
            success = True
        except Exception as e:
            traceback.print_exc()
            error = str(e)
            log.error("%s FAILED. %s", workload_name, e)
        finally:
            try:
                cleanup(prepared, keep_directories=args_save_data_dir)
            except Exception:
                traceback.print_exc()

    return {
        "name": workload_name,
        "success": success,
        "error": error,
        "elapsed": time.monotonic() - start_time,
        "worker_slot": worker_slot,
        "exclusive": exclusive,
        "output": capture.output,
    }


def _print_result(result):
    lane = "exclusive" if result["exclusive"] else "parallel"
    header = (
        f"===== [{result['name']}] worker={result['worker_slot']} lane={lane} "
        f"elapsed={result['elapsed']:.2f}s status={'PASSED' if result['success'] else 'FAILED'} ====="
    )
    print("\n" + header)
    if result["output"]:
        print(result["output"], end="" if result["output"].endswith("\n") else "\n")
    print("=" * len(header), flush=True)


def _worker_error_result(worker_slot, exclusive, entry, exc):
    return {
        "name": entry.workload["name"],
        "success": False,
        "error": str(exc),
        "elapsed": 0.0,
        "worker_slot": worker_slot,
        "exclusive": exclusive,
        "output": traceback.format_exc(),
    }


def run(args):
    entries = load_entries(args.workloads_root_directory)
    if args.workload_name is not None:
        entries = [e for e in entries if e.workload["name"] == args.workload_name]
    if not entries:
        raise RuntimeError("No workloads selected to run.")
    if args.nprocesses < 1:
        raise ValueError("--nprocesses must be at least 1")
    if args.port_offset_step < 1:
        raise ValueError("--port-offset-step must be at least 1")

    if args.nprocesses == 1:
        # Nothing to gain from remapping, behave like runner.py.
        for entry in entries:
            entry.exclusive = True

    exclusive_queue = [e for e in entries if e.exclusive]
    parallel_queue = [e for e in entries if not e.exclusive]
    parallel_slots = min(args.nprocesses - (1 if exclusive_queue else 0), len(parallel_queue))
    worker_count = (1 if exclusive_queue else 0) + parallel_slots
    log.info(
        "Running %d workloads: %d exclusive (serial lane), %d parallel across %d worker(s).",
        len(entries),
        len(exclusive_queue),
        len(parallel_queue),
        parallel_slots,
    )

    args_dict = {
        "save_data_dir": args.save_data_dir,
        "debug": args.debug,
        "gdb": args.gdb,
        "gdb_port": args.gdb_port,
        "port_offset_step": args.port_offset_step,
    }

    results = []
    futures = {}  # future -> (slot, entry)
    running_groups = set()
    # Slot 0 is the exclusive lane, slots 1.. are parallel lanes (each owns a port window).
    free_parallel_slots = list(range(1, parallel_slots + 1))
    exclusive_busy = False
    stop_scheduling = False
    start_time = time.monotonic()

    def pop_schedulable(queue):
        for index, entry in enumerate(queue):
            if entry.group not in running_groups:
                return queue.pop(index)
        return None

    with ProcessPoolExecutor(max_workers=max(worker_count, 1)) as pool:

        def schedule():
            nonlocal exclusive_busy
            if stop_scheduling:
                return
            while free_parallel_slots and parallel_queue:
                entry = pop_schedulable(parallel_queue)
                if entry is None:
                    break
                slot = free_parallel_slots.pop(0)
                running_groups.add(entry.group)
                futures[pool.submit(run_single_workload, entry.workload, slot, False, args_dict)] = (slot, entry)
            # Exclusive workloads keep their fixed ports and own data directories, so they don't take part in group
            # serialization; that only guards parallel workloads sharing files from the same workloads.yaml.
            if exclusive_queue and not exclusive_busy:
                entry = exclusive_queue.pop(0)
                exclusive_busy = True
                futures[pool.submit(run_single_workload, entry.workload, 0, True, args_dict)] = (0, entry)

        schedule()
        while futures:
            done, _ = wait(list(futures.keys()), return_when=FIRST_COMPLETED)
            for finished in done:
                slot, entry = futures.pop(finished)
                if slot == 0 and entry.exclusive:
                    exclusive_busy = False
                else:
                    running_groups.discard(entry.group)
                    free_parallel_slots.append(slot)
                try:
                    result = finished.result()
                except Exception as exc:
                    result = _worker_error_result(slot, entry.exclusive, entry, exc)
                _print_result(result)
                results.append(result)
                if not result["success"] and not args.keep_going:
                    stop_scheduling = True
            schedule()

    failed = [r for r in results if not r["success"]]
    skipped = len(exclusive_queue) + len(parallel_queue)
    print("\n===== SUMMARY =====")
    for result in results:
        print(f"{'PASSED' if result['success'] else 'FAILED':6s} {result['elapsed']:8.2f}s  {result['name']}")
    print(
        f"total={len(results)} passed={len(results) - len(failed)} failed={len(failed)} skipped={skipped} "
        f"wall_time={time.monotonic() - start_time:.2f}s"
    )
    if failed:
        failed_names = ", ".join(result["name"] for result in failed)
        raise RuntimeError(f"{len(failed)} workload(s) failed: {failed_names}")


if __name__ == "__main__":
    args = load_args()

    if args.workload_name_list:
        list_workload_names(args.workloads_root_directory)
        sys.exit(0)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(asctime)s %(name)s] %(message)s")
    if not args.save_data_dir:
        # Data left behind by an interrupted run makes instances recover stale state (e.g. a persisted replica role).
        shutil.rmtree(os.path.join(BUILD_DIR, "e2e", "data"), ignore_errors=True)
    try:
        run(args)
    except RuntimeError as e:
        # Data directories are kept on failure for inspection, the next run wipes them.
        log.error("%s", e)
        sys.exit(1)
    if not args.save_data_dir:
        shutil.rmtree(os.path.join(BUILD_DIR, "e2e", "data"), ignore_errors=True)
    if args.clean_logs_dir:
        shutil.rmtree(os.path.join(BUILD_DIR, "e2e", "logs"), ignore_errors=True)
