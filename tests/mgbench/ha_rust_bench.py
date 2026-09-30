#!/usr/bin/env python3
# Copyright 2024 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL.txt; by using this file, you agree to be bound by the
# terms of the Business Source License, and you may not use this file except in
# compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with the Business
# Source License, use of this software will be governed by the Apache License,
# Version 2.0, included in the file licenses/APL.txt.

"""Run rustlg against a CPU-pinned 2-replica Memgraph HA cluster; emit bench-graph JSON.

Required env: MG_REAL_BINARY — absolute path to the memgraph binary.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import platform
import re
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

# Ensure the mgbench directory is on sys.path so local modules resolve.
_MGBENCH_DIR = Path(__file__).resolve().parent
if str(_MGBENCH_DIR) not in sys.path:
    sys.path.insert(0, str(_MGBENCH_DIR))

# helpers MUST be imported before runners/BenchmarkContext to initialise the
# workloads package and avoid a circular import.
import helpers  # noqa: F401 — side-effect import
import log
import runners
from benchmark_context import BenchmarkContext
from constants import (
    COUNT,
    DATABASE,
    DURATION,
    NUM_WORKERS,
    RETRIES,
    THROUGHPUT,
    WITHOUT_FINE_GRAINED_AUTHORIZATION,
    BenchmarkInstallationType,
)

logger = logging.getLogger(__name__)

_SCRIPT_DIR = Path(__file__).resolve().parent
_PIN_WRAPPER = _SCRIPT_DIR / "ha_pin_wrapper.sh"
_PIN_MAP_SH = _SCRIPT_DIR / "ha_pin_map.sh"
_RUSTLG = _SCRIPT_DIR / "rust_loadgen" / "target" / "release" / "rustlg"
_CLUSTER_YAML = "ha_cluster_2_replicas_pinned.yaml"

_HA_PORTS = (7687, 7688, 7689, 7690, 7691, 7692)

_BATCH_SIZE = 10_000
_NUM_BATCHES = 50
_EXPECTED_NODES = _BATCH_SIZE * _NUM_BATCHES

# Replica to poll for sync completion (port 7688 = instance_2 in the yaml)
_REPLICA_PORT = 7688

# Retry bounds for transient "cannot get read-only access" errors during writes
_WRITE_MAX_RETRIES = 60
_WRITE_RETRY_DELAY_S = 2.0

# local probe showed batch≈5000 makes the commit ~2× the heavy read, i.e. commit-dominated
_SLOW_WRITE_BATCH = 5000

# Fixed writer-thread count for split arms: 2 writers saturate the write path while the
# remaining (args.threads - 2) threads all drive reads against the two replicas.
_SPLIT_WRITERS = 2

_WIPE_BATCH_LIMIT = 50_000
_WIPE_MAX_ITERS = 200  # safety cap: 200 × 50 K = 10 M-node ceiling

# Benchmark arms: (label, rustlg_mode, write_batch, nthreads).
# nthreads=None means "use args.threads".
_BENCH_ARMS = [
    ("point", "point", 1, None),
    ("heavy", "heavy", 1, None),
    ("splitslowmix", "split", _SLOW_WRITE_BATCH, None),
]

_REPLICA_SYNC_TIMEOUT_S = 300
_REPLICA_SYNC_POLL_S = 2.0


def _compute_pin_map() -> tuple[str, str, str, str]:
    """Run ha_pin_map.sh and return (MG_PIN_MODE, MG_PIN_MAP, CLIENT_CPUS, CLIENT_NODE).
    MG_PIN_MODE defaults to "cpu" (back-compat); CLIENT_NODE is "" unless numa mode.
    """
    env = dict(os.environ)
    result = subprocess.run(
        [str(_PIN_MAP_SH)],
        capture_output=True,
        text=True,
        check=True,
        env=env,
    )
    output = result.stdout.strip()
    mg_pin_mode = "cpu"
    pin_map = ""
    client_cpus = ""
    client_node = ""
    for line in output.splitlines():
        # Each line looks like:  KEY='VALUE'
        m = re.match(r"^(\w+)='([^']*)'$", line.strip())
        if m:
            key, val = m.group(1), m.group(2)
            if key == "MG_PIN_MODE":
                mg_pin_mode = val
            elif key == "MG_PIN_MAP":
                pin_map = val
            elif key == "CLIENT_CPUS":
                client_cpus = val
            elif key == "CLIENT_NODE":
                client_node = val
    if not pin_map:
        raise RuntimeError(f"ha_pin_map.sh produced no MG_PIN_MAP; output was:\n{output}")
    if not client_cpus:
        raise RuntimeError(f"ha_pin_map.sh produced no CLIENT_CPUS; output was:\n{output}")
    return mg_pin_mode, pin_map, client_cpus, client_node


def _mgclient_connect(port: int, retries: int = 10, delay: float = 1.0):
    import mgclient

    last_exc: Optional[Exception] = None
    for attempt in range(retries):
        try:
            conn = mgclient.connect(host="127.0.0.1", port=port, lazy=False)
            conn.autocommit = True
            return conn
        except Exception as exc:
            last_exc = exc
            if attempt < retries - 1:
                time.sleep(delay)
    raise RuntimeError(f"Could not connect to bolt://127.0.0.1:{port} after {retries} attempts") from last_exc


def _run_query(conn, query: str, params: Optional[dict] = None):
    cursor = conn.cursor()
    cursor.execute(query, params or {})
    return cursor.fetchall()


def _reset_and_seed(main_port: int, replica_port: int) -> None:
    """Wipe ALL nodes (batched), recreate 500 K :Bench nodes + :Bench(id) index, wait for replica sync.

    DROP GRAPH is analytical-only; batched DETACH DELETE is used instead.
    """
    logger.info("Resetting graph on MAIN (port %d) …", main_port)
    conn = _mgclient_connect(main_port)

    for iteration in range(_WIPE_MAX_ITERS):
        for attempt in range(_WRITE_MAX_RETRIES):
            try:
                _run_query(conn, f"MATCH (n) WITH n LIMIT {_WIPE_BATCH_LIMIT} DETACH DELETE n")
                break
            except Exception as exc:
                if attempt >= _WRITE_MAX_RETRIES - 1:
                    raise RuntimeError(f"Wipe batch {iteration} failed after {_WRITE_MAX_RETRIES} attempts") from exc
                logger.debug("Wipe batch %d transient error (attempt %d): %s", iteration, attempt, exc)
                time.sleep(_WRITE_RETRY_DELAY_S)

        rows = _run_query(conn, "MATCH (n) RETURN count(n) AS c")
        remaining = rows[0][0] if rows else 0
        logger.debug("  After wipe batch %d: %d nodes remaining", iteration, remaining)
        if remaining == 0:
            break
    else:
        raise RuntimeError(
            f"Graph wipe did not complete within {_WIPE_MAX_ITERS} batches "
            f"(safety cap: {_WIPE_MAX_ITERS * _WIPE_BATCH_LIMIT} nodes)"
        )

    logger.info("Graph wiped; seeding %d :Bench nodes …", _EXPECTED_NODES)

    for batch in range(_NUM_BATCHES):
        lo = batch * _BATCH_SIZE
        hi = lo + _BATCH_SIZE - 1
        for attempt in range(_WRITE_MAX_RETRIES):
            try:
                _run_query(
                    conn,
                    "UNWIND range($a, $b) AS i CREATE (:Bench {id: i, x: i % 1000})",
                    {"a": lo, "b": hi},
                )
                break
            except Exception as exc:
                if attempt >= _WRITE_MAX_RETRIES - 1:
                    raise RuntimeError(f"Batch {batch} failed after {_WRITE_MAX_RETRIES} attempts") from exc
                logger.debug("Batch %d transient error (attempt %d): %s", batch, attempt, exc)
                time.sleep(_WRITE_RETRY_DELAY_S)

        if (batch + 1) % 10 == 0:
            logger.info("  … inserted %d / %d nodes", (batch + 1) * _BATCH_SIZE, _EXPECTED_NODES)

    # The :Bench(id) index may already exist if the previous cell's wipe only removed nodes
    # (schema objects survive a data wipe).  Tolerate "already exists" errors.
    for attempt in range(_WRITE_MAX_RETRIES):
        try:
            _run_query(conn, "CREATE INDEX ON :Bench(id)")
            break
        except Exception as exc:
            exc_str = str(exc).lower()
            if "already exists" in exc_str or "index already" in exc_str:
                logger.debug("CREATE INDEX ON :Bench(id) — index already exists, skipping.")
                break
            if attempt >= _WRITE_MAX_RETRIES - 1:
                raise RuntimeError("Could not create :Bench(id) index") from exc
            logger.debug("CREATE INDEX transient error (attempt %d): %s", attempt, exc)
            time.sleep(_WRITE_RETRY_DELAY_S)

    logger.info(":Bench dataset and index ready on MAIN.")
    _wait_for_replica_sync(replica_port, _EXPECTED_NODES)


def _wait_for_replica_sync(replica_port: int, expected_nodes: int) -> None:
    """Poll the replica until it reports `expected_nodes` :Bench nodes."""
    logger.info("Waiting for replica (port %d) to hold %d :Bench nodes …", replica_port, expected_nodes)
    deadline = time.monotonic() + _REPLICA_SYNC_TIMEOUT_S
    conn = _mgclient_connect(replica_port, retries=20, delay=2.0)
    last_count = -1

    while time.monotonic() < deadline:
        try:
            rows = _run_query(conn, "MATCH (n:Bench) RETURN count(n) AS c")
            count = rows[0][0] if rows else 0
        except Exception as exc:
            logger.debug("Replica count query failed: %s — reconnecting", exc)
            try:
                conn = _mgclient_connect(replica_port, retries=5, delay=1.0)
            except Exception:
                pass
            time.sleep(_REPLICA_SYNC_POLL_S)
            continue

        if count != last_count:
            logger.info("  Replica :Bench count = %d / %d", count, expected_nodes)
            last_count = count

        if count >= expected_nodes:
            logger.info("Replica is in sync (%d nodes).", count)
            return

        time.sleep(_REPLICA_SYNC_POLL_S)

    raise RuntimeError(
        f"Replica did not reach {expected_nodes} :Bench nodes within "
        f"{_REPLICA_SYNC_TIMEOUT_S}s (last count: {last_count})"
    )


def _run_rustlg(
    mode: str,
    target: str,
    threads: int,
    duration: int,
    n_writers: int,
    client_pin: list[str],
    write_batch: int = 1,
) -> dict[str, float]:
    """Run rustlg pinned to client CPUs; return {"combined_qps", "read_qps", "write_qps"}.

    CLI: rustlg <nthreads> <dur> <mode> <target> [n_writers] [write_batch] — extras appended for split only.
    """
    cmd = list(client_pin) + [str(_RUSTLG), str(threads), str(duration), mode, target]
    if mode == "split":
        cmd.append(str(n_writers))
        cmd.append(str(write_batch))

    # splitslowmix commits 5 000-node transactions; allow extra drain time after the run window.
    grace = 120 if (mode == "split" and write_batch >= _SLOW_WRITE_BATCH) else 60

    logger.info("Running rustlg: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=duration + grace)

    logger.info("rustlg stdout: %s", result.stdout.strip())
    if result.returncode != 0:
        logger.warning("rustlg stderr: %s", result.stderr.strip())
        raise RuntimeError(f"rustlg exited with code {result.returncode}")

    m = re.search(r"qps=(\d+(?:\.\d+)?)", result.stdout)
    if not m:
        raise RuntimeError(f"Could not parse qps from rustlg output: {result.stdout!r}")
    combined_qps = float(m.group(1))

    # \b so "reads=" does not match the "reads=" substring inside "nthreads=<N>".
    m_reads = re.search(r"\breads=(\d+)", result.stdout)
    m_writes = re.search(r"\bwrites=(\d+)", result.stdout)
    if m_reads and m_writes:
        read_qps = int(m_reads.group(1)) / duration
        write_qps = int(m_writes.group(1)) / duration
    else:
        # point/heavy modes — all ops are reads; no reads=/writes= tokens in output.
        read_qps = combined_qps
        write_qps = 0.0

    logger.info(
        "rustlg mode=%s  target=%s  combined=%.0f  read=%.0f  write=%.0f",
        mode,
        target,
        combined_qps,
        read_qps,
        write_qps,
    )
    return {"combined_qps": combined_qps, "read_qps": read_qps, "write_qps": write_qps}


def _kill_leftover_ha_processes() -> None:
    """Best-effort: kill any memgraph processes listening on the HA ports."""
    for port in _HA_PORTS:
        try:
            result = subprocess.run(
                ["fuser", str(port) + "/tcp"],
                capture_output=True,
                text=True,
            )
            pids = result.stdout.split()
            for pid_str in pids:
                try:
                    os.kill(int(pid_str), signal.SIGKILL)
                    logger.info("Killed leftover process %s on port %d", pid_str, port)
                except ProcessLookupError:
                    pass
        except FileNotFoundError:
            # fuser not available; try lsof
            try:
                result = subprocess.run(
                    ["lsof", "-t", f"-i:{port}"],
                    capture_output=True,
                    text=True,
                )
                for pid_str in result.stdout.split():
                    try:
                        os.kill(int(pid_str), signal.SIGKILL)
                        logger.info("Killed leftover process %s on port %d (lsof)", pid_str, port)
                    except ProcessLookupError:
                        pass
            except FileNotFoundError:
                logger.debug("Neither fuser nor lsof available; skipping port-kill sweep")


def _build_result_json(
    threads: int,
    duration: int,
    results: dict[tuple[str, str], dict[str, float]],
) -> dict:
    """Assemble the bench-graph JSON uploadable by tools/bench-graph-client/main.py.

    results: (label, target) → {"combined_qps", "read_qps", "write_qps"}; each cell emits three scan entries.
    """
    uname = platform.uname()
    platform_str = f"{uname.system}-{uname.machine}-{uname.release}"

    def _metric(qps: float) -> dict:
        return {
            WITHOUT_FINE_GRAINED_AUTHORIZATION: {
                COUNT: 1,
                DURATION: duration,
                THROUGHPUT: qps,
                NUM_WORKERS: threads,
                RETRIES: 0,
                DATABASE: {},
            }
        }

    scan_cells = {
        f"rust_{label}_{target}_{suffix}": _metric(qps_breakdown[f"{suffix}_qps"])
        for (label, target), qps_breakdown in results.items()
        for suffix in ("combined", "read", "write")
    }

    return {
        "__run_configuration__": {
            "vendor": "memgraph",
            "condition": "hot",
            "num_workers_for_benchmark": threads,
            "benchmark_mode": "Mixed",
            "platform": platform_str,
            "installation_type": "ha",
        },
        "__import__": {
            "client": {},
            "database": {},
        },
        "bench": {
            "default": {
                "scan": scan_cells,
            }
        },
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run rustlg against a CPU-pinned Memgraph HA cluster and emit bench-graph JSON."
    )
    parser.add_argument(
        "--export-results",
        required=True,
        metavar="PATH",
        help="Path to write the bench-graph-compatible result JSON.",
    )
    parser.add_argument("--threads", type=int, default=8, metavar="N", help="Rust worker threads (default 8).")
    parser.add_argument(
        "--duration", type=int, default=30, metavar="S", help="Duration per mode in seconds (default 30)."
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )

    args = _parse_args()

    mg_real_binary = os.environ.get("MG_REAL_BINARY", "")
    if not mg_real_binary:
        logger.error("MG_REAL_BINARY is not set; export it before running this script.")
        sys.exit(1)
    if not Path(mg_real_binary).is_file():
        logger.error("MG_REAL_BINARY=%s does not exist or is not a file.", mg_real_binary)
        sys.exit(1)
    if not _RUSTLG.is_file():
        logger.error("rustlg binary not found at %s; build it first.", _RUSTLG)
        sys.exit(1)
    for var in ("MEMGRAPH_ENTERPRISE_LICENSE", "MEMGRAPH_ORGANIZATION_NAME"):
        if not os.environ.get(var):
            logger.error("%s is not set; HA is an enterprise feature.", var)
            sys.exit(1)

    logger.info("Computing CPU pin map via ha_pin_map.sh …")
    mg_pin_mode, mg_pin_map, client_cpus, client_node = _compute_pin_map()
    logger.info(
        "MG_PIN_MODE=%s  MG_PIN_MAP=%s  CLIENT_CPUS=%s  CLIENT_NODE=%s",
        mg_pin_mode,
        mg_pin_map,
        client_cpus,
        client_node,
    )

    # Export so ha_pin_wrapper.sh picks them up when it is exec'd by the runner.
    os.environ["MG_REAL_BINARY"] = mg_real_binary
    os.environ["MG_PIN_MAP"] = mg_pin_map
    os.environ["MG_PIN_MODE"] = mg_pin_mode

    # Client pin prefix: numactl (numa mode, if available) or taskset.
    if mg_pin_mode == "numa" and client_node and shutil.which("numactl"):
        client_pin = ["numactl", f"--cpunodebind={client_node}", f"--membind={client_node}"]
    else:
        client_pin = ["taskset", "-c", client_cpus]

    ctx = BenchmarkContext(
        vendor_name="memgraph",
        installation_type=BenchmarkInstallationType.HA,
        vendor_binary=str(_PIN_WRAPPER),
        vendor_args={"ha-cluster-yaml": _CLUSTER_YAML},
        num_workers_for_benchmark=args.threads,
        temporary_directory=None,  # TemporaryDirectory uses the system default
    )

    runner = runners.BaseRunner.create(benchmark_context=ctx)

    # init phase registers instances and coordinators; measurement phase re-starts with existing Raft state.
    results: dict[tuple[str, str], dict[str, float]] = {}

    try:
        logger.info("Starting HA cluster (init phase) …")
        runner.start_db_init("rust_bench_init")
        logger.info("Cluster init phase up; stopping to checkpoint …")
        runner.stop_db_init("rust_bench_init")

        logger.info("Starting HA cluster (measurement phase) …")
        runner.start_db("rust_bench")

        main_port = runner.get_database_port()
        logger.info("MAIN is on bolt port %d.", main_port)

        for label, rustlg_mode, write_batch, arm_nthreads in _BENCH_ARMS:
            nthreads = arm_nthreads if arm_nthreads is not None else args.threads
            for target in ("direct", "routing"):
                _reset_and_seed(main_port, _REPLICA_PORT)
                results[(label, target)] = _run_rustlg(
                    rustlg_mode, target, nthreads, args.duration, _SPLIT_WRITERS, client_pin, write_batch
                )

    finally:
        logger.info("Tearing down HA cluster …")
        try:
            runner.stop_db("rust_bench")
        except Exception as exc:
            logger.warning("stop_db raised (ignored during teardown): %s", exc)
        try:
            runner.clean_db()
        except Exception as exc:
            logger.warning("clean_db raised (ignored during teardown): %s", exc)
        try:
            runner._cleanup()
        except Exception as exc:
            logger.warning("_cleanup raised (ignored during teardown): %s", exc)
        _kill_leftover_ha_processes()

    result = _build_result_json(
        threads=args.threads,
        duration=args.duration,
        results=results,
    )

    export_path = Path(args.export_results)
    export_path.parent.mkdir(parents=True, exist_ok=True)
    with export_path.open("w") as fh:
        json.dump(result, fh, indent=2)

    logger.info("Results written to %s", export_path)
    logger.info("Summary: threads=%d  duration=%ds", args.threads, args.duration)
    for (label, target), qps_breakdown in results.items():
        logger.info(
            "  rust_%s_%s  combined=%.0f  read=%.0f  write=%.0f",
            label,
            target,
            qps_breakdown["combined_qps"],
            qps_breakdown["read_qps"],
            qps_breakdown["write_qps"],
        )


if __name__ == "__main__":
    main()
