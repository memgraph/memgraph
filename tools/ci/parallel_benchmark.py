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
Run the CI mgbench workloads against a prod memgraph Docker image, one NUMA
node per concurrent workload.

Each NUMA node is a *slot*. A workload takes a free slot and benchmark.py
(--installation-type docker) creates the memgraph and bolt-client containers
pinned to that node's CPUs and memory, imports, benchmarks, then removes them.
Passing a single node reproduces today's serial CI run; passing all nodes is
the parallel experiment. `summarize` then compares the result directories.

  parallel_benchmark.py run --image memgraph/memgraph:<tag> --numa-nodes 0,1,2,3 --results-dir out/parallel
  parallel_benchmark.py run --image memgraph/memgraph:<tag> --numa-nodes 0       --results-dir out/serial
  parallel_benchmark.py summarize out/serial out/parallel
"""

import argparse
import concurrent.futures
import datetime
import json
import math
import os
import pathlib
import queue
import shutil
import statistics
import subprocess
import sys
import threading
import time
from dataclasses import asdict, dataclass, field

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
MGBENCH_DIR = REPO_ROOT / "tests" / "mgbench"
DOCKER_NETWORK_NAME = "mgbench_network"  # Must match runners.DOCKER_NETWORK_NAME
BASE_BOLT_PORT = 7687


@dataclass(frozen=True)
class Task:
    name: str  # Matches the bench-graph benchmark name used in diff_release.yaml
    workers: int
    targets: tuple
    minutes: float  # Rough duration from daily_benchmark.yaml, used to schedule longest-first


# Mirrors the `test-memgraph mgbench*` cases in release/package/mgbuild.sh. The
# vector/text workloads drop `query_modules_directory`: the image already
# points at /usr/lib/memgraph/query_modules.
TASKS = [
    Task("mgbench", 6, ("pokec/medium/*/*",), 20),
    Task("mgbench-planner-optimizations", 6, ("pokec_planner_optimizations/medium/*/*",), 15),
    Task("vector-search-index", 1, ("vector_search_index/default/vector/*",), 5),
    Task("vector-search-edge-index", 1, ("vector_search_edge_index/default/vector/*",), 5),
    Task("text-search-index", 1, ("text_search_index/default/text/*",), 5),
    Task("text-search-edge-index", 1, ("text_search_edge_index/default/text/*",), 5),
    Task("load-parquet", 1, ("load_parquet",), 4),
    Task("supernode", 1, ("supernode",), 1.5),
]
TASKS_BY_NAME = {t.name: t for t in TASKS}


@dataclass(frozen=True)
class Slot:
    node: int
    cpus: str  # cpulist as reported by sysfs, e.g. "0-15,64-79"

    @property
    def cpu_count(self):
        count = 0
        for part in self.cpus.split(","):
            lo, _, hi = part.partition("-")
            count += (int(hi) - int(lo) + 1) if hi else 1
        return count

    @property
    def bolt_port(self):
        return BASE_BOLT_PORT + self.node


@dataclass
class RunRecord:
    task: str
    repeat: int
    node: int
    cpus: str
    started_at: str = ""
    finished_at: str = ""
    duration_sec: float = 0.0
    returncode: int = None
    result_file: str = ""
    log_file: str = ""
    concurrent_with: list = field(default_factory=list)  # Tasks that were running when this one started


def numa_slot(node: int) -> Slot:
    cpulist = pathlib.Path(f"/sys/devices/system/node/node{node}/cpulist")
    if not cpulist.is_file():
        sys.exit(f"NUMA node {node} not found ({cpulist})")
    return Slot(node=node, cpus=cpulist.read_text().strip())


def available_numa_nodes() -> list:
    nodes = []
    for entry in pathlib.Path("/sys/devices/system/node").glob("node[0-9]*"):
        nodes.append(int(entry.name[len("node") :]))
    return sorted(nodes)


def ensure_docker_network():
    # The runners create this lazily; doing it once up front avoids a create/create race between slots.
    networks = subprocess.run(
        ["docker", "network", "ls", "--format", "{{.Name}}"], check=True, capture_output=True, text=True
    ).stdout.split()
    if DOCKER_NETWORK_NAME not in networks:
        subprocess.run(["docker", "network", "create", DOCKER_NETWORK_NAME], check=True)


def ensure_image(image: str):
    if subprocess.run(["docker", "image", "inspect", image], capture_output=True).returncode != 0:
        sys.exit(f"Docker image not found locally: {image}")


def build_command(task: Task, slot: Slot, args, result_file: pathlib.Path) -> list:
    cmd = []
    if shutil.which("numactl"):
        # benchmark.py itself is light, but keep its docker CLI calls off the other nodes too.
        cmd += ["numactl", f"--cpunodebind={slot.node}", f"--membind={slot.node}"]
    resources = MGBENCH_DIR / "workloads" / "resources"
    cmd += [
        sys.executable,
        "benchmark.py",
        "--vendor-name",
        "memgraph",
        "--installation-type",
        "docker",
        "--docker-image",
        args.image,
        "--docker-client-image",
        args.client_image,
        "--docker-name-suffix",
        f"_node{slot.node}",
        "--docker-cpuset-cpus",
        slot.cpus,
        "--docker-cpuset-mems",
        str(slot.node),
        # load_parquet references this directory by host-absolute path from inside memgraph.
        "--docker-volume",
        f"{resources}:{resources}:ro",
        "--num-workers-for-benchmark",
        str(task.workers),
        "--num-workers-for-import",
        str(max(1, slot.cpu_count // 2)),
        "--cache-directory",
        str(pathlib.Path(args.cache_root) / task.name),
        "--export-results",
        str(result_file),
        "--no-authorization",
        "--vendor-specific",
        f"bolt-port={slot.bolt_port}",
        "--",
        *task.targets,
    ]
    return cmd


def now_iso():
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")


class Runner:
    def __init__(self, args, slots, results_dir: pathlib.Path):
        self.args = args
        self.results_dir = results_dir
        self.slots = queue.Queue()
        for slot in slots:
            self.slots.put(slot)
        self.lock = threading.Lock()
        self.in_flight = {}  # task name -> node
        self.records = []

    def run_one(self, task: Task, repeat: int) -> RunRecord:
        slot = self.slots.get()
        stem = f"{task.name}.r{repeat}"
        result_file = self.results_dir / f"{stem}.json"
        log_file = self.results_dir / f"{stem}.log"
        record = RunRecord(task=task.name, repeat=repeat, node=slot.node, cpus=slot.cpus)
        record.result_file = result_file.name
        record.log_file = log_file.name
        try:
            with self.lock:
                record.concurrent_with = sorted(f"{name}@node{node}" for name, node in self.in_flight.items())
                self.in_flight[stem] = slot.node
            cmd = build_command(task, slot, self.args, result_file)
            print(f"[node{slot.node}] start {stem} (alongside: {record.concurrent_with or 'nothing'})", flush=True)
            record.started_at = now_iso()
            start = time.monotonic()
            if self.args.dry_run:
                print("  " + " ".join(cmd), flush=True)
                record.returncode = 0
            else:
                with open(log_file, "w") as log:
                    log.write("+ " + " ".join(cmd) + "\n\n")
                    log.flush()
                    proc = subprocess.run(cmd, cwd=MGBENCH_DIR, stdout=log, stderr=subprocess.STDOUT)
                record.returncode = proc.returncode
            record.duration_sec = round(time.monotonic() - start, 1)
            record.finished_at = now_iso()
            status = "ok" if record.returncode == 0 else f"FAILED rc={record.returncode}"
            print(f"[node{slot.node}] done  {stem} in {record.duration_sec:.0f}s: {status}", flush=True)
        finally:
            with self.lock:
                self.in_flight.pop(stem, None)
                self.records.append(record)
            self.slots.put(slot)
        return record


def cmd_run(args):
    nodes = [int(n) for n in args.numa_nodes.split(",")] if args.numa_nodes else available_numa_nodes()
    slots = [numa_slot(n) for n in nodes]
    if len({s.bolt_port for s in slots}) != len(slots):
        sys.exit("NUMA nodes must be distinct")

    if args.tasks == "all":
        tasks = list(TASKS)
    else:
        try:
            tasks = [TASKS_BY_NAME[name] for name in args.tasks.split(",")]
        except KeyError as e:
            sys.exit(f"Unknown task {e}; known: {', '.join(TASKS_BY_NAME)}")
    # Longest first packs the slots better; each repeat re-runs the whole set.
    tasks.sort(key=lambda t: t.minutes, reverse=True)

    results_dir = pathlib.Path(args.results_dir).resolve()
    results_dir.mkdir(parents=True, exist_ok=True)
    pathlib.Path(args.cache_root).mkdir(parents=True, exist_ok=True)

    if not args.dry_run:
        ensure_image(args.image)
        ensure_image(args.client_image)
        ensure_docker_network()

    print(f"Slots: {', '.join(f'node{s.node} (cpus {s.cpus}, bolt {s.bolt_port})' for s in slots)}")
    print(f"Tasks: {', '.join(t.name for t in tasks)} x{args.repeat}")

    runner = Runner(args, slots, results_dir)
    started = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(slots)) as pool:
        futures = [pool.submit(runner.run_one, task, r) for r in range(1, args.repeat + 1) for task in tasks]
        for future in concurrent.futures.as_completed(futures):
            future.result()  # re-raise unexpected exceptions from the worker
    wall = round(time.monotonic() - started, 1)

    records = sorted(runner.records, key=lambda r: r.started_at)
    manifest = {
        "label": args.label or ("serial" if len(slots) == 1 else f"parallel-{len(slots)}"),
        "image": args.image,
        "client_image": args.client_image,
        "slots": [asdict(s) for s in slots],
        "repeat": args.repeat,
        "wall_clock_sec": wall,
        "busy_sec": round(sum(r.duration_sec for r in records), 1),
        "runs": [asdict(r) for r in records],
    }
    (results_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")

    failed = [r for r in records if r.returncode != 0]
    print(f"\nWall clock {wall:.0f}s for {manifest['busy_sec']:.0f}s of benchmark time on {len(slots)} slot(s).")
    if failed:
        print("Failed runs: " + ", ".join(f"{r.task}.r{r.repeat} (node{r.node}, rc={r.returncode})" for r in failed))
        return 1
    return 0


# --- summarize ---------------------------------------------------------------


def walk_queries(data, path=()):
    """Yield (path, throughput, mean latency) for every query result in a benchmark_result.json."""
    if not isinstance(data, dict):
        return
    if "throughput" in data and "latency_stats" in data:
        if path and path[-1].endswith("_authorization"):  # CI runs one auth mode; drop the constant suffix
            path = path[:-1]
        yield path, data["throughput"], data["latency_stats"].get("mean")
        return
    for key, value in data.items():
        if key.startswith("__"):  # __run_configuration__, __import__
            continue
        yield from walk_queries(value, path + (key,))


def load_results(results_dir: pathlib.Path):
    """-> {task: {query_path: [(throughput, latency), ...]}} over every repeat that produced results."""
    manifest = json.loads((results_dir / "manifest.json").read_text())
    per_task = {}
    for run in manifest["runs"]:
        result_file = results_dir / run["result_file"]
        if run["returncode"] != 0 or not result_file.is_file():
            continue
        queries = per_task.setdefault(run["task"], {})
        for path, throughput, latency in walk_queries(json.loads(result_file.read_text())):
            queries.setdefault("/".join(path), []).append((throughput, latency))
    return manifest, per_task


def cv(values):
    if len(values) < 2:
        return float("nan")
    mean = statistics.fmean(values)
    return 100.0 * statistics.stdev(values) / mean if mean else float("nan")


def geomean(values):
    values = [v for v in values if v and v > 0]
    return math.exp(sum(math.log(v) for v in values) / len(values)) if values else float("nan")


def fmt(value, spec):
    return "-" if value is None or (isinstance(value, float) and math.isnan(value)) else format(value, spec)


def cmd_summarize(args):
    dirs = [pathlib.Path(d).resolve() for d in args.results_dirs]
    loaded = [load_results(d) for d in dirs]
    labels = [m["label"] for m, _ in loaded]
    md = args.markdown

    def row(cells):
        return "| " + " | ".join(cells) + " |" if md else "  ".join(cells)

    print(row(["run", "slots", "repeat", "wall clock", "busy", "failed runs"]))
    if md:
        print(row(["---"] * 6))
    for manifest, _ in loaded:
        failed = sum(1 for r in manifest["runs"] if r["returncode"] != 0)
        print(
            row(
                [
                    manifest["label"],
                    str(len(manifest["slots"])),
                    str(manifest["repeat"]),
                    f"{manifest['wall_clock_sec'] / 60:.1f} min",
                    f"{manifest['busy_sec'] / 60:.1f} min",
                    str(failed),
                ]
            )
        )
    print()

    base_manifest, base = loaded[0]
    header = ["task", "query", f"{labels[0]} QPS", "CV%"]
    for label in labels[1:]:
        header += [f"{label} QPS", "CV%", f"vs {labels[0]}"]
    print(row(header))
    if md:
        print(row(["---"] * len(header)))

    ratios_per_dir = {label: [] for label in labels[1:]}
    for task in [t.name for t in TASKS if t.name in base]:
        for query, samples in sorted(base[task].items()):
            base_qps = [s[0] for s in samples]
            cells = [task, query, fmt(statistics.fmean(base_qps), ".1f"), fmt(cv(base_qps), ".1f")]
            for label, (_, other) in zip(labels[1:], loaded[1:]):
                other_samples = other.get(task, {}).get(query)
                if not other_samples:
                    cells += ["-", "-", "-"]
                    continue
                other_qps = [s[0] for s in other_samples]
                ratio = statistics.fmean(other_qps) / statistics.fmean(base_qps) if statistics.fmean(base_qps) else None
                if ratio:
                    ratios_per_dir[label].append(ratio)
                cells += [fmt(statistics.fmean(other_qps), ".1f"), fmt(cv(other_qps), ".1f"), fmt(ratio, ".3f")]
            print(row(cells))

    if labels[1:]:
        print()
        for label, ratios in ratios_per_dir.items():
            if not ratios:
                continue
            worst = min(ratios)
            print(
                f"{label} vs {labels[0]}: geomean throughput ratio {geomean(ratios):.3f} over {len(ratios)} queries "
                f"(worst {worst:.3f}, best {max(ratios):.3f}); 1.000 means no change"
            )
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="run the workloads, one NUMA node per slot")
    run.add_argument("--image", required=True, help="memgraph image to benchmark, e.g. memgraph/memgraph:<tag>")
    run.add_argument("--client-image", default="memgraph/mgbench-client", help="mgbench bolt client image")
    run.add_argument(
        "--numa-nodes",
        default="",
        help="comma-separated NUMA nodes to use as slots; '0' reproduces the serial CI run (default: all nodes)",
    )
    run.add_argument("--tasks", default="all", help=f"comma-separated subset of: {', '.join(TASKS_BY_NAME)}")
    run.add_argument("--repeat", type=int, default=1, help="run the whole task set this many times")
    run.add_argument("--results-dir", required=True, help="where result JSONs, logs and manifest.json go")
    run.add_argument(
        "--cache-root",
        default=os.path.join(os.path.expanduser("~"), ".cache", "mgbench-parallel"),
        help="per-task mgbench caches live under here (datasets + calibrated query counts); "
        "keep it stable between serial and parallel runs so both use the same query counts",
    )
    run.add_argument("--label", default="", help="name recorded in manifest.json (default: serial / parallel-N)")
    run.add_argument("--dry-run", action="store_true", help="print the benchmark.py commands and exit")
    run.set_defaults(func=cmd_run)

    summarize = sub.add_parser("summarize", help="compare result directories; the first one is the baseline")
    summarize.add_argument("results_dirs", nargs="+")
    summarize.add_argument("--markdown", action="store_true", help="emit a markdown table (for GITHUB_STEP_SUMMARY)")
    summarize.set_defaults(func=cmd_summarize)

    args = parser.parse_args()
    sys.exit(args.func(args))


if __name__ == "__main__":
    main()
