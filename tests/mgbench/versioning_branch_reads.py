#!/usr/bin/env python3
# Copyright 2026 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL.txt; by using this file, you agree to be bound by the
# terms of the Business Source License, and you may not use this file except in
# compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with the Business
# Source License, use of this software will be governed by the Apache License,
# Version 2.0, included in the file licenses/APL.txt.

"""Read latency on main vs Graph Versioning branches at several change levels (pokec medium).

One server, one load. One branch per level, each used from its own Bolt connection. Per level, of the
selected :User vertices: all get SET n.v2probe = 1, every 2nd gets SET n:Probe, every 5th gets a new
(:User)-[:V2Probe]->(:User) edge. For each query and round, main and every branch run back to back;
the order rotates every round and alternates direction, so drift and position bias hit all targets
equally. Ratios are per-round paired (branch / main in the same round), then the median.
Every branch result is checked against the exact expected value.

Verdict (exit code):
  FAIL    (1)  a wrong branch result, the server did not start or crashed, or a ratio above FAIL_RATIO.
  FLAG    (0)  a ratio above the per-level budget; emitted as a GitHub `::warning`.
  PASS    (0)
  SKIPPED (0)  branches refused for lack of an enterprise license.
Any other refusal of CREATE BRANCH is a FAIL. `--no-gate` makes every ratio threshold report-only
(wrong results and crashes still fail). point_lookup_x200 (sub-millisecond, noisy) and the unpaired
label_probe (main has no :Probe label, the paired variant covers it) are only correctness-checked.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import signal
import socket
import statistics
import subprocess
import sys
import tempfile
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import mgclient

SCRIPT_DIR = Path(__file__).resolve().parent

# Ratio (branch / main, per-round median) thresholds.
FAIL_RATIO = 2.5
# (level, budget): the budget of a level is the one of the nearest listed level.
BUDGETS: dict[float, float] = {0.0: 1.25, 0.01: 1.30, 0.10: 1.40, 0.50: 1.60, 0.90: 1.80}
UNGATED_QUERIES = frozenset({"point_lookup_x200", "label_probe"})

DEFAULT_LEVELS = "0,0.01,0.10,0.50,0.90"
LICENSE_REFUSAL = "enterprise feature"
START_TIMEOUT_S = 120

QUERIES: dict[str, str] = {
    "point_lookup_x200": "UNWIND range(0, 199) AS i MATCH (n:User {id: i * 7 + 1}) RETURN count(n)",
    "count_all": "MATCH (n:User) RETURN count(n)",
    "agg_props": "MATCH (n:User) RETURN avg(n.age), sum(n.completion_percentage), count(n.v2probe)",
    "label_scan_filter": "MATCH (n:User) WHERE n.age > 30 RETURN count(n)",
    "label_probe": "MATCH (n:Probe) RETURN count(n)",
    "expand_1hop_friend": "MATCH (n:User)-[:Friend]->(m) RETURN count(m)",
    "expand_1hop_any": "MATCH (n:User)-->(m) RETURN count(m)",
    "expand_2hop_from_100": "UNWIND range(1, 100) AS i MATCH (n:User {id: i})-[:Friend]->()-[:Friend]->(m) "
    "RETURN count(m)",
    "bfs_x20": "UNWIND range(1, 20) AS i MATCH (a:User {id: i}), (z:User {id: i + 5000}) "
    "MATCH p = (a)-[:Friend *BFS ..6]->(z) RETURN count(p)",
    "order_by_limit": "MATCH (n:User) RETURN n.id ORDER BY n.id DESC LIMIT 10",
}

EDGE_RE = re.compile(r"MATCH \(n:User \{id: (\d+)\}\), \(m:User \{id: (\d+)\}\) CREATE")
USER_RE = re.compile(r"CREATE \(:User \{(.*)\}\);")
PROP_RE = re.compile(r'(\w+): ("[^"]*"|-?\d+)')


class BenchError(RuntimeError):
    """The benchmark cannot produce a trustworthy result (server down, refused statement, bad dataset)."""


class Skipped(Exception):
    """Branches are unavailable for a legitimate reason (no enterprise license)."""


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


class Server:
    def __init__(self, binary: Path, ld_path: str, datadir: Path, port: int) -> None:
        self.binary, self.ld_path, self.datadir, self.port = binary, ld_path, datadir, port
        self.logfile = datadir.parent / f"{datadir.name}.log"
        self.proc: subprocess.Popen[bytes] | None = None

    def start(self) -> None:
        with socket.socket() as s:
            if s.connect_ex(("127.0.0.1", self.port)) == 0:
                raise BenchError(f"port {self.port} already in use (stale server?)")
        env = dict(os.environ)
        if self.ld_path:
            env["LD_LIBRARY_PATH"] = self.ld_path
        args = [
            str(self.binary),
            "--experimental-enabled=versioning",
            "--storage-wal-enabled=true",
            "--storage-snapshot-interval-sec=86400",
            "--telemetry-enabled=false",
            "--log-level=WARNING",
            f"--data-directory={self.datadir}",
            f"--bolt-port={self.port}",
        ]
        with open(self.logfile, "wb") as logf:
            self.proc = subprocess.Popen(args, env=env, stdout=logf, stderr=logf, start_new_session=True)
        deadline = time.time() + START_TIMEOUT_S
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise BenchError(f"memgraph exited at startup, rc={self.proc.returncode}")
            try:
                connect(self.port).close()
                return
            except mgclient.Error:
                time.sleep(0.5)
        raise BenchError("memgraph did not become ready")

    def alive(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def stop(self) -> None:
        if self.proc is None:
            return
        if self.proc.poll() is None:
            try:
                os.killpg(self.proc.pid, signal.SIGTERM)
                self.proc.wait(timeout=30)
            except (ProcessLookupError, subprocess.TimeoutExpired):
                _sigkill(self.proc)
        self.proc = None

    def log_tail(self, lines: int = 30) -> str:
        try:
            return "\n".join(self.logfile.read_text(errors="replace").splitlines()[-lines:])
        except OSError:
            return ""


def _sigkill(proc: subprocess.Popen[bytes]) -> None:
    try:
        os.killpg(proc.pid, signal.SIGKILL)
        proc.wait(timeout=30)
    except (ProcessLookupError, subprocess.TimeoutExpired):
        pass


def connect(port: int) -> Any:
    conn = mgclient.connect(host="127.0.0.1", port=port)
    conn.autocommit = True
    return conn


def q(conn: Any, query: str, params: dict[str, Any] | None = None) -> list[tuple]:
    cur = conn.cursor()
    cur.execute(query, params or {})
    return cur.fetchall()


def timed(conn: Any, query: str) -> tuple[float, list[tuple]]:
    t = time.perf_counter()
    rows = q(conn, query)
    return (time.perf_counter() - t) * 1000, rows


def chunks(seq: Sequence[Any], n: int) -> list[Sequence[Any]]:
    return [seq[i : i + n] for i in range(0, len(seq), n)]


def select_indices(total: int, ratio: float) -> list[int]:
    """Deterministic, evenly spread subset of range(total) of size round(ratio*total)."""
    k = round(total * ratio)
    return sorted({(i * total) // k for i in range(k)}) if k else []


def level_name(level: float) -> str:
    return f"b{round(level * 100):02d}"


def budget_for(level: float) -> float:
    return BUDGETS[min(BUDGETS, key=lambda known: abs(known - level))]


def ensure_dataset(cache_dir: Path) -> Path:
    """Directory holding pokec medium's dataset.cypher + memgraph.cypher, fetched through mgbench if absent."""
    dataset = cache_dir / "datasets" / "pokec" / "medium"
    if (dataset / "dataset.cypher").is_file() and (dataset / "memgraph.cypher").is_file():
        return dataset
    log(f"pokec medium not cached in {cache_dir}, fetching it with mgbench")
    sys.path.insert(0, str(SCRIPT_DIR))
    import helpers  # noqa: PLC0415  (pulls in the whole mgbench package, only needed on a cold cache)
    from benchmark_context import BenchmarkContext  # noqa: PLC0415
    from constants import GraphVendors  # noqa: PLC0415
    from workloads.pokec import Pokec  # noqa: PLC0415

    context = BenchmarkContext(vendor_name=GraphVendors.MEMGRAPH)
    cache = helpers.Cache(str(cache_dir))
    Pokec("medium", benchmark_context=context).prepare(cache.cache_directory("datasets", "pokec", "medium"))
    return dataset


def load_pokec(conn: Any, dataset: Path) -> tuple[int, int]:
    for line in (dataset / "memgraph.cypher").read_text().splitlines():
        if line.startswith("CREATE INDEX"):
            q(conn, line.strip().rstrip(";"))
    users: list[dict[str, Any]] = []
    edges: list[list[int]] = []
    with open(dataset / "dataset.cypher") as f:
        for line in f:
            if line.startswith("CREATE (:User"):
                m = USER_RE.match(line)
                if not m:
                    raise BenchError(f"unparsable user line: {line[:120]}")
                users.append({k: (v[1:-1] if v[0] == '"' else int(v)) for k, v in PROP_RE.findall(m[1])})
            elif line.startswith("MATCH"):
                m = EDGE_RE.match(line)
                if not m:
                    raise BenchError(f"unparsable edge line: {line[:120]}")
                edges.append([int(m[1]), int(m[2])])
    log(f"parsed {len(users)} users, {len(edges)} edges")
    for batch in chunks(users, 10000):
        q(conn, "UNWIND $rows AS r CREATE (n:User) SET n = r", {"rows": list(batch)})
    for batch in chunks(edges, 20000):
        q(
            conn,
            "UNWIND $rows AS r MATCH (n:User {id: r[0]}), (m:User {id: r[1]}) CREATE (n)-[:Friend]->(m)",
            {"rows": list(batch)},
        )
    return len(users), len(edges)


def apply_mix(conn: Any, ids: list[int], idx: list[int], batch: int = 20000) -> dict[str, int]:
    sel = [ids[i] for i in idx]
    labelled = sel[::2]
    pairs = [[ids[i], ids[(i * 7 + 13) % len(ids)]] for i in idx[::5]]
    for c in chunks(sel, batch):
        q(conn, "UNWIND $ids AS i MATCH (n:User {id: i}) SET n.v2probe = 1", {"ids": list(c)})
    for c in chunks(labelled, batch):
        q(conn, "UNWIND $ids AS i MATCH (n:User {id: i}) SET n:Probe", {"ids": list(c)})
    for c in chunks(pairs, batch):
        q(
            conn,
            "UNWIND $p AS r MATCH (a:User {id: r[0]}), (z:User {id: r[1]}) CREATE (a)-[:V2Probe]->(z)",
            {"p": list(c)},
        )
    return {"props": len(sel), "labels": len(labelled), "edges": len(pairs)}


def expected(query_name: str, main_rows: list[tuple], changes: dict[str, int]) -> list[tuple]:
    if query_name == "agg_props":
        avg, s, _ = main_rows[0]
        return [(avg, s, changes["props"])]
    if query_name == "label_probe":
        return [(changes["labels"],)]
    if query_name == "expand_1hop_any":
        return [(main_rows[0][0] + changes["edges"],)]
    return main_rows


def median_iqr(values: list[float]) -> tuple[float, float, float]:
    ordered = sorted(values)
    return statistics.median(ordered), ordered[len(ordered) // 4], ordered[(3 * len(ordered)) // 4]


class Report:
    """Collects one record per query x level, prints the machine-readable lines, and derives the verdict."""

    def __init__(self, gate: bool) -> None:
        self.gate = gate
        self.records: list[dict[str, Any]] = []

    def add(
        self, query: str, level: float, main_ms: float, branch_ms: float, paired: list[float], correct: bool
    ) -> None:
        ratio, q1, q3 = median_iqr(paired)
        ungated = query in UNGATED_QUERIES
        budget = None if ungated else budget_for(level)
        if not correct:
            status = "fail"
        elif ungated:
            status = "ok"
        elif ratio > FAIL_RATIO:
            status = "fail" if self.gate else "flag"
        elif ratio > budget:
            status = "flag"
        else:
            status = "ok"
        rec = {
            "query": query,
            "level": level_name(level),
            "main_ms": round(main_ms, 3),
            "branch_ms": round(branch_ms, 3),
            "ratio": round(ratio, 3),
            "q1": round(q1, 3),
            "q3": round(q3, 3),
            "correct": correct,
            "budget": budget,
            "status": status,
            "gross": (not ungated) and correct and ratio > FAIL_RATIO,
        }
        self.records.append(rec)
        print(
            f"[vlevels] query={query} level={rec['level']} main_ms={rec['main_ms']} branch_ms={rec['branch_ms']} "
            f"ratio={rec['ratio']} q1={rec['q1']} q3={rec['q3']} correct={int(correct)} "
            f"budget={'-' if budget is None else budget} status={status}",
            flush=True,
        )
        if status == "flag":
            limit = FAIL_RATIO if ratio > FAIL_RATIO else budget
            print(f"::warning title=branch read budget::{query} {rec['level']} x{ratio:.2f} > {limit}", flush=True)

    @property
    def wrong(self) -> int:
        return sum(not r["correct"] for r in self.records)

    @property
    def flagged(self) -> int:
        return sum(r["status"] == "flag" for r in self.records)

    @property
    def gross(self) -> int:
        return sum(r["gross"] for r in self.records)

    def verdict(self) -> str:
        if any(r["status"] == "fail" for r in self.records):
            return "FAIL"
        return "FLAG" if self.flagged else "PASS"


def run_levels(args: argparse.Namespace, srv: Server, report: Report, out: dict[str, Any]) -> None:
    levels = [float(x) for x in args.levels.split(",")]
    dataset = ensure_dataset(Path(args.cache_directory))
    srv.start()
    main_c = connect(args.port)
    out["vertices"], out["edges"] = load_pokec(main_c, dataset)
    ids = sorted(r[0] for r in q(main_c, "MATCH (n:User) RETURN n.id"))
    targets: list[tuple[str, Any, dict[str, int] | None]] = [("main", main_c, None)]

    # Fair label probe: before any fork, give main its own unindexed label on exactly the vertices each branch
    # will later label :Probe, so main and branch scan for the same vertex set and return the same count.
    for lvl in levels:
        sel = [ids[i] for i in select_indices(len(ids), lvl)][::2]
        for c in chunks(sel, 20000):
            q(
                main_c,
                f"UNWIND $ids AS i MATCH (n:User {{id: i}}) SET n:MainProbe{level_name(lvl)[1:]}",
                {"ids": list(c)},
            )

    for lvl in levels:
        name = level_name(lvl)
        try:
            q(main_c, f"CREATE BRANCH {name}")
        except mgclient.DatabaseError as e:
            if LICENSE_REFUSAL in str(e):
                raise Skipped(str(e)) from e
            raise BenchError(f"CREATE BRANCH {name} refused: {e}") from e
        c = connect(args.port)
        q(c, f"USE BRANCH {name}")
        idx = select_indices(len(ids), lvl)
        t0 = time.perf_counter()
        changes = apply_mix(c, ids, idx) if idx else {"props": 0, "labels": 0, "edges": 0}
        out["branches"][name] = {"level": lvl, "changes": changes, "apply_s": round(time.perf_counter() - t0, 2)}
        log(f"{name}: {changes} in {out['branches'][name]['apply_s']} s")
        targets.append((name, c, changes))

    for qname, query in QUERIES.items():
        for _, c, _ in targets:  # warm-up: plan cache, page-in
            q(c, query)
        times: dict[str, list[float]] = {t[0]: [] for t in targets}
        ok = {t[0]: True for t in targets[1:]}
        n = len(targets)
        for r in range(args.rounds):
            order = [targets[(r + i) % n] for i in range(n)]
            if r % 2:
                order.reverse()
            round_rows: dict[str, list[tuple]] = {}
            for name, c, _ in order:
                ms, round_rows[name] = timed(c, query)
                times[name].append(ms)
            for name, _, changes in targets[1:]:
                assert changes is not None
                if round_rows[name] != expected(qname, round_rows["main"], changes):
                    ok[name] = False
        line = f"{qname:22} main {statistics.median(times['main']):9.2f} ms |"
        for (name, _, _), lvl in zip(targets[1:], levels):
            paired = [tb / tm for tb, tm in zip(times[name], times["main"])]
            report.add(qname, lvl, statistics.median(times["main"]), statistics.median(times[name]), paired, ok[name])
            line += f" {name} x{statistics.median(paired):5.2f}{'' if ok[name] else ' WRONG'}"
        print(line, flush=True)

    # Paired probe: main scans MainProbeNN, branch bNN scans Probe; same vertices, same count, no index either side.
    for (name, c, changes), lvl in zip(targets[1:], levels):
        assert changes is not None
        mq = f"MATCH (n:MainProbe{name[1:]}) RETURN count(n)"
        bq = "MATCH (n:Probe) RETURN count(n)"
        q(main_c, mq)
        q(c, bq)
        tm: list[float] = []
        tb: list[float] = []
        ok_pair = True
        for r in range(args.rounds):
            pair = [(tm, main_c, mq), (tb, c, bq)]
            if r % 2:
                pair.reverse()
            rows: dict[int, list[tuple]] = {}
            for acc, conn, qq in pair:
                ms, rows[id(acc)] = timed(conn, qq)
                acc.append(ms)
            ok_pair = ok_pair and rows[id(tm)] == rows[id(tb)] == [(changes["labels"],)]
        report.add(
            "label_probe_paired",
            lvl,
            statistics.median(tm),
            statistics.median(tb),
            [y / x for x, y in zip(tm, tb)],
            ok_pair,
        )


def write_summary(path: Path, report: Report, verdict: str, footer: str) -> None:
    levels = list(dict.fromkeys(r["level"] for r in report.records))
    queries = list(dict.fromkeys(r["query"] for r in report.records))
    by_key = {(r["query"], r["level"]): r for r in report.records}
    lines = [f"### Graph Versioning branch reads vs main: {verdict}", "", footer, ""]
    lines.append("| query | main ms | " + " | ".join(levels) + " |")
    lines.append("|---|---:|" + "---:|" * len(levels))
    for query in queries:
        first = by_key[(query, levels[0])]
        cells = []
        for level in levels:
            r = by_key.get((query, level))
            if r is None:
                cells.append("")
                continue
            mark = {"ok": "", "flag": " (over budget)", "fail": " (FAIL)"}[r["status"]]
            if not r["correct"]:
                mark = " (WRONG)"
            cells.append(f"x{r['ratio']:.2f}{mark}")
        lines.append(f"| {query} | {first['main_ms']:.2f} | " + " | ".join(cells) + " |")
    lines += [
        "",
        "Ratio = branch / main, per-round paired median. Budgets: "
        + ", ".join(f"{level_name(k)} x{v}" for k, v in BUDGETS.items())
        + f"; fail above x{FAIL_RATIO}.",
        "",
    ]
    path.write_text("\n".join(lines))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--binary", default=str(SCRIPT_DIR / ".." / ".." / "build" / "memgraph"))
    ap.add_argument("--ld-library-path", default="", help="only set for the server when given")
    ap.add_argument(
        "--cache-directory", default=str(SCRIPT_DIR / ".cache"), help="mgbench cache (datasets/pokec/medium)"
    )
    ap.add_argument("--scratch", default=None, help="parent of the per-run data directory (default: system tmp)")
    ap.add_argument("--port", type=int, default=7698)
    ap.add_argument("--levels", default=DEFAULT_LEVELS, help="comma-separated change ratios, one branch each")
    ap.add_argument("--rounds", type=int, default=11)
    ap.add_argument("--out", default=None, help="write the full result as JSON")
    ap.add_argument("--summary", default=None, help="write a markdown table (GitHub step summary)")
    ap.add_argument("--no-gate", action="store_true", help="ratio thresholds only report; wrong results still fail")
    args = ap.parse_args()

    work = Path(tempfile.mkdtemp(prefix="vlevels-", dir=args.scratch))
    srv = Server(Path(args.binary), args.ld_library_path, work / "data", args.port)
    report = Report(gate=not args.no_gate)
    out: dict[str, Any] = {
        "levels": [float(x) for x in args.levels.split(",")],
        "rounds": args.rounds,
        "binary": args.binary,
        "vertices": 0,
        "edges": 0,
        "branches": {},
    }
    started = time.time()
    verdict, reason = "", ""
    try:
        try:
            run_levels(args, srv, report, out)
            verdict = report.verdict()
        except Skipped as e:
            verdict, reason = "SKIPPED", str(e)
        except (BenchError, mgclient.Error, OSError) as e:
            verdict = "FAIL"
            reason = f"{'server crashed: ' if srv.proc is not None and not srv.alive() else ''}{type(e).__name__}: {e}"
            print(f"[vlevels] error={reason}", flush=True)
            tail = srv.log_tail()
            if tail:
                print(f"--- memgraph log tail ---\n{tail}\n---", flush=True)
    finally:
        srv.stop()
        shutil.rmtree(work, ignore_errors=True)

    runtime = round(time.time() - started)
    levels_str = ",".join(f"{x:g}" for x in out["levels"])
    if verdict == "SKIPPED":
        print(f"[vlevels] verdict=SKIPPED reason={reason}", flush=True)
        return 0
    print(
        f"[vlevels] verdict={verdict} wrong={report.wrong} flagged={report.flagged} gross={report.gross} "
        f"rounds={args.rounds} levels={levels_str} vertices={out['vertices']} edges={out['edges']} runtime_s={runtime}",
        flush=True,
    )
    out.update(verdict=verdict, reason=reason, gate=report.gate, runtime_s=runtime, records=report.records)
    if args.out:
        Path(args.out).write_text(json.dumps(out, indent=2))
    if args.summary:
        footer = (
            f"rounds={args.rounds}, vertices={out['vertices']}, edges={out['edges']}, runtime {runtime} s"
            + ("" if report.gate else ", report-only (`--no-gate`)")
            + (f", error: {reason}" if reason else "")
        )
        if report.records:
            write_summary(Path(args.summary), report, verdict, footer)
        else:
            Path(args.summary).write_text(f"### Graph Versioning branch reads vs main: {verdict}\n\n{footer}\n")
    return 1 if verdict == "FAIL" else 0


if __name__ == "__main__":
    sys.exit(main())
