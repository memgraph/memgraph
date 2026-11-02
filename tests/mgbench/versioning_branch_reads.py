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

One server, one load. One branch per level and mix (bNN, rNN), each used from its own Bolt connection.
Two change mixes, applied to the selected :User vertices of a level (`--mix unrelated|related|both`):
  unrelated  touches data the queries do not read: all get SET n.v2probe = 1, every 2nd gets SET n:Probe,
             every 5th gets a new (:User)-[:V2Probe]->(:User) edge.
  related    changes what the queries read: age and completion_percentage of all, id of every 4th (shifted
             out of the looked-up ranges), one :Friend edge deleted per 3rd, a new :Friend edge per 3rd,
             DETACH DELETE of every 20th; vertices behind the point lookups, BFS and ORDER BY are always
             among them. Its expected rows come from running the same mix on main inside one transaction,
             running every query there, and rolling back.
For each query and round, main and every branch of the mix run back to back; the order rotates every
round and alternates direction, so drift and position bias hit all targets equally. Ratios are per-round
paired (branch / main in the same round), then the median. Every branch result is checked against the
exact expected rows.

Verdict (exit code):
  FAIL    (1)  a wrong branch result, the server did not start or crashed, or a ratio above FAIL_RATIO.
  FLAG    (0)  a ratio above the per-level budget; emitted as a GitHub `::warning`.
  PASS    (0)
  SKIPPED (0)  branches refused for lack of an enterprise license.
Any other refusal of CREATE BRANCH is a FAIL. `--no-gate` makes every ratio threshold report-only
(wrong results and crashes still fail). point_lookup_x200 (sub-millisecond, noisy) and
label_probe_unpaired_info (informational: the branch returns the ~45k :Probe vertices, main has no :Probe
label and returns 0 rows, so the ratio is not like-for-like; unrelated mix only) are only
correctness-checked. label_probe is the gated like-for-like probe: main scans MainProbeNN, the branch
scans Probe, same vertices and same count.
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
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import mgclient

SCRIPT_DIR = Path(__file__).resolve().parent

# Ratio (branch / main, per-round median) thresholds.
FAIL_RATIO = 2.5
# (level, budget): the budget of a level is the one of the nearest listed level.
BUDGETS: dict[float, float] = {0.0: 1.25, 0.01: 1.30, 0.10: 1.40, 0.50: 1.60, 0.90: 1.80}
UNGATED_QUERIES = frozenset({"point_lookup_x200", "label_probe_unpaired_info"})

DEFAULT_LEVELS = "0,0.01,0.10,0.50,0.90"
MIXES = ("unrelated", "related")
LICENSE_REFUSAL = "enterprise feature"
START_TIMEOUT_S = 120

QUERIES: dict[str, str] = {
    "point_lookup_x200": "UNWIND range(0, 199) AS i MATCH (n:User {id: i * 7 + 1}) RETURN count(n)",
    "count_all": "MATCH (n:User) RETURN count(n)",
    "agg_props": "MATCH (n:User) RETURN avg(n.age), sum(n.completion_percentage), count(n.v2probe)",
    "label_scan_filter": "MATCH (n:User) WHERE n.age > 30 RETURN count(n)",
    "label_probe_unpaired_info": "MATCH (n:Probe) RETURN count(n)",
    "expand_1hop_friend": "MATCH (n:User)-[:Friend]->(m) RETURN count(m)",
    "expand_1hop_any": "MATCH (n:User)-->(m) RETURN count(m)",
    "expand_2hop_from_100": "UNWIND range(1, 100) AS i MATCH (n:User {id: i})-[:Friend]->()-[:Friend]->(m) "
    "RETURN count(m)",
    "bfs_x20": "UNWIND range(1, 20) AS i MATCH (a:User {id: i}), (z:User {id: i + 5000}) "
    "MATCH p = (a)-[:Friend *BFS ..6]->(z) RETURN count(p)",
    "order_by_limit": "MATCH (n:User) RETURN n.id ORDER BY n.id DESC LIMIT 10",
}

UNRELATED_ONLY_QUERIES = frozenset({"label_probe_unpaired_info"})
ID_SHIFT = 1_000_000  # moves a vertex's id out of every range the queries look up

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


def level_name(level: float, mix: str = "unrelated") -> str:
    return f"{'r' if mix == 'related' else 'b'}{round(level * 100):02d}"


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


def load_pokec(conn: Any, dataset: Path) -> tuple[int, list[list[int]]]:
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
    return len(users), edges


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


def hot_ids(ids: list[int]) -> list[int]:
    """Ids the QUERIES look up by value (point lookups, 2-hop starts, BFS ends) plus the ORDER BY ... DESC top 10."""
    wanted = {i * 7 + 1 for i in range(200)} | set(range(1, 101)) | set(range(5001, 5021)) | set(ids[-10:])
    return [i for i in ids if i in wanted]


def related_changes(ids: list[int], first_out: dict[int, int], level: float) -> dict[str, list[Any]]:
    """Deterministic 'related' change set: ids (original, pre-shift) per operation."""
    spread = {ids[i] for i in select_indices(len(ids), level)}
    hot = hot_ids(ids)
    touched = sorted(spread | {hot[i] for i in select_indices(len(hot), max(level, 0.5))}) if spread else []
    n = len(touched)
    return {
        "touched": touched,
        "drop_edges": [[a, first_out[a]] for a in touched[::3] if a in first_out],
        "new_edges": [[touched[j], touched[(j * 7 + 13) % n]] for j in range(0, n, 3) if (j * 7 + 13) % n != j],
        "deleted": touched[::20],
        "shifted": touched[::4],
    }


def apply_related(conn: Any, changes: dict[str, list[Any]], batch: int = 20000) -> dict[str, int]:
    """Change what the QUERIES read. Order matters: everything is keyed by original id, the id shift comes last."""
    for c in chunks(changes["touched"], batch):
        q(
            conn,
            "UNWIND $ids AS i MATCH (n:User {id: i}) "
            "SET n.age = n.age + 1, n.completion_percentage = n.completion_percentage + 1",
            {"ids": list(c)},
        )
    for c in chunks(changes["drop_edges"], batch):
        q(
            conn,
            "UNWIND $p AS r MATCH (a:User {id: r[0]})-[e:Friend]->(z:User {id: r[1]}) DELETE e",
            {"p": list(c)},
        )
    for c in chunks(changes["new_edges"], batch):
        q(
            conn,
            "UNWIND $p AS r MATCH (a:User {id: r[0]}), (z:User {id: r[1]}) CREATE (a)-[:Friend]->(z)",
            {"p": list(c)},
        )
    for c in chunks(changes["deleted"], batch):
        q(conn, "UNWIND $ids AS i MATCH (n:User {id: i}) DETACH DELETE n", {"ids": list(c)})
    for c in chunks(changes["shifted"], batch):
        q(conn, f"UNWIND $ids AS i MATCH (n:User {{id: i}}) SET n.id = n.id + {ID_SHIFT}", {"ids": list(c)})
    return {key: len(val) for key, val in changes.items()}


def related_expectations(port: int, changes: dict[str, list[Any]], queries: dict[str, str]) -> dict[str, list[tuple]]:
    """Exact rows of `queries` after the related mix, from the mix applied on main in one transaction that is rolled back."""
    conn = mgclient.connect(host="127.0.0.1", port=port)
    conn.autocommit = False  # the first execute opens the transaction, nothing is visible to others until commit()
    try:
        apply_related(conn, changes)
        rows = {name: q(conn, query) for name, query in queries.items()}
    finally:
        conn.rollback()
        conn.close()
    return rows


def expected(query_name: str, main_rows: list[tuple], changes: dict[str, int]) -> list[tuple]:
    if query_name == "agg_props":
        avg, s, _ = main_rows[0]
        return [(avg, s, changes["props"])]
    if query_name == "label_probe_unpaired_info":
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
        self,
        query: str,
        level: float,
        main_ms: float,
        branch_ms: float,
        paired: list[float],
        correct: bool,
        mix: str = "unrelated",
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
            "mix": mix,
            "level": level_name(level, mix),
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
            f"budget={'-' if budget is None else budget} status={status} mix={mix}",
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


@dataclass(frozen=True)
class Target:
    name: str
    conn: Any
    level: float = 0.0
    mix: str = ""
    changes: dict[str, int] = field(default_factory=dict)
    expect: Callable[[str, list[tuple]], list[tuple]] | None = None  # (query, main rows this round) -> rows


def mix_queries(mix: str) -> dict[str, str]:
    return {n: x for n, x in QUERIES.items() if mix == "unrelated" or n not in UNRELATED_ONLY_QUERIES}


def create_branch(main_c: Any, port: int, name: str) -> Any:
    try:
        q(main_c, f"CREATE BRANCH {name}")
    except mgclient.DatabaseError as e:
        if LICENSE_REFUSAL in str(e):
            raise Skipped(str(e)) from e
        raise BenchError(f"CREATE BRANCH {name} refused: {e}") from e
    c = connect(port)
    q(c, f"USE BRANCH {name}")
    return c


def time_mix(mix: str, main_c: Any, branches: list[Target], rounds: int, report: Report) -> None:
    log(f"timing mix={mix}")
    targets = [Target("main", main_c)] + branches
    n = len(targets)
    for qname, query in mix_queries(mix).items():
        for t in targets:  # warm-up: plan cache, page-in
            q(t.conn, query)
        times: dict[str, list[float]] = {t.name: [] for t in targets}
        ok = {t.name: True for t in branches}
        for r in range(rounds):
            order = [targets[(r + i) % n] for i in range(n)]
            if r % 2:
                order.reverse()
            round_rows: dict[str, list[tuple]] = {}
            for t in order:
                ms, round_rows[t.name] = timed(t.conn, query)
                times[t.name].append(ms)
            for t in branches:
                assert t.expect is not None
                if round_rows[t.name] != t.expect(qname, round_rows["main"]):
                    ok[t.name] = False
        line = f"{mix[:3]} {qname:22} main {statistics.median(times['main']):9.2f} ms |"
        for t in branches:
            paired = [tb / tm for tb, tm in zip(times[t.name], times["main"])]
            report.add(
                qname,
                t.level,
                statistics.median(times["main"]),
                statistics.median(times[t.name]),
                paired,
                ok[t.name],
                mix,
            )
            line += f" {t.name} x{statistics.median(paired):5.2f}{'' if ok[t.name] else ' WRONG'}"
        print(line, flush=True)

    if mix != "unrelated":
        return
    # Paired probe: main scans MainProbeNN, branch bNN scans Probe; same vertices, same count, no index either side.
    for t in branches:
        mq = f"MATCH (n:MainProbe{t.name[1:]}) RETURN count(n)"
        bq = "MATCH (n:Probe) RETURN count(n)"
        q(main_c, mq)
        q(t.conn, bq)
        tm: list[float] = []
        tb: list[float] = []
        ok_pair = True
        for r in range(rounds):
            pair = [(tm, main_c, mq), (tb, t.conn, bq)]
            if r % 2:
                pair.reverse()
            rows: dict[int, list[tuple]] = {}
            for acc, conn, qq in pair:
                ms, rows[id(acc)] = timed(conn, qq)
                acc.append(ms)
            ok_pair = ok_pair and rows[id(tm)] == rows[id(tb)] == [(t.changes["labels"],)]
        report.add(
            "label_probe",
            t.level,
            statistics.median(tm),
            statistics.median(tb),
            [y / x for x, y in zip(tm, tb)],
            ok_pair,
            mix,
        )


def run_levels(args: argparse.Namespace, srv: Server, report: Report, out: dict[str, Any]) -> None:
    levels = [float(x) for x in args.levels.split(",")]
    mixes = list(MIXES) if args.mix == "both" else [args.mix]
    dataset = ensure_dataset(Path(args.cache_directory))
    srv.start()
    main_c = connect(args.port)
    out["vertices"], edges = load_pokec(main_c, dataset)
    out["edges"] = len(edges)
    ids = sorted(r[0] for r in q(main_c, "MATCH (n:User) RETURN n.id"))
    first_out: dict[int, int] = {}
    for src, dst in edges:
        first_out.setdefault(src, dst)
    del edges
    branches: dict[str, list[Target]] = {mix: [] for mix in mixes}

    if "unrelated" in mixes:
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

    related_rows: dict[float, dict[str, list[tuple]]] = {}
    if "related" in mixes:
        queries = mix_queries("related")
        base_rows = {name: q(main_c, query) for name, query in queries.items()}
        users_before = q(main_c, "MATCH (n:User) RETURN count(n)")
        for lvl in levels:
            changes = related_changes(ids, first_out, lvl)
            t0 = time.perf_counter()
            related_rows[lvl] = related_expectations(args.port, changes, queries) if changes["touched"] else base_rows
            moved = [name for name in queries if related_rows[lvl][name] != base_rows[name]]
            log(
                f"{level_name(lvl, 'related')}: expected rows from main txn in {time.perf_counter() - t0:.1f} s; "
                f"queries seeing the change: {len(moved)}/{len(queries)}, unchanged: {sorted(set(queries) - set(moved))}"
            )
            if (
                q(main_c, "MATCH (n:User) RETURN count(n)") != users_before
                or q(main_c, queries["count_all"]) != base_rows["count_all"]
            ):
                raise BenchError(f"main changed after the rolled-back related mix at level {lvl}")
        for name, query in queries.items():
            if q(main_c, query) != base_rows[name]:
                raise BenchError(f"main result of {name} changed after the rolled-back related mixes")

    for mix in mixes:
        for lvl in levels:
            name = level_name(lvl, mix)
            c = create_branch(main_c, args.port, name)
            t0 = time.perf_counter()
            if mix == "related":
                changes = related_changes(ids, first_out, lvl)
                counts = apply_related(c, changes) if changes["touched"] else {k: 0 for k in changes}
                rows = related_rows[lvl]
                expect = lambda qname, _main, rows=rows: rows[qname]
            else:
                idx = select_indices(len(ids), lvl)
                counts = apply_mix(c, ids, idx) if idx else {"props": 0, "labels": 0, "edges": 0}
                expect = lambda qname, main_rows, counts=counts: expected(qname, main_rows, counts)
            out["branches"][name] = {
                "level": lvl,
                "mix": mix,
                "changes": counts,
                "apply_s": round(time.perf_counter() - t0, 2),
            }
            log(f"{name}: {counts} in {out['branches'][name]['apply_s']} s")
            branches[mix].append(Target(name, c, lvl, mix, counts, expect))

    for mix in mixes:
        time_mix(mix, main_c, branches[mix], args.rounds, report)


def write_summary(path: Path, report: Report, verdict: str, footer: str) -> None:
    lines = [f"### Graph Versioning branch reads vs main: {verdict}", "", footer, ""]
    for mix in MIXES:
        records = [r for r in report.records if r["mix"] == mix]
        if not records:
            continue
        levels = list(dict.fromkeys(r["level"] for r in records))
        queries = list(dict.fromkeys(r["query"] for r in records))
        by_key = {(r["query"], r["level"]): r for r in records}
        lines += [f"#### {mix} changes", ""]
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
        lines.append("")
    lines += [
        "Ratio = branch / main, per-round paired median. Budgets: "
        + ", ".join(f"x{v} at {int(k * 100)}%" for k, v in BUDGETS.items())
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
    ap.add_argument("--mix", choices=[*MIXES, "both"], default="both", help="which change mix(es) to benchmark")
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
        "mixes": list(MIXES) if args.mix == "both" else [args.mix],
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
        f"rounds={args.rounds} mixes={','.join(out['mixes'])} levels={levels_str} vertices={out['vertices']} edges={out['edges']} runtime_s={runtime}",
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
