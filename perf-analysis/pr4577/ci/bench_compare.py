#!/usr/bin/env python3
"""Compare a PR's `Release / Benchmark` mgbench throughput against the nightly master baseline.

The PR job runs mgbench once (no auth, ~40 queries) on `refs/pull/N/merge`; the Daily Benchmark
runs it 10x per night. bench-graph-api is internal, so both sides are parsed from GitHub job logs.

Baseline = median of the last --nights successful scheduled Daily Benchmark runs whose SHA is an
ancestor of the PR run's merge base (so an already-landed master regression is not blamed on the PR).

Exit: 0 PASS, 1 SUSPECT/REGRESSION, 2 no usable data.
"""
import argparse
import io
import json
import os
import re
import statistics as st
import subprocess
import sys
import time
import zipfile

REPO = "memgraph/memgraph"
CACHE = os.path.expanduser("~/.cache/mg-bench-compare")
ANSI = re.compile(r"\x1b\[[0-9;]*m")
# Cheap point queries: fixed per-query overhead shows here first (a 2-3% session-layer regression
# moved these while heavy traversals stayed flat).
POINT = (
    "single_vertex_read/",
    "single_vertex_write/",
    "single_edge_write/",
    "create/vertex/",
    "create/edge/",
    "create/pattern/",
    "match/vertex_on_property/",
    "vertex_on_label_property_index/",
    "pattern_long/",
    "expansion_1_with_filter/",
)
# Single-iteration PR runs of an unchanged master scatter about +-1% on the POINT median, +-2-3% per query.
SUSPECT, REGRESSION, PER_QUERY, NOISY = -0.015, -0.03, -0.06, 0.05


def gh(*args, raw=False, attempts=3):
    for i in range(attempts):
        r = subprocess.run(["gh", *args], capture_output=True)
        if r.returncode == 0:
            return r.stdout if raw else r.stdout.decode()
        if i + 1 < attempts:
            time.sleep(5 * (i + 1))  # log downloads of ~300MB nightly runs fail transiently
    raise RuntimeError(f"gh {' '.join(args)} failed: {r.stderr.decode().strip()}")


def gh_json(path, jq=None):
    args = ["api", path] + (["-q", jq] if jq else [])
    return json.loads(gh(*args))


def strip(raw):
    return ANSI.sub("", raw[29:] if raw[:2] == "20" and raw[4:5] == "-" else raw).rstrip("\n")


def parse_pr_log(text):
    """-> ({query: qps}, base_sha, head_sha) from the PR benchmark job log (mgbench step only)."""
    res, step, q, base, head = {}, None, None, None, None
    for raw in text.splitlines():
        line = strip(raw)
        if base is None and (m := re.search(r"HEAD is now at [0-9a-f]+ Merge ([0-9a-f]+) into ([0-9a-f]+)", line)):
            head, base = m.group(1), m.group(2)
        if line.startswith("##[group]Run "):
            step = ""
            continue
        if step == "" and (m := re.search(r"test-memgraph (\S+)", line)):
            step = m.group(1)
        if step != "mgbench":
            continue
        if m := re.search(r"Running query:(\S+)", line):
            q = m.group(1)
        elif (m := re.search(r"Throughput: ([0-9.]+) QPS", line)) and q:
            res.setdefault(q, float(m.group(1)))
            q = None
    return res, base, head


def parse_nightly_log(text):
    """-> {query: [qps per loop iteration]} for the nightly `mgbench` step."""
    res, step, pending, q = {}, None, 0, None
    for raw in text.splitlines():
        line = strip(raw)
        if line.startswith("##[group]Run ./tools/ci/loop-benchmark.sh"):
            pending = 1
            continue
        if pending:
            pending += 1
            if pending == 4:  # 2nd script arg = benchmark name
                step, pending = line.strip().rstrip("\\").strip(), 0
            continue
        if line.startswith("##[group]Run "):
            step = None
            continue
        if step != "mgbench":
            continue
        if m := re.search(r"Running query:(\S+)", line):
            q = m.group(1)
        elif (m := re.search(r"Throughput: ([0-9.]+) QPS", line)) and q:
            res.setdefault(q, []).append(float(m.group(1)))
            q = None
    return res


def nightly(run_id):
    os.makedirs(CACHE, exist_ok=True)
    path = os.path.join(CACHE, f"nightly-{run_id}.v2.json")
    if os.path.exists(path):
        return json.load(open(path))
    z = zipfile.ZipFile(io.BytesIO(gh("api", f"repos/{REPO}/actions/runs/{run_id}/logs", raw=True)))
    name = next(n for n in z.namelist() if "/" not in n and n.endswith(".txt"))
    data = parse_nightly_log(z.read(name).decode(errors="replace"))
    json.dump(data, open(path, "w"))
    return data


def is_ancestor(repo, a, b):
    if not repo:
        return True
    r = subprocess.run(["git", "-C", repo, "merge-base", "--is-ancestor", a, b], capture_output=True)
    return r.returncode == 0 if r.returncode in (0, 1) else True  # unknown object -> don't filter


def find_pr_job(pr, run_id):
    """Latest successful benchmark job of the PR's Diff pull_request runs (or of --run-id)."""
    if run_id:
        runs = [gh_json(f"repos/{REPO}/actions/runs/{run_id}")]
    else:
        branch = gh("pr", "view", str(pr), "-R", REPO, "--json", "headRefName", "-q", ".headRefName").strip()
        runs = json.loads(
            gh(
                "run",
                "list",
                "-R",
                REPO,
                "--workflow",
                "diff.yaml",
                "--branch",
                branch,
                "-L",
                "30",
                "--json",
                "databaseId,headSha,createdAt,event",
            )
        )
        runs = [
            {"id": r["databaseId"], "head_sha": r["headSha"], "created_at": r["createdAt"]}
            for r in runs
            if r["event"] == "pull_request"
        ]
    for r in runs:
        jobs = gh_json(f"repos/{REPO}/actions/runs/{r['id']}/jobs?per_page=100")["jobs"]
        for j in jobs:
            if j["name"].endswith("Benchmark") and j["conclusion"] == "success":
                return r, j
    return None, None


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("pr", type=int)
    ap.add_argument("--run-id", type=int, help="a specific Diff run instead of the latest one")
    ap.add_argument("--nights", type=int, default=3)
    ap.add_argument("--repo", default=".", help="memgraph checkout for the ancestry filter ('' to skip)")
    a = ap.parse_args()
    repo = a.repo if a.repo and os.path.exists(os.path.join(a.repo, ".git")) else ""

    run, job = find_pr_job(a.pr, a.run_id)
    if not job:
        print(
            f"NO DATA: PR #{a.pr} has no successful `Release / Benchmark` job. Is the "
            "`CI -build=release -test=benchmark` label set, and was a commit pushed after setting it?"
        )
        return 2
    pr_q, base, head = parse_pr_log(gh("api", f"repos/{REPO}/actions/jobs/{job['id']}/logs"))
    if not pr_q:
        print(f"NO DATA: benchmark job {job['id']} has no mgbench throughput lines")
        return 2
    pr_head = gh("pr", "view", str(a.pr), "-R", REPO, "--json", "headRefOid", "-q", ".headRefOid").strip()

    if repo and base:
        subprocess.run(["git", "-C", repo, "fetch", "-q", "origin", "master"], capture_output=True)
    nights = json.loads(
        gh(
            "run",
            "list",
            "-R",
            REPO,
            "--workflow",
            "daily_benchmark.yaml",
            "--event",
            "schedule",
            "-L",
            "40",
            "--json",
            "databaseId,headSha,createdAt,conclusion",
        )
    )
    nights = sorted((n for n in nights if n["conclusion"] == "success"), key=lambda n: n["createdAt"], reverse=True)
    chosen = [n for n in nights if not base or is_ancestor(repo, n["headSha"], base)][: a.nights]
    if not chosen:
        print("NO DATA: no nightly master baseline found")
        return 2
    base_runs = [nightly(n["databaseId"]) for n in chosen]
    common = [q for q in pr_q if all(q in b for b in base_runs)]
    master = {q: st.median(st.median(b[q]) for b in base_runs) for q in common}
    # Within-night spread: a query that scatters more than NOISY run-to-run can't be judged on one PR sample.
    cv = {q: st.median(st.pstdev(b[q]) / st.mean(b[q]) for b in base_runs) for q in common}
    noisy = {q for q in common if cv[q] > NOISY}
    ratio = {q: pr_q[q] / master[q] - 1 for q in common}
    point = [ratio[q] for q in common if any(p in q for p in POINT)]
    pmed, amed = st.median(point) if point else 0.0, st.median(ratio.values())
    worst = sorted(ratio.items(), key=lambda kv: kv[1])
    bad = [(q, r) for q, r in worst if r <= PER_QUERY and q not in noisy]
    verdict = "REGRESSION" if pmed <= REGRESSION or len(bad) >= 2 else "SUSPECT" if pmed <= SUSPECT or bad else "PASS"

    print(f"## mgbench vs nightly master — {verdict}\n")
    print(
        f"- PR #{a.pr} Diff run {run['id']}, job {job['id']} on `{job.get('runner_name')}`, "
        f"merge of `{(head or '?')[:9]}` into master `{(base or '?')[:9]}`"
    )
    if head and not pr_head.startswith(head[:9]):
        print(f"- ⚠️ benchmarked head `{head[:9]}` is not the current PR head `{pr_head[:9]}` — push to re-run")
    print(
        f"- Baseline: median of {len(chosen)} nightly runs "
        + ", ".join(f"{n['createdAt'][:10]} `{n['headSha'][:9]}`" for n in chosen)
    )
    print(
        f"- **Point-query median {pmed * 100:+.1f}%** ({len(point)} queries), all-query median "
        f"{amed * 100:+.1f}% ({len(common)} queries). Thresholds: suspect ≤{SUSPECT * 100:.1f}%, "
        f"regression ≤{REGRESSION * 100:.0f}%, single query ≤{PER_QUERY * 100:.0f}%.\n"
    )
    print("| query | PR QPS | master QPS | Δ | nightly CV |\n|---|---:|---:|---:|---:|")
    for q, r in worst[:8] + [("…", None)] + worst[-3:]:
        if r is None:
            print("| … | | | | |")
            continue
        name = "/".join(q.split("/")[:2]) + (" (noisy)" if q in noisy else "")
        print(f"| {name} | {pr_q[q]:.0f} | {master[q]:.0f} | {r * 100:+.1f}% | {cv[q] * 100:.1f}% |")
    if verdict != "PASS":
        print(
            "\nPR runs are single-iteration: re-run once (push an empty commit) before concluding; "
            "a repeat at the same level is a real regression."
        )
    return 0 if verdict == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
