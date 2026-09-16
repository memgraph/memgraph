#!/usr/bin/env python3
"""Report on a tools/build_profile run.

Reads the per-step log written by step.py and, optionally, the machine-wide
samples from sysmon.py and the build directory's pool configuration, then prints
where the build's memory, CPU and wall time went. Can also emit every step as
CSV, a machine-readable JSON summary, and a Chrome trace (load it in
https://ui.perfetto.dev or chrome://tracing) that shows the steps on a timeline
against the memory they used.

    report.py --steps steps.jsonl [--sysmon sysmon.jsonl] [--build-dir build]
              [--top N] [--timeline-rows N] [--csv FILE] [--timeline-csv FILE]
              [--json FILE] [--trace FILE]
"""
import argparse
import csv
import json
import os
import re
import sys
from collections import defaultdict

KIB = 1024.0


# --- loading -----------------------------------------------------------------


def load_jsonl(path):
    records = []
    if not path or not os.path.exists(path):
        return records
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                sys.stderr.write(f"report.py: skipping malformed line in {path}\n")
    return records


# Tools a `cmake -P` script is handed through -D<NAME>=<path>; naming them says
# what the step really does (RewriteDtNeededAbi3.cmake is a patchelf run).
SCRIPT_TOOLS = ("patchelf", "objcopy", "llvm-objcopy", "strip", "llvm-strip", "ld", "lld")


def custom_exe(exe, cmd):
    """What a custom step really runs, seen past cmake's -E and -P front ends."""
    toks = cmd.split()
    if exe != "cmake" or len(toks) < 3:
        return exe
    if "-E" in toks:
        i = toks.index("-E")
        sub = toks[i + 1] if i + 1 < len(toks) else "?"
        if sub == "env":
            prog = next((t for t in toks[i + 2 :] if "=" not in t and not t.startswith("-")), None)
            return os.path.basename(prog) if prog else "cmake -E env"
        return f"cmake -E {sub}"
    if "-P" in toks:
        i = toks.index("-P")
        script = os.path.basename(toks[i + 1]) if i + 1 < len(toks) else "?"
        for t in toks:
            if t.startswith("-D") and "=" in t:
                tool = os.path.basename(t.split("=", 1)[1])
                if tool in SCRIPT_TOOLS:
                    return f"{tool} via {script}"
        return f"cmake -P {script}"
    return exe


def load_steps(path):
    meta = {}
    steps = []
    for rec in load_jsonl(path):
        if rec.get("type") == "meta":
            meta = rec
        else:
            if rec.get("kind") == "custom":
                rec["exe"] = custom_exe(rec.get("exe", ""), rec.get("cmd", ""))
            steps.append(rec)
    return meta, steps


def merge_links(steps):
    """A static-library link is several launched commands (rm, ar, ranlib); fold
    each link's parts into one step so the tables talk about the artifact."""
    merged = []
    by_output = {}
    for s in steps:
        if s["kind"] != "link":
            merged.append(s)
            continue
        key = (s["target"], s["output"])
        if key not in by_output:
            s = dict(s)
            s["parts"] = 1
            by_output[key] = s
            merged.append(s)
            continue
        m = by_output[key]
        if peak_kb(s) > peak_kb(m):
            m["exe"] = s["exe"]
        for k in ("wall_s", "user_s", "sys_s", "majflt", "minflt", "inblock", "oublock", "samples"):
            m[k] = m.get(k, 0) + s.get(k, 0)
        for k in ("maxrss_kb", "tree_rss_peak_kb", "tree_anon_peak_kb", "tree_procs_peak"):
            m[k] = max(m.get(k, 0), s.get(k, 0))
        m["start"] = min(m["start"], s["start"])
        m["end"] = max(m["end"], s["end"])
        m["exit"] = m["exit"] or s["exit"]
        m["parts"] += 1
    return merged


def read_build_config(build_dir):
    cfg = {}
    if not build_dir:
        return cfg
    cache = os.path.join(build_dir, "CMakeCache.txt")
    wanted = (
        "MG_MEMORY_PER_COMPILE_JOB_MB",
        "MG_MEMORY_PER_LINK_JOB_MB",
        "MG_LIMIT_PARALLELISM_BY_MEMORY",
        "CMAKE_BUILD_TYPE",
        "CMAKE_CXX_COMPILER",
        "CMAKE_CXX_COMPILER_LAUNCHER",
        "MG_ENABLE_TESTING",
        "MG_SPLIT_DEBUG",
    )
    if os.path.exists(cache):
        with open(cache) as f:
            for line in f:
                m = re.match(r"^([A-Za-z0-9_]+):[A-Z]+=(.*)$", line.strip())
                if m and m.group(1) in wanted:
                    cfg[m.group(1)] = m.group(2)
    cfg["pools"] = {}
    for name in ("build.ninja", os.path.join("CMakeFiles", "rules.ninja")):
        path = os.path.join(build_dir, name)
        if os.path.exists(path):
            with open(path) as f:
                cfg["pools"].update(re.findall(r"^pool (\S+)\n\s+depth = (\d+)", f.read(), re.M))
    return cfg


# --- helpers -----------------------------------------------------------------


def peak_kb(s):
    return max(s.get("maxrss_kb", 0), s.get("tree_rss_peak_kb", 0))


def anon_kb(s):
    """The step's own (anonymous) memory; falls back to the RSS peak when the
    tree sampler never ran."""
    return s.get("tree_anon_peak_kb") or peak_kb(s)


def cpu_s(s):
    return s.get("user_s", 0) + s.get("sys_s", 0)


def mib(kb):
    return kb / KIB


def fmt_mib(kb):
    return f"{mib(kb):,.0f}"


def fmt_s(sec):
    if sec >= 3600:
        return f"{sec / 3600:.1f}h"
    if sec >= 60:
        return f"{sec / 60:.1f}m"
    return f"{sec:.1f}s"


def percentile(values, p):
    if not values:
        return 0
    values = sorted(values)
    k = (len(values) - 1) * p / 100.0
    lo, hi = int(k), min(int(k) + 1, len(values) - 1)
    return values[lo] + (values[hi] - values[lo]) * (k - lo)


def relpath(path, root):
    if not path:
        return ""
    if root and os.path.isabs(path):
        try:
            rel = os.path.relpath(path, root)
            if not rel.startswith(".."):
                return rel
        except ValueError:
            pass
    return path


def label(s, root):
    if s["kind"] in ("compile", "scan"):
        return relpath(s.get("source") or s.get("output"), root)
    text = relpath(s.get("output"), root)
    if s["kind"] == "custom" and s.get("exe"):
        text = f"{text}  [{s['exe']}]"
    return text


def shorten(text, width):
    if len(text) <= width:
        return text
    return "…" + text[-(width - 1) :]


def table(headers, rows, numeric=None):
    """Plain-text table; columns listed in `numeric` are right-aligned."""
    numeric = set(numeric or ())
    rows = [[str(c) for c in r] for r in rows]
    widths = [len(h) for h in headers]
    for r in rows:
        for i, c in enumerate(r):
            widths[i] = max(widths[i], len(c))
    out = []

    def fmt(cells):
        parts = []
        for i, c in enumerate(cells):
            parts.append(c.rjust(widths[i]) if i in numeric else c.ljust(widths[i]))
        return "  ".join(parts).rstrip()

    out.append(fmt(headers))
    out.append("  ".join("-" * w for w in widths))
    out.extend(fmt(r) for r in rows)
    return "\n".join(out)


def heading(text):
    return f"\n== {text} " + "=" * max(0, 74 - len(text))


# --- analysis ----------------------------------------------------------------


def concurrency(steps):
    """Sweep the timeline: how many steps overlapped, and the largest sum of the
    running steps' peaks, which bounds the memory the build could have needed."""
    events = []
    for s in steps:
        events.append((s["start"], 1, s))
        events.append((s["end"], -1, s))
    events.sort(key=lambda e: (e[0], e[1]))
    running = {}
    max_count = defaultdict(int)
    worst = {"sum_kb": 0, "anon_kb": 0, "t": None, "steps": []}
    for t, delta, s in events:
        if delta > 0:
            running[id(s)] = s
        else:
            running.pop(id(s), None)
        counts = defaultdict(int)
        for r in running.values():
            counts[r["kind"]] += 1
        counts["all"] = len(running)
        for k, v in counts.items():
            max_count[k] = max(max_count[k], v)
        total = sum(peak_kb(r) for r in running.values())
        if total > worst["sum_kb"]:
            worst = {
                "sum_kb": total,
                "t": t,
                "anon_kb": sum(anon_kb(r) for r in running.values()),
                "steps": sorted(running.values(), key=peak_kb, reverse=True)[:8],
            }
    return dict(max_count), worst


def running_at(steps, times):
    """For each time in ascending `times`, the steps running then."""
    by_start = sorted(steps, key=lambda s: s["start"])
    active = {}
    i = 0
    out = []
    for t in times:
        while i < len(by_start) and by_start[i]["start"] <= t:
            active[id(by_start[i])] = by_start[i]
            i += 1
        for key in [k for k, s in active.items() if s["end"] < t]:
            del active[key]
        out.append(list(active.values()))
    return out


def timeline_rows(steps, samples, t_start, t_end):
    """One row per machine sample: what the machine saw next to what the
    running steps account for."""
    used = [(r["t"], r) for r in samples if "memtotal_kb" in r and "memavailable_kb" in r]
    before = [r["memtotal_kb"] - r["memavailable_kb"] for t, r in used if t < t_start]
    baseline = min(before) if before else (used[0][1]["memtotal_kb"] - used[0][1]["memavailable_kb"] if used else 0)
    during = [(t, r) for t, r in used if t_start - 1 <= t <= t_end + 1]
    running = running_at(steps, [t for t, _ in during])
    rows = []
    for (t, r), active in zip(during, running):
        u = r["memtotal_kb"] - r["memavailable_kb"]
        counts = defaultdict(int)
        for s in active:
            counts[s["kind"]] += 1
        rows.append(
            {
                "t": t,
                "rel_s": t - t_start,
                "used_kb": u,
                "build_kb": max(u - baseline, 0),
                "available_kb": r["memavailable_kb"],
                "cgroup_kb": r.get("cgroup_kb"),
                "anon_demand_kb": sum(anon_kb(s) for s in active),
                "rss_demand_kb": sum(peak_kb(s) for s in active),
                "compile": counts.get("compile", 0),
                "link": counts.get("link", 0),
                "other": len(active) - counts.get("compile", 0) - counts.get("link", 0),
                "cpu_busy": r.get("cpu_busy"),
                "psi_some_avg10": r.get("psi_some_avg10"),
            }
        )
    return rows


def machine_memory(samples, t_start, t_end):
    if not samples:
        return {}
    used = [
        (r["t"], r["memtotal_kb"] - r["memavailable_kb"])
        for r in samples
        if "memtotal_kb" in r and "memavailable_kb" in r
    ]
    if not used:
        return {}
    before = [u for t, u in used if t < t_start]
    baseline = min(before) if before else used[0][1]
    during = [(t, u) for t, u in used if t_start <= t <= t_end] or used
    peak_t, peak = max(during, key=lambda x: x[1])
    out = {
        "baseline_used_kb": baseline,
        "peak_used_kb": peak,
        "peak_t": peak_t,
        "build_peak_kb": peak - baseline,
        "memtotal_kb": samples[0].get("memtotal_kb", 0),
    }
    cg = [r["cgroup_kb"] for r in samples if "cgroup_kb" in r]
    if cg:
        out["cgroup_peak_kb"] = max(cg)
        out["cgroup_rise_kb"] = max(cg) - min(cg)
    swap = [(r["swaptotal_kb"] - r["swapfree_kb"]) for r in samples if "swaptotal_kb" in r]
    if swap:
        out["swap_rise_kb"] = max(swap) - min(swap)
    busy = [r["cpu_busy"] for r in samples if "cpu_busy" in r and t_start <= r["t"] <= t_end]
    if busy:
        out["cpu_busy_mean"] = sum(busy) / len(busy)
    psi = [r["psi_some_avg10"] for r in samples if "psi_some_avg10" in r]
    if psi:
        out["psi_some_avg10_max"] = max(psi)
    return out


def kind_rows(steps, key, top=None):
    groups = defaultdict(list)
    for s in steps:
        groups[key(s)].append(s)
    rows = []
    for name, group in groups.items():
        peaks = [peak_kb(s) for s in group]
        rows.append(
            [
                name,
                len(group),
                fmt_s(sum(s["wall_s"] for s in group)),
                fmt_s(sum(cpu_s(s) for s in group)),
                fmt_mib(max(peaks)),
                fmt_mib(percentile(peaks, 99)),
                fmt_mib(percentile(peaks, 90)),
                fmt_mib(percentile(peaks, 50)),
                fmt_mib(sum(peaks) / len(peaks)),
            ]
        )
    rows.sort(key=lambda r: -r[1])
    return rows[:top] if top else rows


def timeline_table(rows, wanted):
    """Condense per-sample rows into about `wanted` buckets, keeping each
    bucket's peak so a short spike is not averaged away."""
    if not rows:
        return []
    span = rows[-1]["rel_s"] - rows[0]["rel_s"]
    bucket_s = max(span / max(wanted, 1), 1e-3)
    buckets = defaultdict(list)
    for r in rows:
        buckets[int((r["rel_s"] - rows[0]["rel_s"]) / bucket_s)].append(r)
    scale = max(r["build_kb"] for r in rows) or 1
    out = []
    for key in sorted(buckets):
        group = buckets[key]
        peak = max(group, key=lambda r: r["build_kb"])
        bar = "#" * int(round(24 * peak["build_kb"] / scale))
        row = [
            f"+{fmt_s(max(group[0]['rel_s'], 0))}",
            fmt_mib(peak["build_kb"]),
            bar,
            fmt_mib(min(r["available_kb"] for r in group)),
            fmt_mib(max(r["anon_demand_kb"] for r in group)),
            fmt_mib(max(r["rss_demand_kb"] for r in group)),
            max(r["compile"] for r in group),
            max(r["link"] for r in group),
            max(r["other"] for r in group),
        ]
        if peak["cgroup_kb"] is not None:
            row.append(fmt_mib(max(r["cgroup_kb"] or 0 for r in group)))
        out.append(row)
    return out


def build_report(meta, steps, samples, cfg, top, timeline_rows_wanted=30):
    out = []
    root = meta.get("cwd", "")
    if not steps:
        return "No steps recorded."
    t_start = min(s["start"] for s in steps)
    t_end = max(s["end"] for s in steps)
    duration = t_end - t_start
    cpus = meta.get("cpus") or os.cpu_count() or 1
    total_cpu = sum(cpu_s(s) for s in steps)

    out.append(heading("Run"))
    info = [
        ("host", f"{meta.get('host', '?')}  ({cpus} cpus, {fmt_mib(meta.get('mem_total_kb', 0))} MiB RAM)"),
        ("git", f"{meta.get('git_head', '?')}{' (dirty)' if meta.get('git_dirty') else ''}"),
        (
            "command" if meta.get("runner") == "exec" else "build.sh args",
            " ".join(meta.get("build_args", [])) or "(none)",
        ),
        ("build type", cfg.get("CMAKE_BUILD_TYPE", "?")),
        ("compiler", os.path.basename(cfg.get("CMAKE_CXX_COMPILER", "?"))),
        ("ccache", meta.get("ccache", "?")),
        ("job pools", ", ".join(f"{k}={v}" for k, v in cfg.get("pools", {}).items()) or "none"),
        (
            "budgets",
            f"compile {cfg.get('MG_MEMORY_PER_COMPILE_JOB_MB', '?')} MiB, "
            f"link {cfg.get('MG_MEMORY_PER_LINK_JOB_MB', '?')} MiB per job",
        ),
        ("steps", f"{len(steps)} (first start to last end: {fmt_s(duration)})"),
        ("cpu time", f"{fmt_s(total_cpu)} in steps = {total_cpu / duration / cpus * 100:.0f}% of {cpus} cpus"),
    ]
    failed = [s for s in steps if s.get("exit")]
    if failed:
        info.append(("FAILED steps", str(len(failed))))
    width = max(len(k) for k, _ in info)
    out.extend(f"  {k.ljust(width)}  {v}" for k, v in info)

    hdr = ["kind", "steps", "wall Σ", "cpu Σ", "peak max", "p99", "p90", "p50", "mean"]
    num = range(1, 9)
    out.append(heading("Steps by kind (peak MiB per step)"))
    out.append(table(hdr, kind_rows(steps, lambda s: s["kind"]), num))

    def subkind(s):
        if s["kind"] == "compile":
            return f"compile/{s.get('language') or '?'}"
        if s["kind"] == "link":
            return f"link/{(s.get('target_type') or '?').lower()}"
        return f"{s['kind']}/{s.get('exe') or '?'}"

    out.append(heading("Steps by sub-kind"))
    out.append(table(hdr, kind_rows(steps, subkind, top), num))

    out.append(heading(f"Top {top} steps by peak memory"))
    rows = []
    for s in sorted(steps, key=peak_kb, reverse=True)[:top]:
        rows.append(
            [
                fmt_mib(peak_kb(s)),
                fmt_mib(s.get("tree_anon_peak_kb", 0)),
                fmt_s(s["wall_s"]),
                fmt_s(cpu_s(s)),
                s["kind"],
                s.get("target", ""),
                shorten(label(s, root), 70),
            ]
        )
    out.append(table(["peak MiB", "anon MiB", "wall", "cpu", "kind", "target", "step"], rows, (0, 1, 2, 3)))

    for kind in ("compile", "link"):
        group = sorted((s for s in steps if s["kind"] == kind), key=peak_kb, reverse=True)[:top]
        if not group:
            continue
        out.append(heading(f"Top {min(top, len(group))} {kind} steps by peak memory"))
        rows = [
            [
                fmt_mib(peak_kb(s)),
                fmt_mib(s.get("tree_anon_peak_kb", 0)),
                fmt_s(s["wall_s"]),
                fmt_s(cpu_s(s)),
                s.get("target", ""),
                shorten(label(s, root), 70),
            ]
            for s in group
        ]
        out.append(table(["peak MiB", "anon MiB", "wall", "cpu", "target", "step"], rows, (0, 1, 2, 3)))

    out.append(heading(f"Top {top} steps by wall time"))
    rows = []
    for s in sorted(steps, key=lambda s: s["wall_s"], reverse=True)[:top]:
        rows.append(
            [
                fmt_s(s["wall_s"]),
                fmt_s(cpu_s(s)),
                fmt_mib(peak_kb(s)),
                s["kind"],
                s.get("target", ""),
                shorten(label(s, root), 70),
            ]
        )
    out.append(table(["wall", "cpu", "peak MiB", "kind", "target", "step"], rows, (0, 1, 2)))

    out.append(heading(f"Top {top} targets by peak memory"))
    per_target = defaultdict(list)
    for s in steps:
        per_target[s.get("target") or "(custom)"].append(s)
    rows = []
    for name, group in per_target.items():
        heavy = max(group, key=peak_kb)
        rows.append(
            [
                fmt_mib(peak_kb(heavy)),
                len(group),
                fmt_s(sum(cpu_s(s) for s in group)),
                fmt_s(sum(s["wall_s"] for s in group)),
                name,
                shorten(label(heavy, root), 55),
            ]
        )
    rows.sort(key=lambda r: -float(r[0].replace(",", "")))
    out.append(table(["peak MiB", "steps", "cpu Σ", "wall Σ", "target", "heaviest step"], rows[:top], (0, 1, 2, 3)))

    out.append(heading("Concurrency and machine memory"))
    max_count, worst = concurrency(steps)
    out.append(
        "  max concurrent steps: "
        + ", ".join(f"{k}={v}" for k, v in sorted(max_count.items(), key=lambda kv: (kv[0] != "all", kv[0])))
    )
    out.append(
        f"  worst-case demand (sum of running steps' peaks): {fmt_mib(worst['sum_kb'])} MiB RSS,"
        f" {fmt_mib(worst['anon_kb'])} MiB anon, at +{fmt_s((worst['t'] or t_start) - t_start)}"
    )
    for s in worst["steps"][:5]:
        out.append(f"      {fmt_mib(peak_kb(s)):>8} MiB  {s['kind']:<8} {shorten(label(s, root), 60)}")
    mm = machine_memory(samples, t_start, t_end)
    if mm:
        out.append(
            f"  machine used memory (MemTotal - MemAvailable): baseline {fmt_mib(mm['baseline_used_kb'])} MiB,"
            f" peak {fmt_mib(mm['peak_used_kb'])} MiB at +{fmt_s(mm['peak_t'] - t_start)}"
            f" -> build added {fmt_mib(mm['build_peak_kb'])} MiB"
        )
        if "cgroup_peak_kb" in mm:
            out.append(
                f"  cgroup memory: peak {fmt_mib(mm['cgroup_peak_kb'])} MiB"
                f" (rose {fmt_mib(mm['cgroup_rise_kb'])} MiB over the run)"
            )
        if mm.get("swap_rise_kb"):
            out.append(f"  swap grew by {fmt_mib(mm['swap_rise_kb'])} MiB during the run")
        if "psi_some_avg10_max" in mm:
            out.append(f"  memory pressure (PSI some avg10) peaked at {mm['psi_some_avg10_max']:.1f}%")
        if "cpu_busy_mean" in mm:
            out.append(f"  machine CPU busy over the build: {mm['cpu_busy_mean'] * 100:.0f}%")
    else:
        out.append("  (no machine-wide samples)")

    rows = timeline_rows(steps, samples, t_start, t_end) if samples else []
    if rows:
        out.append(heading("Memory over time"))
        out.append(
            "  'build' is machine used memory (MemTotal - MemAvailable) above the pre-build baseline;"
            " 'anon/rss demand' sums the peaks of the steps running at that moment."
        )
        out.append(
            table(
                ["time", "build MiB", "", "avail MiB", "anon demand", "rss demand", "compile", "link", "other"]
                + (["cgroup MiB"] if rows[0]["cgroup_kb"] is not None else []),
                timeline_table(rows, timeline_rows_wanted),
                (1, 3, 4, 5, 6, 7, 8, 9),
            )
        )

    out.append(heading("Job-pool budgets vs. observed peaks"))
    for kind, key in (("compile", "MG_MEMORY_PER_COMPILE_JOB_MB"), ("link", "MG_MEMORY_PER_LINK_JOB_MB")):
        group = [s for s in steps if s["kind"] == kind]
        if not group:
            continue
        budget = cfg.get(key)
        budget = int(budget) if budget and budget.isdigit() else None
        out.append(f"  {kind} ({len(group)} steps, budget {budget if budget else '?'} MiB per job):")
        # anon is what each extra concurrent job costs; RSS is the conservative
        # single-job figure that also counts shared file-backed pages.
        for name, metric in (("anon", anon_kb), ("rss ", peak_kb)):
            values = [metric(s) for s in group]
            line = (
                f"      {name}  max {fmt_mib(max(values)):>7} MiB   p99 {fmt_mib(percentile(values, 99)):>7} MiB"
                f"   p90 {fmt_mib(percentile(values, 90)):>7} MiB   p50 {fmt_mib(percentile(values, 50)):>7} MiB"
            )
            if budget:
                over = [s for s in group if mib(metric(s)) > budget]
                line += f"   over budget: {len(over)}"
            out.append(line)
        if budget:
            over = sorted((s for s in group if mib(anon_kb(s)) > budget), key=anon_kb, reverse=True)
            for s in over[:5]:
                out.append(f"      {fmt_mib(anon_kb(s)):>8} MiB anon  {shorten(label(s, root), 60)}")

    cached = [s for s in steps if s.get("ccache") == "on"]
    if cached:
        # A hit never runs the compiler, so it is over in well under a second of CPU.
        hits = sum(1 for s in cached if cpu_s(s) < 0.5)
        out.append(heading("Caveat"))
        out.append(
            f"  {len(cached)} step(s) ran through an active ccache and {hits} look like cache hits;"
            " a hit records ccache's own footprint, not the compiler's."
            " Profile with ccache disabled for real numbers."
        )

    if failed:
        out.append(heading("Failed steps"))
        for s in failed[:top]:
            out.append(f"  exit {s['exit']:>3}  {s['kind']:<8} {shorten(label(s, root), 70)}")
    return "\n".join(out) + "\n"


# --- exports -----------------------------------------------------------------


def write_csv(path, steps, root):
    keys = [
        "kind",
        "target",
        "language",
        "target_type",
        "label",
        "exe",
        "ccache",
        "start",
        "end",
        "wall_s",
        "user_s",
        "sys_s",
        "peak_kb",
        "maxrss_kb",
        "tree_rss_peak_kb",
        "tree_anon_peak_kb",
        "tree_procs_peak",
        "samples",
        "majflt",
        "minflt",
        "inblock",
        "oublock",
        "nvcsw",
        "nivcsw",
        "exit",
        "parts",
        "source",
        "output",
        "cwd",
        "cmd",
    ]
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
        w.writeheader()
        for s in sorted(steps, key=lambda s: s["start"]):
            row = dict(s)
            row["label"] = label(s, root)
            row["peak_kb"] = peak_kb(s)
            w.writerow(row)


def write_timeline_csv(path, steps, samples, t_start, t_end):
    rows = timeline_rows(steps, samples, t_start, t_end)
    keys = [
        "t",
        "rel_s",
        "used_kb",
        "build_kb",
        "available_kb",
        "cgroup_kb",
        "anon_demand_kb",
        "rss_demand_kb",
        "compile",
        "link",
        "other",
        "cpu_busy",
        "psi_some_avg10",
    ]
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        for r in rows:
            r = dict(r)
            r["rel_s"] = round(r["rel_s"], 3)
            w.writerow(r)


def write_trace(path, steps, samples, root):
    """Chrome trace: one row ("thread") per concurrent slot, plus memory counters."""
    t0 = min(s["start"] for s in steps)
    events = []
    lanes = []  # end time of the step occupying each lane
    for s in sorted(steps, key=lambda s: s["start"]):
        lane = next((i for i, end in enumerate(lanes) if end <= s["start"]), None)
        if lane is None:
            lane = len(lanes)
            lanes.append(0)
        lanes[lane] = s["end"]
        events.append(
            {
                "name": label(s, root),
                "cat": s["kind"],
                "ph": "X",
                "ts": (s["start"] - t0) * 1e6,
                "dur": max(s["wall_s"], 1e-4) * 1e6,
                "pid": 1,
                "tid": lane + 1,
                "args": {
                    "peak_MiB": round(mib(peak_kb(s)), 1),
                    "anon_MiB": round(mib(s.get("tree_anon_peak_kb", 0)), 1),
                    "cpu_s": round(cpu_s(s), 2),
                    "target": s.get("target", ""),
                    "exe": s.get("exe", ""),
                    "exit": s.get("exit", 0),
                },
            }
        )
    for i in range(len(lanes)):
        events.append({"name": "thread_name", "ph": "M", "pid": 1, "tid": i + 1, "args": {"name": f"slot {i + 1:02d}"}})
    events.append({"name": "process_name", "ph": "M", "pid": 1, "args": {"name": "build steps"}})

    # Sum of running steps' peaks over time, as a counter track.
    marks = []
    for s in steps:
        marks.append((s["start"], peak_kb(s), anon_kb(s)))
        marks.append((s["end"], -peak_kb(s), -anon_kb(s)))
    marks.sort()
    total = anon = 0
    for t, d, a in marks:
        total += d
        anon += a
        events.append(
            {
                "name": "demand: sum of running steps' peaks",
                "ph": "C",
                "pid": 1,
                "ts": (t - t0) * 1e6,
                "args": {"rss MiB": round(mib(max(total, 0)), 1), "anon MiB": round(mib(max(anon, 0)), 1)},
            }
        )
    if samples:
        base = None
        for r in samples:
            if "memtotal_kb" not in r:
                continue
            used = r["memtotal_kb"] - r["memavailable_kb"]
            base = used if base is None else min(base, used) if r["t"] < t0 else base
            args = {"used MiB": round(mib(used), 1)}
            if "cgroup_kb" in r:
                args["cgroup MiB"] = round(mib(r["cgroup_kb"]), 1)
            events.append({"name": "machine memory", "ph": "C", "pid": 1, "ts": (r["t"] - t0) * 1e6, "args": args})
            if "cpu_busy" in r:
                events.append(
                    {
                        "name": "machine cpu busy %",
                        "ph": "C",
                        "pid": 1,
                        "ts": (r["t"] - t0) * 1e6,
                        "args": {"busy": round(r["cpu_busy"] * 100, 1)},
                    }
                )
    with open(path, "w") as f:
        json.dump({"traceEvents": events, "displayTimeUnit": "ms"}, f)


def summary_json(meta, steps, samples, cfg):
    t_start = min(s["start"] for s in steps)
    t_end = max(s["end"] for s in steps)
    max_count, worst = concurrency(steps)
    by_kind = {}
    for kind in sorted({s["kind"] for s in steps}):
        group = [s for s in steps if s["kind"] == kind]
        peaks = [peak_kb(s) for s in group]
        by_kind[kind] = {
            "steps": len(group),
            "wall_s": sum(s["wall_s"] for s in group),
            "cpu_s": sum(cpu_s(s) for s in group),
            "peak_max_kb": max(peaks),
            "peak_p99_kb": percentile(peaks, 99),
            "peak_p90_kb": percentile(peaks, 90),
            "peak_p50_kb": percentile(peaks, 50),
        }
    return {
        "meta": meta,
        "config": cfg,
        "duration_s": t_end - t_start,
        "steps": len(steps),
        "failed": sum(1 for s in steps if s.get("exit")),
        "by_kind": by_kind,
        "max_concurrent": max_count,
        "worst_case_demand_kb": worst["sum_kb"],
        "machine": machine_memory(samples, t_start, t_end),
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--steps", required=True, help="steps.jsonl written by step.py")
    ap.add_argument("--sysmon", help="sysmon.jsonl written by sysmon.py")
    ap.add_argument("--build-dir", help="build directory, for pool sizes and budgets")
    ap.add_argument("--top", type=int, default=25)
    ap.add_argument("--csv", help="write every step as CSV")
    ap.add_argument("--json", help="write a machine-readable summary")
    ap.add_argument("--trace", help="write a Chrome trace of the timeline")
    ap.add_argument("--timeline-csv", help="write machine memory vs. running steps, one row per sample")
    ap.add_argument("--timeline-rows", type=int, default=30, help="rows in the memory-over-time table")
    args = ap.parse_args()

    meta, raw = load_steps(args.steps)
    steps = merge_links(raw)
    samples = load_jsonl(args.sysmon)
    cfg = read_build_config(args.build_dir)
    root = meta.get("cwd", "")
    if not steps:
        sys.exit("report.py: no steps in " + args.steps)

    sys.stdout.write(build_report(meta, steps, samples, cfg, args.top, args.timeline_rows))
    if args.csv:
        write_csv(args.csv, steps, root)
    if args.timeline_csv and samples:
        write_timeline_csv(
            args.timeline_csv, steps, samples, min(s["start"] for s in steps), max(s["end"] for s in steps)
        )
    if args.trace:
        write_trace(args.trace, steps, samples, root)
    if args.json:
        with open(args.json, "w") as f:
            json.dump(summary_json(meta, steps, samples, cfg), f, indent=1, default=str)
    return 0


if __name__ == "__main__":
    sys.exit(main())
