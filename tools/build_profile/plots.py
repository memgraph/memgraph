#!/usr/bin/env python3
"""Render a tools/build_profile run as a single self-contained HTML page.

Charts, all on one shared, zoomable time axis:
  * memory vs build time: what the machine used above its pre-build baseline,
    next to the summed anon / RSS peaks of the steps running at that moment
  * running steps: compile / link / other over time
  * step timeline ("flame chart"): one row per concurrent slot, coloured by
    kind or by peak memory, hover for the step's numbers
  * memory vs wall time per step (scatter; anon by default, RSS selectable)

The page is plots_template.html with the run's data embedded, so the file can
be shared on its own; no dependencies beyond the browser.

    plots.py --steps steps.jsonl [--sysmon sysmon.jsonl] [--build-dir build] --out plots.html
"""
import argparse
import json
import os
import sys

sys.dont_write_bytecode = True  # keep __pycache__ out of the tools tree
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from report import merge_links  # noqa: E402
from report import anon_kb, cpu_s, label, load_jsonl, load_steps, mib, peak_kb, read_build_config, timeline_rows


def assign_lanes(steps):
    lanes = []
    for s in sorted(steps, key=lambda s: s["start"]):
        lane = next((i for i, end in enumerate(lanes) if end <= s["start"]), None)
        if lane is None:
            lane = len(lanes)
            lanes.append(0)
        lanes[lane] = s["end"]
        s["_lane"] = lane
    return len(lanes)


def build_data(meta, steps, samples, cfg):
    t0 = min(s["start"] for s in steps)
    t1 = max(s["end"] for s in steps)
    root = meta.get("cwd", "")
    n_lanes = assign_lanes(steps)
    step_rows = [
        {
            "n": label(s, root),
            "k": s["kind"],
            "g": s.get("target", ""),
            "x": s.get("exe", ""),
            "s": round(s["start"] - t0, 3),
            "d": round(max(s["wall_s"], 1e-3), 3),
            "p": round(mib(peak_kb(s)), 1),
            "a": round(mib(anon_kb(s)), 1),
            "c": round(cpu_s(s), 2),
            "e": s.get("exit", 0),
            "l": s["_lane"],
        }
        for s in steps
    ]
    sample_rows = []
    for r in timeline_rows(steps, samples, t0, t1):
        sample_rows.append(
            {
                "t": round(r["rel_s"], 3),
                "b": round(mib(r["build_kb"]), 1),
                "u": round(mib(r["used_kb"]), 1),
                "v": round(mib(r["available_kb"]), 1),
                "ad": round(mib(r["anon_demand_kb"]), 1),
                "rd": round(mib(r["rss_demand_kb"]), 1),
                "nc": r["compile"],
                "nl": r["link"],
                "no": r["other"],
                "cg": round(mib(r["cgroup_kb"]), 1) if r["cgroup_kb"] is not None else None,
            }
        )
    return {
        "title": f"memgraph build profile, {meta.get('host', '?')}, {meta.get('git_head', '?')}",
        "subtitle": " ".join(
            [
                f"build.sh {' '.join(meta.get('build_args', [])) or '(no args)'};",
                f"{cfg.get('CMAKE_BUILD_TYPE', '?')};",
                f"pools {', '.join(f'{k}={v}' for k, v in cfg.get('pools', {}).items()) or 'none'};",
                f"budgets compile {cfg.get('MG_MEMORY_PER_COMPILE_JOB_MB', '?')} / link"
                f" {cfg.get('MG_MEMORY_PER_LINK_JOB_MB', '?')} MiB;",
                f"ccache {meta.get('ccache', '?').split(' ')[0]}",
            ]
        ),
        "duration": round(t1 - t0, 3),
        "mem_total": round(mib(meta.get("mem_total_kb", 0)), 1),
        "budget_compile": int(cfg.get("MG_MEMORY_PER_COMPILE_JOB_MB", 0) or 0),
        "budget_link": int(cfg.get("MG_MEMORY_PER_LINK_JOB_MB", 0) or 0),
        "lanes": n_lanes,
        "steps": step_rows,
        "samples": sample_rows,
    }


HERE = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_PATH = os.path.join(HERE, "plots_template.html")


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--steps", required=True)
    ap.add_argument("--sysmon")
    ap.add_argument("--build-dir")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    meta, raw = load_steps(args.steps)
    steps = merge_links(raw)
    if not steps:
        sys.exit("plots.py: no steps in " + args.steps)
    data = build_data(meta, steps, load_jsonl(args.sysmon), read_build_config(args.build_dir))
    with open(TEMPLATE_PATH) as f:
        template = f.read()
    # The template's __TITLE__, __SUBTITLE__ and __DATA__ markers take the run's
    # text and its JSON; "</" is escaped so no label can close the script tag.
    html = (
        template.replace("__TITLE__", data["title"])
        .replace("__SUBTITLE__", data["subtitle"])
        .replace("__DATA__", json.dumps(data, separators=(",", ":")).replace("</", "<\\/"))
    )
    with open(args.out, "w") as f:
        f.write(html)
    return 0


if __name__ == "__main__":
    sys.exit(main())
