#!/usr/bin/env python3
"""Read SessionHL pull stats and mgbench stats files, match by Run ID,
and plot normalized distributions of pull time (server) vs round-trip time (client)
so we can see how much longer the round trip usually takes.
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


def _parse_sessionhl_like_file(path: Path, header_pattern: str) -> dict[str, list[float]]:
    """Parse a SessionHL-style stats file. Returns {run_id: [time_1, time_2, ...]}."""
    data: dict[str, list[float]] = {}
    run_id = None
    current_times: list[float] = []
    header_re = re.compile(header_pattern)

    with open(path) as f:
        for line in f:
            line = line.rstrip("\n")
            m = header_re.match(line)
            if m:
                if run_id is not None and current_times:
                    data.setdefault(run_id, []).extend(current_times)
                run_id = m.group(1).strip()
                current_times = []
                continue
            m = re.match(r"^\s*Execution times \(seconds\):\s*(.+)\s*$", line)
            if m and run_id is not None:
                part = m.group(1).strip()
                if part:
                    current_times = []
                    for x in part.split(","):
                        try:
                            current_times.append(float(x.strip()))
                        except ValueError:
                            continue
                    # Ignore blocks with only one execution (no distribution to plot)
                    if len(current_times) > 1:
                        data.setdefault(run_id, []).extend(current_times)
                current_times = []
                continue

    if run_id is not None and current_times:
        data.setdefault(run_id, []).extend(current_times)

    return data


def parse_sessionhl_file(path: Path) -> dict[str, list[float]]:
    """Parse sessionhl_pull_stats.txt. Returns {run_id: [pull_time_1, ...]}."""
    return _parse_sessionhl_like_file(path, r"^=== Session Stats \(Run ID: ([^)]+)\) ===\s*$")


def parse_summary_times_file(path: Path) -> dict[str, list[float]]:
    """Parse sessionhl_summary_times.txt (parsing+planning+plan_execution sum). Returns {run_id: [time_1, ...]}."""
    return _parse_sessionhl_like_file(path, r"^=== Session Summary Times \(Run ID: ([^)]+)\) ===\s*$")


def parse_detailed_stats_file(path: Path) -> dict[str, list[float]]:
    """Parse mgbench detailed-stats file (round_trip or summary). Returns {run_id: [time_1, ...]}."""
    return _parse_sessionhl_like_file(path, r"^=== .+ \(Run ID: ([^)]+)\) ===\s*$")


def get_last_run_id_from_detailed_stats(path: Path) -> str | None:
    """Return the run_id of the last block in a detailed-stats file (most recent run)."""
    last_rid = None
    header_re = re.compile(r"^=== .+ \(Run ID: ([^)]+)\) ===\s*$")
    with open(path) as f:
        for line in f:
            m = header_re.match(line.rstrip("\n"))
            if m:
                last_rid = m.group(1).strip()
    return last_rid


def parse_mgbench_file(path: Path) -> dict[str, list[float]]:
    """Parse mgbench_stats.txt. Returns {run_id: [round_trip_time_1, ...]}."""
    data: dict[str, list[float]] = {}
    run_id = None

    with open(path) as f:
        for line in f:
            line = line.rstrip("\n")
            m = re.match(r"^=== Mgbench Stats \(Run ID: ([^)]+)\) ===\s*$", line)
            if m:
                run_id = m.group(1).strip()
                continue
            m = re.match(r"^\s*Execution times \(seconds\):\s*(.+)\s*$", line)
            if m and run_id is not None:
                part = m.group(1).strip()
                if part:
                    times = []
                    for x in part.split(","):
                        try:
                            times.append(float(x.strip()))
                        except ValueError:
                            continue
                    # Ignore queries run only once (no distribution to plot)
                    if len(times) > 1:
                        data.setdefault(run_id, []).extend(times)
                continue
            # New query block (next "Query:") resets run_id context only when we see next Run ID header
            # So we keep appending to same run_id until next === header

    return data


def main():
    parser = argparse.ArgumentParser(
        description="Plot normalized distributions of pull time vs round-trip time from stats files."
    )
    parser.add_argument(
        "--sessionhl",
        type=Path,
        default=Path("/tmp/sessionhl_pull_stats.txt"),
        help="Path to SessionHL pull stats file",
    )
    parser.add_argument(
        "--mgbench",
        type=Path,
        default=Path("/tmp/mgbench_stats.txt"),
        help="Path to mgbench stats file",
    )
    parser.add_argument(
        "--summary-times",
        type=Path,
        default=Path("/tmp/sessionhl_summary_times.txt"),
        help="Path to SessionHL summary times (parsing+planning+plan_execution) file; if present, plot for comparison",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=Path("/tmp/stats_distribution.png"),
        help="Output image path",
    )
    parser.add_argument(
        "--detailed-stats-dir",
        type=Path,
        default=None,
        metavar="DIR",
        help="If set, read round-trip and summary times from DIR/mgbench_round_trip_times.txt and "
        "DIR/mgbench_summary_times.txt (from mgbench --detailed-stats DIR); no sessionhl/mgbench needed",
    )
    parser.add_argument(
        "--bins",
        type=int,
        default=50,
        help="Number of histogram bins (default 50)",
    )
    parser.add_argument(
        "--ms",
        action="store_true",
        help="Display times in milliseconds (input files use seconds); default is to use ms when max value < 1",
    )
    parser.add_argument(
        "--run-id",
        type=str,
        default=None,
        metavar="ID",
        help="When multiple runs exist in the stats files, plot only this run_id. "
        "If not set, the run with the largest median round-trip time is used (avoids fast artifact runs).",
    )
    args = parser.parse_args()

    if args.detailed_stats_dir is not None:
        round_trip_path = args.detailed_stats_dir / "mgbench_round_trip_times.txt"
        summary_path = args.detailed_stats_dir / "mgbench_summary_times.txt"
        if not round_trip_path.exists():
            print(f"Error: Round-trip file not found: {round_trip_path}", file=sys.stderr)
            sys.exit(1)
        if not summary_path.exists():
            print(f"Error: Summary times file not found: {summary_path}", file=sys.stderr)
            sys.exit(1)
        pull_by_run = {}
        round_trip_by_run = parse_detailed_stats_file(round_trip_path)
        summary_times_by_run = parse_detailed_stats_file(summary_path)
        common_run_ids = set(round_trip_by_run) & set(summary_times_by_run)
        if not common_run_ids:
            print("No matching Run ID in both detailed-stats files.", file=sys.stderr)
            sys.exit(1)
        if args.run_id is not None:
            if args.run_id not in common_run_ids:
                print(
                    f"Error: --run-id {args.run_id} not found in both files. " f"Available: {sorted(common_run_ids)}",
                    file=sys.stderr,
                )
                sys.exit(1)
            run_ids_to_plot = [args.run_id]
        else:
            # Multiple runs: use the last run_id in the file (most recent run)
            last_rid = get_last_run_id_from_detailed_stats(round_trip_path)
            if last_rid is not None and last_rid in common_run_ids:
                run_ids_to_plot = [last_rid]
            else:
                run_ids_to_plot = [next(iter(common_run_ids))]
            if len(common_run_ids) > 1:
                print(
                    f"Multiple runs in files; plotting last run: {run_ids_to_plot[0]} "
                    f"(use --run-id to pick another).",
                    file=sys.stderr,
                )
        pull_times = []
        round_trip_times = []
        summary_times = []
        for rid in run_ids_to_plot:
            round_trip_times.extend(round_trip_by_run[rid])
            summary_times.extend(summary_times_by_run[rid])
        round_trip_times = [t for t in round_trip_times if t >= 0]
        summary_times = [t for t in summary_times if t >= 0]
        if not round_trip_times or not summary_times:
            print("No execution times to plot.", file=sys.stderr)
            sys.exit(1)
        # Plot only round-trip vs summary (no pull)
        pull_times = []
    else:
        if not args.sessionhl.exists():
            print(f"Error: SessionHL stats file not found: {args.sessionhl}", file=sys.stderr)
            sys.exit(1)
        if not args.mgbench.exists():
            print(f"Error: Mgbench stats file not found: {args.mgbench}", file=sys.stderr)
            sys.exit(1)
        pull_by_run = parse_sessionhl_file(args.sessionhl)
        round_trip_by_run = parse_mgbench_file(args.mgbench)
        summary_times_by_run = {}
        if args.summary_times.exists():
            summary_times_by_run = parse_summary_times_file(args.summary_times)
        common_run_ids = set(pull_by_run) & set(round_trip_by_run)
        if not common_run_ids:
            print(
                "No Run ID found in both files. Make sure you ran mgbench against Memgraph "
                "so the same run_id appears in both stats files.",
                file=sys.stderr,
            )
            print(f"  SessionHL run IDs: {list(pull_by_run.keys())[:5]}...", file=sys.stderr)
            print(f"  Mgbench run IDs:  {list(round_trip_by_run.keys())[:5]}...", file=sys.stderr)
            sys.exit(1)
        pull_times = []
        round_trip_times = []
        summary_times = []
        for rid in common_run_ids:
            pull_times.extend(pull_by_run[rid])
            round_trip_times.extend(round_trip_by_run[rid])
            if rid in summary_times_by_run:
                summary_times.extend(summary_times_by_run[rid])
        pull_times = [t for t in pull_times if t >= 0]
        round_trip_times = [t for t in round_trip_times if t >= 0]
        summary_times = [t for t in summary_times if t >= 0]
        if not pull_times or not round_trip_times:
            print("No execution times to plot.", file=sys.stderr)
            sys.exit(1)

    try:
        import matplotlib.pyplot as plt
        import numpy as np
    except ImportError as e:
        print(f"Error: matplotlib and numpy are required: {e}", file=sys.stderr)
        sys.exit(1)

    # All stats files use seconds; optionally display in ms for readability (e.g. when values are small)
    all_times = pull_times + round_trip_times + summary_times
    use_ms = args.ms or (all_times and max(all_times) < 1.0)
    scale = 1000.0 if use_ms else 1.0
    unit_label = "milliseconds" if use_ms else "seconds"
    pull_s = np.array(pull_times) * scale if pull_times else np.array([])
    round_trip_s = np.array(round_trip_times) * scale
    summary_s = np.array(summary_times) * scale if summary_times else np.array([])
    parts = [round_trip_s]
    if pull_times:
        parts.insert(0, pull_s)
    if summary_times:
        parts.append(summary_s)
    all_s = np.concatenate(parts)
    low = float(np.min(all_s))
    high = float(np.max(all_s))
    if low == high:
        low -= 1e-6 if use_ms else 1e-9
        high += 1e-6 if use_ms else 1e-9
    bins = np.linspace(low, high, args.bins + 1)

    fig, ax = plt.subplots(figsize=(10, 6))
    if pull_times:
        ax.hist(
            pull_s,
            bins=bins,
            density=True,
            alpha=0.6,
            label="Pull time (server)",
            color="C0",
            edgecolor="none",
        )
    if summary_times:
        ax.hist(
            summary_s,
            bins=bins,
            density=True,
            alpha=0.6,
            label="Summary times (parsing+planning+plan_exec)",
            color="C2",
            edgecolor="none",
        )
    ax.hist(
        round_trip_s,
        bins=bins,
        density=True,
        alpha=0.6,
        label="Round-trip time (mgbench)",
        color="C1",
        edgecolor="none",
    )
    ax.set_xlabel(f"Time ({unit_label})")
    ax.set_ylabel("Density (normalized)")
    ax.set_title("Execution time distribution: pull vs summary times vs round-trip")
    ax.legend(loc="upper right")
    ax.grid(True, alpha=0.3)

    # Vertical lines at medians (already scaled to display unit)
    rt_median = float(np.median(round_trip_s))
    ax.axvline(rt_median, color="C1", linestyle="--", linewidth=1, alpha=0.8)
    ylim = ax.get_ylim()[1]
    y_pos = 0.95
    median_fmt = f"{{:.2f}} {unit_label}" if use_ms else f"{{:.4f}} s"
    if pull_times:
        pull_median = float(np.median(pull_s))
        ax.axvline(pull_median, color="C0", linestyle="--", linewidth=1, alpha=0.8)
        ax.text(
            pull_median,
            ylim * y_pos,
            f"  pull median: {median_fmt.format(pull_median)}",
            color="C0",
            fontsize=8,
            va="top",
        )
        y_pos -= 0.07
    ax.text(
        rt_median,
        ylim * y_pos,
        f"  round-trip median: {median_fmt.format(rt_median)}",
        color="C1",
        fontsize=8,
        va="top",
    )
    y_pos -= 0.07
    if summary_times:
        summary_median = float(np.median(summary_s))
        ax.axvline(summary_median, color="C2", linestyle="--", linewidth=1, alpha=0.8)
        ax.text(
            summary_median,
            ylim * y_pos,
            f"  summary median: {median_fmt.format(summary_median)}",
            color="C2",
            fontsize=8,
            va="top",
        )
    lines = []
    if pull_times:
        pull_median = float(np.median(pull_s))
        gap = rt_median - pull_median
        lines.append(f"Median gap (RTT − pull): {gap:.2f} {unit_label}")
    if summary_times:
        rt_vs_summary = rt_median - float(np.median(summary_s))
        lines.append(f"RTT − summary: {rt_vs_summary:.2f} {unit_label}")
    if lines:
        ax.text(
            0.98,
            0.02,
            "\n".join(lines),
            transform=ax.transAxes,
            fontsize=9,
            ha="right",
            va="bottom",
            bbox=dict(boxstyle="round", facecolor="wheat", alpha=0.5),
        )

    fig.tight_layout()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(args.output, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {args.output}")


if __name__ == "__main__":
    main()
