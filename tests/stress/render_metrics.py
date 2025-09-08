#!/usr/bin/env python3
"""
Metrics Visualization Tool

This script reads a JSON metrics file and generates charts for all metrics found in the data.
Each metric gets its own chart with timestamps on the x-axis and metric values on the y-axis.

Usage:
    python render_metrics.py <metrics_file.json>

Example:
    python render_metrics.py metrics_write-write-conflicts.json
"""

import argparse
import json
import sys
from datetime import datetime
from typing import Any, Dict, List

import matplotlib

matplotlib.use("TkAgg")  # Use tkinter backend for display
from collections import defaultdict

import matplotlib.dates as mdates
import matplotlib.pyplot as plt


def parse_metrics_file(file_path: str) -> Dict[str, Any]:
    """Parse the JSON metrics file and return the data."""
    try:
        with open(file_path, "r") as f:
            data = json.load(f)
        return data
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in file '{file_path}': {e}")
        sys.exit(1)


def extract_metrics_data(metrics_data: List[Dict]) -> Dict[str, List[tuple]]:
    """
    Extract metrics data from the metrics_data array.
    Returns a dictionary where keys are metric names and values are lists of (timestamp, value) tuples.
    """
    metrics = defaultdict(list)

    for entry in metrics_data:
        timestamp_str = entry.get("date", "")
        data = entry.get("data", {})

        # Parse timestamp
        try:
            timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        except ValueError:
            print(f"Warning: Could not parse timestamp '{timestamp_str}', skipping entry")
            continue

        # Extract all metrics from the data dictionary
        for metric_name, value in data.items():
            if isinstance(value, (int, float)):
                metrics[metric_name].append((timestamp, value))

    return dict(metrics)


def create_charts(metrics: Dict[str, List[tuple]], workload_name: str) -> None:
    """Create, save, and display charts for all metrics."""
    if not metrics:
        print("No metrics data found to visualize.")
        return

    # Calculate subplot layout
    num_metrics = len(metrics)
    if num_metrics == 1:
        rows, cols = 1, 1
    elif num_metrics == 2:
        rows, cols = 1, 2
    elif num_metrics <= 4:
        rows, cols = 2, 2
    elif num_metrics <= 6:
        rows, cols = 2, 3
    elif num_metrics <= 9:
        rows, cols = 3, 3
    else:
        rows = (num_metrics + 2) // 3
        cols = 3

    # Create figure with subplots
    fig, axes = plt.subplots(rows, cols, figsize=(5 * cols, 4 * rows))
    fig.suptitle(f"Metrics Visualization - {workload_name}", fontsize=16, fontweight="bold")

    # Handle single subplot case
    if num_metrics == 1:
        axes = [axes]
    elif rows == 1:
        axes = axes if isinstance(axes, list) else [axes]
    else:
        axes = axes.flatten()

    # Create a chart for each metric
    for i, (metric_name, data_points) in enumerate(metrics.items()):
        if i >= len(axes):
            break

        ax = axes[i]

        # Sort data points by timestamp
        data_points.sort(key=lambda x: x[0])

        # Extract timestamps and values
        timestamps = [point[0] for point in data_points]
        values = [point[1] for point in data_points]

        # Plot the data
        ax.plot(timestamps, values, marker="o", linewidth=2, markersize=4)
        ax.set_title(metric_name, fontweight="bold")
        ax.set_xlabel("Time")
        ax.set_ylabel("Value")
        ax.grid(True, alpha=0.3)

        # Format x-axis to show time nicely
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
        ax.xaxis.set_major_locator(mdates.MinuteLocator(interval=1))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)

        # Add value annotations for key points
        if len(values) > 0:
            min_val = min(values)
            max_val = max(values)
            min_idx = values.index(min_val)
            max_idx = values.index(max_val)

            # Annotate min and max values
            ax.annotate(
                f"Min: {min_val}",
                xy=(timestamps[min_idx], min_val),
                xytext=(10, 10),
                textcoords="offset points",
                bbox=dict(boxstyle="round,pad=0.3", facecolor="lightgreen", alpha=0.7),
                fontsize=8,
            )

            if min_idx != max_idx:  # Only show max annotation if it's different from min
                ax.annotate(
                    f"Max: {max_val}",
                    xy=(timestamps[max_idx], max_val),
                    xytext=(10, -15),
                    textcoords="offset points",
                    bbox=dict(boxstyle="round,pad=0.3", facecolor="lightcoral", alpha=0.7),
                    fontsize=8,
                )

    # Hide unused subplots
    for i in range(num_metrics, len(axes)):
        axes[i].set_visible(False)

    # Adjust layout to prevent overlap
    plt.tight_layout()

    # Save charts to files
    import os

    output_dir = "metrics_charts"
    os.makedirs(output_dir, exist_ok=True)

    # Save as PNG
    output_file = os.path.join(output_dir, f"{workload_name}_metrics.png")
    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    print(f"Charts saved to: {output_file}")

    # Display the charts
    print("Displaying charts...")
    plt.show()


def main():
    """Main function to run the metrics visualization tool."""
    parser = argparse.ArgumentParser(
        description="Generate charts from metrics JSON file",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python render_metrics.py metrics_write-write-conflicts.json
  python render_metrics.py metrics_metrics-monitoring-basic.json
        """,
    )

    parser.add_argument("metrics_file", help="Path to the JSON metrics file")

    args = parser.parse_args()

    # Parse the metrics file
    print(f"Loading metrics from: {args.metrics_file}")
    data = parse_metrics_file(args.metrics_file)

    # Extract workload name
    workload_name = data.get("workload_name", "Unknown Workload")
    print(f"Workload: {workload_name}")

    # Extract metrics data
    metrics_data = data.get("metrics_data", [])
    if not metrics_data:
        print("No metrics data found in the file.")
        return

    print(f"Found {len(metrics_data)} data points")

    # Extract metrics
    metrics = extract_metrics_data(metrics_data)

    if not metrics:
        print("No valid metrics found to visualize.")
        return

    print(f"Found {len(metrics)} metrics: {', '.join(metrics.keys())}")

    # Create, save, and display charts
    print("Generating charts...")
    create_charts(metrics, workload_name)
    print("Charts saved and displayed successfully!")


if __name__ == "__main__":
    main()
