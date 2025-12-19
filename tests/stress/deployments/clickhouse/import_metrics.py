#!/usr/bin/env python3
"""
Import Prometheus metrics JSON export into ClickHouse.

Usage:
    python import_metrics.py <metrics_file.json> [--host localhost] [--port 8123]
"""

import argparse
import json
import sys
from datetime import datetime

import requests


def parse_args():
    parser = argparse.ArgumentParser(description="Import Prometheus metrics to ClickHouse")
    parser.add_argument("metrics_file", help="Path to the metrics JSON file")
    parser.add_argument("--host", default="localhost", help="ClickHouse host (default: localhost)")
    parser.add_argument("--port", default="8123", help="ClickHouse HTTP port (default: 8123)")
    parser.add_argument("--database", default="metrics", help="ClickHouse database (default: metrics)")
    return parser.parse_args()


def execute_query(host, port, query, data=None):
    """Execute a ClickHouse query via HTTP interface."""
    url = f"http://{host}:{port}/"

    if data:
        # For INSERT with data
        response = requests.post(url, params={"query": query}, data=data)
    else:
        response = requests.post(url, data=query)

    if response.status_code != 200:
        print(f"Error: {response.text[:500]}")
        return None
    return response.text


def escape_string(s):
    """Escape string for ClickHouse SQL."""
    if s is None:
        return ""
    return str(s).replace("\\", "\\\\").replace("'", "\\'")


def import_metrics(args):
    """Import metrics from JSON file to ClickHouse."""
    print(f"Loading metrics from {args.metrics_file}...")

    with open(args.metrics_file, "r") as f:
        data = json.load(f)

    # Handle direct Prometheus query result format
    if "status" in data and data["status"] == "success" and "data" in data:
        results = data["data"].get("result", [])
    elif "metrics" in data:
        # Our custom export format
        results = []
        for metric_entry in data["metrics"]:
            if "data" in metric_entry and "data" in metric_entry["data"]:
                results.extend(metric_entry["data"]["data"].get("result", []))
    else:
        print("Unknown JSON format")
        print(f"Keys found: {list(data.keys())}")
        return

    print(f"Found {len(results)} metric series to import")

    if not results:
        print("No metrics to import")
        return

    # Prepare rows for batch insert using TSV format (safer than SQL VALUES)
    rows = []
    skipped = 0

    for result in results:
        metric = result.get("metric", {})
        metric_name = metric.get("__name__", "unknown")
        instance = metric.get("instance", "")
        job = metric.get("job", "")
        labels = json.dumps(metric)

        # Handle both instant and range vectors
        if "value" in result:
            # Instant vector: [timestamp, value]
            ts, val = result["value"]
            try:
                timestamp = datetime.fromtimestamp(float(ts)).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                value = float(val)
                # TSV format: tab-separated values
                row = f"{timestamp}\t{metric_name}\t{instance}\t{job}\t{escape_string(labels)}\t{value}"
                rows.append(row)
            except (ValueError, TypeError, OSError) as e:
                skipped += 1
                continue
        elif "values" in result:
            # Range vector: [[timestamp, value], ...]
            for ts, val in result["values"]:
                try:
                    timestamp = datetime.fromtimestamp(float(ts)).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                    value = float(val)
                    row = f"{timestamp}\t{metric_name}\t{instance}\t{job}\t{escape_string(labels)}\t{value}"
                    rows.append(row)
                except (ValueError, TypeError, OSError) as e:
                    skipped += 1
                    continue

    if skipped > 0:
        print(f"Skipped {skipped} invalid entries")

    if not rows:
        print("No valid rows to insert")
        return

    print(f"Inserting {len(rows)} rows into ClickHouse...")

    # Insert using TSV format (much safer than SQL VALUES)
    query = f"INSERT INTO {args.database}.prometheus_timeseries FORMAT TabSeparated"
    data_payload = "\n".join(rows)

    # Insert in batches
    batch_size = 50000
    total_inserted = 0

    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        batch_data = "\n".join(batch)

        result = execute_query(args.host, args.port, query, batch_data)
        if result is not None:
            total_inserted += len(batch)
            print(f"  Inserted {total_inserted}/{len(rows)} rows...")
        else:
            print(f"  Failed to insert batch starting at row {i}")

    print(f"\nSuccessfully imported {total_inserted} metrics!")

    # Show summary
    summary_query = f"""
    SELECT
        metric_name,
        count() as count,
        min(timestamp) as first_ts,
        max(timestamp) as last_ts
    FROM {args.database}.prometheus_timeseries
    GROUP BY metric_name
    ORDER BY count DESC
    LIMIT 10
    """

    print("\nTop 10 metrics by count:")
    result = execute_query(args.host, args.port, summary_query)
    if result:
        print(result)


if __name__ == "__main__":
    args = parse_args()
    import_metrics(args)
