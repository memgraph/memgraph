import json
import os
import random
import time
from abc import ABC
from datetime import datetime
from enum import Enum

from gqlalchemy import Memgraph

import requests


class WorkerType(Enum):
    """Enumeration of supported worker types."""

    READER = "reader"
    WRITER = "writer"
    LAB_SIMULATOR = "lab-simulator"
    METRICS = "metrics"


class Worker(ABC):
    def __init__(self, worker):
        """Initialize worker with basic parameters."""
        self._name = worker["name"]
        self._worker_type = worker["type"]
        self._query_host = worker.get("querying", {}).get("host", None)
        self._query_port = worker.get("querying", {}).get("port", None)

    def run(self):
        pass


class BasicWorker(Worker):
    """Executes a fixed query multiple times."""

    def __init__(self, worker):
        super().__init__(worker)
        self._query = worker["query"]
        self._repetitions = worker["num_repetitions"]
        self._sleep_millis = worker.get("sleep_millis", 0)
        self._worker_metrics = worker.get("metrics", [])

    def run(self):
        """Executes the assigned query in a loop."""
        print(f"Starting worker '{self._name}'...")
        start = time.time()
        memgraph = Memgraph(self._query_host, self._query_port)
        for i in range(self._repetitions):
            print(f"Worker '{self._name}' executing query: {self._query}")
            memgraph.execute(self._query)

            if self._sleep_millis > 0:
                time.sleep(self._sleep_millis / 1000.0)

        end = time.time()
        print(f"Worker '{self._name}' finished.")

        for metric in self._worker_metrics:
            if metric == "throughput":
                throughput = self._repetitions / (end - start)
                print(f"Throughput: {throughput} QPS")
                continue
            if metric == "duration":
                duration = end - start
                print(f"Worker finished in {duration} seconds")
                continue
            raise Exception(f"Unknown worker metric {metric}")


class LabSimulator(Worker):
    """Executes a set of system queries randomly."""

    def __init__(self, worker):
        super().__init__(worker)
        self._repetitions = worker["num_repetitions"]
        self._sleep_millis = worker.get("sleep_millis", 0)

    def run(self):
        print(f"Starting worker '{self._name}'...")

        queries = [
            "SHOW INDEX INFO;",
            "SHOW CONSTRAINT INFO;",
            "SHOW TRIGGERS;",
            "SHOW REPLICATION ROLE;",
            "SHOW REPLICAS;",
            "SHOW METRICS INFO;",
            "RETURN 1;",
            "SHOW STORAGE INFO;",
            "SHOW TRANSACTIONS;",
        ]

        memgraph = Memgraph(self._query_host, self._query_port)
        for i in range(self._repetitions):
            query = random.choice(queries)
            print(f"Worker '{self._name}' executing query: {query}")
            memgraph.execute(query)

            if self._sleep_millis > 0:
                time.sleep(self._sleep_millis / 1000.0)

        print(f"Worker '{self._name}' finished.")


class MetricsWorker(Worker):
    """Executes requests to get metrics every 5 seconds and stores results in an array."""

    def __init__(self, worker):
        super().__init__(worker)
        self._repetitions = worker["num_repetitions"]
        self._sleep_millis = worker.get("sleep_millis", 5000)  # Default 5 seconds
        self._metrics_data = []
        self._workload_name = worker.get("workload_name", "unknown")
        # Get metrics host from metrics section, fallback to query host, then localhost
        metrics_config = worker.get("metrics", {})
        self._metrics_host = metrics_config.get("host", self._query_host or "localhost")
        # Default metrics port is 9091
        self._metrics_port = metrics_config.get("port", 9091)

    def run(self):
        """Executes requests to get metrics in a loop and stores results."""
        print(f"Starting metrics worker '{self._name}'...")
        metrics_url = f"http://{self._metrics_host}:{self._metrics_port}"

        for i in range(self._repetitions):
            print(f"Worker '{self._name}' fetching metrics from {metrics_url} (iteration {i+1}/{self._repetitions})")

            try:
                # Use requests to get metrics
                response = requests.get(metrics_url, timeout=30)
                response.raise_for_status()  # Raises an HTTPError for bad responses

                # Parse the JSON response
                try:
                    metrics_json = response.json()
                except json.JSONDecodeError:
                    # If response is not valid JSON, store as text
                    metrics_json = response.text

            except requests.exceptions.Timeout:
                raise Exception("Error: Request timeout")
            except requests.exceptions.ConnectionError:
                raise Exception("Error: Connection failed")
            except requests.exceptions.HTTPError as e:
                raise Exception(f"Error: HTTP {e.response.status_code} - {e.response.reason}")
            except Exception as e:
                raise Exception(f"Error: {str(e)}")

            # Store the metrics data with timestamp and date
            current_time = time.time()
            metrics_entry = {
                "timestamp": current_time,
                "date": datetime.fromtimestamp(current_time).isoformat(),
                "iteration": i + 1,
                "data": metrics_json,
            }
            self._metrics_data.append(metrics_entry)

            if self._sleep_millis > 0:
                time.sleep(self._sleep_millis / 1000.0)

        print(f"Worker '{self._name}' finished. Collected {len(self._metrics_data)} metrics entries.")

        # Dump metrics to file
        self._dump_metrics_to_file()

    def _dump_metrics_to_file(self):
        """Dumps the collected metrics to a JSON file."""
        if not self._metrics_data:
            print(f"No metrics data to dump for worker '{self._name}'")
            return

        # Get the current directory (where the stress test is running from)
        script_dir = os.getcwd()

        metrics_file = os.path.join(script_dir, f"metrics_{self._workload_name}.json")

        try:
            current_time = time.time()
            with open(metrics_file, "w") as f:
                json.dump(
                    {
                        "workload_name": self._workload_name,
                        "timestamp": current_time,
                        "date": datetime.fromtimestamp(current_time).isoformat(),
                        "worker_name": self._name,
                        "metrics_data": self._metrics_data,
                    },
                    f,
                    indent=2,
                )
            print(f"Metrics data saved to: {metrics_file}")
        except Exception as e:
            print(f"Warning: Failed to save metrics to file: {e}")

    def dump_metrics_json(self):
        """Returns the collected metrics as a JSON string."""
        return json.dumps(self._metrics_data, indent=2, default=str)


def get_worker_object(worker) -> Worker:
    """Factory function to create the appropriate worker object."""
    worker_type_str = worker["type"]

    try:
        worker_type = WorkerType(worker_type_str)
    except ValueError:
        raise Exception(f"Unknown worker type: '{worker_type_str}'!")

    if worker_type in [WorkerType.READER, WorkerType.WRITER]:
        return BasicWorker(worker)
    if worker_type == WorkerType.LAB_SIMULATOR:
        return LabSimulator(worker)
    if worker_type == WorkerType.METRICS:
        return MetricsWorker(worker)

    raise Exception(f"Unhandled worker type: '{worker_type}'!")


def get_worker_steps(workers):
    """Extracts unique steps from worker configurations."""
    steps = [worker.get("step", 1) for worker in workers]

    for step in steps:
        if step <= 0:
            raise Exception(f"Step cannot be {step}!")

    return sorted(set(steps))
