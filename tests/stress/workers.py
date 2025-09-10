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
    RETRIABLE_WRITER = "retriable-writer"


class Worker(ABC):
    def __init__(self, worker):
        """Initialize worker with basic parameters."""
        self._name = worker["name"]
        self._worker_type = worker["type"]
        self._query_host = worker.get("querying", {}).get("host", None)
        self._query_port = worker.get("querying", {}).get("port", None)
        self._metrics = worker.get("metrics", [])

    def run(self):
        pass


class BasicWorker(Worker):
    """Executes a fixed query multiple times."""

    def __init__(self, worker):
        super().__init__(worker)
        self._query = worker["query"]
        self._repetitions = worker["num_repetitions"]
        self._sleep_millis = worker.get("sleep_millis", 0)

    def run(self):
        """Executes the assigned query in a loop."""
        print(f"Starting worker '{self._name}'...")
        start = time.time()
        memgraph = Memgraph(self._query_host, self._query_port)
        for i in range(self._repetitions):
            print(f"Worker '{self._name}' executing query: {self._query}")
            self._execute_query(memgraph, self._query)

            if self._sleep_millis > 0:
                time.sleep(self._sleep_millis / 1000.0)

        end = time.time()
        print(f"Worker '{self._name}' finished.")

        for metric in self._metrics:
            if metric == "throughput":
                throughput = self._repetitions / (end - start)
                print(f"Throughput: {throughput} QPS")
                continue
            if metric == "duration":
                duration = end - start
                print(f"Worker finished in {duration} seconds")
                continue
            raise Exception(f"Unknown worker metric {metric}")

    def _execute_query(self, memgraph, query):
        memgraph.execute(query)


class RetriableBasicWorker(BasicWorker):
    def __init__(self, worker):
        super().__init__(worker)

    def _execute_query(self, memgraph, query):
        while True:
            try:
                memgraph.execute(query)
                break
            except:
                print("Query failed, retrying...")
                time.sleep(0.1)


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

    def run(self):
        """Executes requests to get metrics in a loop and stores results."""
        print(f"Starting metrics worker '{self._name}'...")
        metrics_url = f"http://{self._query_host}:{self._query_port}"

        for _ in range(self._repetitions):
            try:
                # Use requests to get metrics
                print(f"Metrics worker '{self._name}' getting metrics...")
                response = requests.get(metrics_url, timeout=30)
                response.raise_for_status()  # Raises an HTTPError for bad responses

                # Parse the JSON response
                metrics_json = response.json()

                # Flatten dictionary and extract metrics provided from the configuration
                extracted_metrics = {}
                for value in metrics_json.values():
                    extracted_metrics.update({k: v for (k, v) in value.items() if k in self._metrics})
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
                "data": extracted_metrics,
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
    if worker_type == WorkerType.RETRIABLE_WRITER:
        return RetriableBasicWorker(worker)

    raise Exception(f"Unhandled worker type: '{worker_type}'!")


def get_worker_steps(workers):
    """Extracts unique steps from worker configurations."""
    steps = [worker.get("step", 1) for worker in workers]

    for step in steps:
        if step <= 0:
            raise Exception(f"Step cannot be {step}!")

    return sorted(set(steps))
