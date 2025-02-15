import random
import time
from abc import ABC
from gqlalchemy import Memgraph


class Worker(ABC):
    def run(self, memgraph: Memgraph):
        """Abstract method to execute a worker task."""
        pass


class BasicWorker(Worker):
    """Executes a fixed query multiple times."""

    def __init__(self, worker):
        super().__init__()
        self._name = worker["name"]
        self._query = worker["query"]
        self._repetitions = worker["num_repetitions"]
        self._sleep_millis = worker.get("sleep_millis", 0)

    def run(self, memgraph: Memgraph):
        """Executes the assigned query in a loop."""
        print(f"Starting worker '{self._name}'...")

        for i in range(self._repetitions):
            try:
                print(f"Worker '{self._name}' executing query: {self._query}")
                memgraph.execute(self._query)
            except Exception as e:
                print(f"Error in worker '{self._name}': {e}")

            if self._sleep_millis > 0:
                time.sleep(self._sleep_millis / 1000.0)

        print(f"Worker '{self._name}' finished.")


class LabSimulator(Worker):
    """Executes a set of system queries randomly."""

    def __init__(self, worker):
        super().__init__()
        self._name = worker["name"]
        self._repetitions = worker["num_repetitions"]
        self._sleep_millis = worker.get("sleep_millis", 0)

    def run(self, memgraph: Memgraph):
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

        for i in range(self._repetitions):
            query = random.choice(queries)
            try:
                print(f"Worker '{self._name}' executing query: {query}")
                memgraph.execute(query)
            except Exception as e:
                print(f"Error in worker '{self._name}': {e}")

            if self._sleep_millis > 0:
                time.sleep(self._sleep_millis / 1000.0)

        print(f"Worker '{self._name}' finished.")


class QueryWorker(Worker):
    """Executes a set of predefined queries from the configuration."""

    def __init__(self, worker):
        super().__init__()
        self._name = worker["name"]
        self._queries = worker["queries"]  # List of queries
        self._repetitions = worker["num_repetitions"]
        self._sleep_millis = worker.get("sleep_millis", 0)

    def run(self, memgraph: Memgraph):
        print(f"Starting worker '{self._name}'...")

        for i in range(self._repetitions):
            for query in self._queries:
                try:
                    print(f"Worker '{self._name}' executing query: {query}")
                    memgraph.execute(query)
                except Exception as e:
                    print(f"Error in worker '{self._name}': {e}")

                if self._sleep_millis > 0:
                    time.sleep(self._sleep_millis / 1000.0)

        print(f"Worker '{self._name}' finished.")


def get_worker_object(worker) -> Worker:
    """Factory function to create the appropriate worker object."""
    worker_type = worker["type"]

    if worker_type == "reader" or worker_type == "writer":
        return BasicWorker(worker)
    if worker_type == "lab-simulator":
        return LabSimulator(worker)
    if worker_type == "query":
        return QueryWorker(worker)

    raise Exception(f"Unknown worker type: '{worker_type}'!")


def get_worker_steps(workers):
    """Extracts unique steps from worker configurations."""
    steps = [worker.get("step", 1) for worker in workers]

    for step in steps:
        if step <= 0:
            raise Exception(f"Step cannot be {step}!")

    return sorted(set(steps))
