import random
import time
from abc import ABC
from gqlalchemy import Memgraph
import os


class Worker(ABC):
    def __init__(self, worker):
        """Initialize worker with basic parameters."""
        self._name = worker["name"]
        self._query_host = worker.get("querying", {}).get("host", None)
        self._query_port = worker.get("querying", {}).get("port", None)
        self._database = worker.get("database", None)

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
        
        memgraph = Memgraph(self._query_host, self._query_port)
        if self._database:
            memgraph.execute(f"USE DATABASE {self._database}")

        for i in range(self._repetitions):
            print(f"Worker '{self._name}' executing query: {self._query}")
            memgraph.execute(self._query)

            if self._sleep_millis > 0:
                time.sleep(self._sleep_millis / 1000.0)

        print(f"Worker '{self._name}' finished.")


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
        if self._database:
            memgraph.execute(f"USE DATABASE {self._database}")

        for i in range(self._repetitions):
            query = random.choice(queries)
            print(f"Worker '{self._name}' executing query: {query}")
            memgraph.execute(query)

            if self._sleep_millis > 0:
                time.sleep(self._sleep_millis / 1000.0)

        print(f"Worker '{self._name}' finished.")
        
        
class CypherlIngest(Worker):
    """Executes a set of system queries randomly."""

    def __init__(self, worker):
        super().__init__(worker)
        self._path = worker["path"]
        
        if not os.path.exists(self._path):
            raise Exception(f"File not found: {self._path}, skipping...")


    def run(self):
        memgraph = Memgraph(self._query_host, self._query_port)
        if self._database:
            memgraph.execute(f"USE DATABASE {self._database}")
        
        # Read and execute Cypher queries line by line
        with open(self._path, "r", encoding="utf-8") as file:
            for line in file:
                line = line.strip()
                if line and not line.startswith("//"):  # Ignore empty lines and comments
                    memgraph.execute(line)

        print(f"Worker '{self._name}' finished.")    


def get_worker_object(worker) -> Worker:
    """Factory function to create the appropriate worker object."""
    worker_type = worker["type"]

    if worker_type == "reader" or worker_type == "writer":
        return BasicWorker(worker)
    if worker_type == "lab-simulator":
        return LabSimulator(worker)
    if worker_type == "cypherl-ingest":
        return CypherlIngest(worker)

    raise Exception(f"Unknown worker type: '{worker_type}'!")


def get_worker_steps(workers):
    """Extracts unique steps from worker configurations."""
    steps = [worker.get("step", 1) for worker in workers]

    for step in steps:
        if step <= 0:
            raise Exception(f"Step cannot be {step}!")

    return sorted(set(steps))
