import random
import time
from abc import ABC


class Worker(ABC):
    def run(self, deployment):
        pass


class BasicWorker(Worker):
    def __init__(self, worker):
        super().__init__()
        self._name = worker["name"]
        self._query = worker["query"]
        self._repetitions = worker["num_repetitions"]
        self._sleep_millis = worker["sleep_millis"]

    def run(self, deployment):
        """Function to be executed in a separate process for each worker."""
        print(f"Starting worker '{self._name}'...")

        for i in range(self._repetitions):
            deployment.execute_query(self._query)
            if self._sleep_millis > 0:
                time.sleep(self._sleep_millis / 1000.0)

        print(f"Worker '{self._name}' finished.")


class LabSimulator(Worker):
    def __init__(self, worker):
        super().__init__()
        self._name = worker["name"]
        self._repetitions = worker["num_repetitions"]
        self._sleep_millis = worker["sleep_millis"]

    def run(self, deployment):
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
            deployment.execute_query(query)
            if self._sleep_millis > 0:
                time.sleep(self._sleep_millis / 1000.0)

        print(f"Worker '{self._name}' finished.")


class RandomMultitenantWriter(Worker):
    def __init__(self, worker):
        super().__init__()
        self._name = worker["name"]
        self._query = worker["query"]
        self._repetitions = worker["num_repetitions"]
        self._sleep_millis = worker["sleep_millis"]

    def run(self, deployment):
        print(f"Starting worker '{self._name}'...")
        databases = [f"db{x}" for x in range(1, 101)]

        for i in range(self._repetitions):
            db = random.choice(databases)

            deployment.execute_query_for_tenant(db, self._query)

            if self._sleep_millis > 0:
                time.sleep(self._sleep_millis / 1000.0)

        print(f"Worker '{self._name}' finished.")


def get_worker_object(worker) -> Worker:
    type = worker["type"]
    if type == "reader":
        return BasicWorker(worker)
    if type == "writer":
        return BasicWorker(worker)
    if type == "lab-simulator":
        return LabSimulator(worker)
    if type == "random-multitenant-writer":
        return RandomMultitenantWriter(worker)
    raise Exception(f"Unknown worker type: '{type}'!")


def get_worker_steps(workers):
    steps = [worker.get("step", 1) for worker in workers]

    for step in steps:
        if step <= 0:
            raise Exception(f"Step can not be {step}!")

    return sorted(set(steps))
