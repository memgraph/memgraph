from runners import Memgraph, Neo4j, Runners
from workload.dataset import Dataset


class Importer:
    def __init__(self, dataset: Dataset, vendor: Runners, size: str) -> None:
        if dataset.NAME == "LDBC_interactive" and isinstance(vendor, Neo4j):
            print("Runnning Neo4j import")
            print(dataset.URL_CSV[size])

        elif dataset.NAME == "LDBC_interactive" and isinstance(vendor, Memgraph):
            print("Running Memgraph import")
