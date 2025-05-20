import argparse
import itertools
import logging
import os
import re
import shutil
import tempfile
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, Set

import mgclient
from datasets import load_dataset
from memgraph_server_context import memgraph_server

RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RESET = "\033[0m"

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parents[2]
DEFAULT_MEMGRAPH_BINARY = PROJECT_DIR / "build" / "memgraph"
EXPECTATIONS = SCRIPT_DIR / "expectations"

DEFAULT_PORT = 7687
DEFAULT_GROUP_FILTER = r"^.*$"


def parse_args():
    parser = argparse.ArgumentParser(description="Run queries against Memgraph.")
    parser.add_argument(
        "--memgraph-binary", type=str, default=str(DEFAULT_MEMGRAPH_BINARY), help="Path to the Memgraph binary to use"
    )
    parser.add_argument(
        "--group-filter",
        type=str,
        default=DEFAULT_GROUP_FILTER,
        help="Regex pattern to filter which groups (by alias) to run (default: %(default)s)",
    )
    parser.add_argument(
        "--override-expectations",
        action="store_true",
        help="If set, overrides the expected failure set with the new one from this run.",
    )
    parser.add_argument("--list-groups", action="store_true", help="Only list available groups and exit.")
    parser.add_argument(
        "--port", type=int, default=DEFAULT_PORT, help="Port to use for Memgraph Bolt connection (default: %(default)s)"
    )

    return parser.parse_args()


def execute_query(query, port):
    try:
        conn = mgclient.connect(host="127.0.0.1", port=port)
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(query)
        cursor.fetchall()
        cursor.close()
        conn.close()
        return True, ""
    except mgclient.Error as e:
        return False, f"{e}"
    except:
        return False, ""


logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

ERROR_PATTERNS_MEMGRAPH: Dict[str, re.Pattern] = {
    "parse error": re.compile(r"^Error on line", re.IGNORECASE),
    "not implemented": re.compile(r"^Not yet implemented", re.IGNORECASE),
    "Invalid query": re.compile(r"^Invalid query", re.IGNORECASE),
    "Invalid types": re.compile(r"^Invalid types", re.IGNORECASE),
    "Parameter not provided": re.compile(r"^Parameter \$.*? not provided", re.IGNORECASE),
    "Unbound variable": re.compile(r"^Unbound variable", re.IGNORECASE),
    "Unknown key": re.compile(r"^Unknown key", re.IGNORECASE),
    "Missing function": re.compile(r"^Function '.*?' doesn't exist", re.IGNORECASE),
    "Redeclaring variable": re.compile(r"^Redeclaring variable", re.IGNORECASE),
    "There is no procedure named": re.compile(r"^There is no procedure named", re.IGNORECASE),
}


@dataclass
class Collector:
    unexpected_passes: set = field(default_factory=set)
    unexpected_failures: defaultdict = field(default_factory=lambda: defaultdict(set))
    tried_queries: int = 0
    passed_queries: int = 0
    failed_queries: int = 0
    number_of_restarts: int = 0

    def classify_error(self, msg: str) -> str:
        for category, pattern in ERROR_PATTERNS_MEMGRAPH.items():
            if pattern.search(msg):
                return category
        return msg  # Use the full message as fallback category

    def record(self, run: Callable[[str], tuple], raw_query: str, expected_failures: Set[str]):
        self.tried_queries += 1
        try:
            query, success, msg = run(raw_query)
            if success:
                self.passed_queries += 1
                if query in expected_failures:
                    self.unexpected_passes.add(query)
            else:
                self.failed_queries += 1
                if query not in expected_failures:
                    category = self.classify_error(msg)
                    self.unexpected_failures[category].add(query)
        except Exception:
            self.number_of_restarts += 1
            raise

    def summary(self):
        for categorry, queries in self.unexpected_failures.items():
            print(f"{YELLOW}Category: {categorry}{RESET}")
            for query in queries:
                print(f"{RED}FAIL: {query}{RESET}")
        for query in self.unexpected_passes:
            print(f"{GREEN}PASS: {query}{RESET}")

        # TODO NOW: if new failure or unxpected pass are non empty then
        #           write out the set of expected failures to disk (which reflect what we just tested)

        print(f"Total queries tried:     {self.tried_queries}")
        print(f"Queries passed:          {self.passed_queries}")
        print(f"Queries failed:          {self.failed_queries}")
        print(f"Memgraph restarts:       {self.number_of_restarts}")

    def collect_failures(self, old_failures: set) -> set:
        updated = old_failures - self.unexpected_passes
        for queries in self.unexpected_failures.values():
            updated.update(queries)
        return updated

    def has_unexpected(self) -> bool:
        return bool(self.unexpected_passes) or bool(self.unexpected_failures)


def load_expectations(file_path: str) -> set:
    try:
        with open(file_path, "r") as f:
            return {line.strip() for line in f if line.strip()}
    except FileNotFoundError:
        return set()


def save_expectations(file_path: Path, query_set):
    """Helper function to save a set of queries to a file."""
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, "w") as file:
        for query in query_set:
            file.write(query + "\n")


def run_queries(memgraph_binary, grouped_by_db, port, override_expectations):
    NEWLINE = re.compile(r"(\\n|\r\n|\r|\n)")

    def run(raw_query: str):
        query = re.sub(NEWLINE, " ", raw_query)
        result, errmsg = execute_query(query, port)
        return query, result, errmsg

    def for_group(group_name, extra, data_maker, queries):
        expectations_file = EXPECTATIONS / group_name
        expected_failures = load_expectations(expectations_file)

        collector = Collector()

        total = len(queries)
        while True:
            try:
                with data_maker() as data_directory, memgraph_server(
                    memgraph_binary, Path(data_directory), port, logger, extra
                ) as server:
                    for i, item in enumerate(queries):
                        percent = (i + 1) / total * 100
                        print(f"\rProgress: {percent:.2f}% [{i + 1}/{total}]", end="", flush=True)
                        collector.record(run, item["cypher"], expected_failures)
                print()
                break  # success
            except KeyboardInterrupt:
                # TODO: KeyboardInterrupt still not working ATM
                #      use `pkill -9 python3; pkill -9 memgraph` for now
                break

        collector.summary()

        if override_expectations and collector.has_unexpected():
            new_expectations = collector.collect_failures(expected_failures)
            save_expectations(expectations_file, new_expectations)

    for group, queries in grouped_by_db.items():
        if group != "empty_db":
            print(f"=== Running for: {group} ===")
            print(f"Skipping, due to lack of dataset")
            # TODO: download from git
            # neo4j dumps...need to import via cypherl
            # //   Path("/home/gareth/checkout/memgraph/data_here"),
            # ["--bolt-num-workers=1", "--data-recovery-on-startup=true"]
        else:
            print("=== Running for: Empty DB ===")

            @contextmanager
            def empty_graph_data_maker():
                with tempfile.TemporaryDirectory() as data_directory:
                    shutil.rmtree(data_directory)
                    os.makedirs(data_directory)
                    yield data_directory

            for_group("empty_db", ["--bolt-num-workers=1"], empty_graph_data_maker, queries)


def get_grouped_queries(group_filter: str = DEFAULT_GROUP_FILTER) -> Dict[str, list]:
    dataset = load_dataset("neo4j/text2cypher-2025v1", split="train+test")

    filter_regex = re.compile(group_filter)

    grouped_by_db = defaultdict(list)
    for item in dataset:
        db_alias = item.get("database_reference_alias") or "empty_db"
        if filter_regex.search(db_alias) is not None:
            grouped_by_db[db_alias].append(item)

    # TODO: add in our own regression queries

    return grouped_by_db


if __name__ == "__main__":
    args = parse_args()

    grouped_queries = get_grouped_queries(args.group_filter)
    run_queries(args.memgraph_binary, grouped_queries, args.port, args.override_expectations)
