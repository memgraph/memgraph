import itertools
import logging
import re
import tempfile
from collections import defaultdict
from pathlib import Path

import mgclient
from datasets import load_dataset
from memgraph_server_context import memgraph_server


def execute_query(query):
    try:
        conn = mgclient.connect(host="127.0.0.1", port=7687)
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


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parents[2]
memgraph_binary = PROJECT_DIR / "build" / "memgraph"

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# def save_set_to_file(file_path, query_set):
#     """Helper function to save a set of queries to a file."""
#     with open(file_path, "w") as file:
#         for query in query_set:
#             file.write(query + "\n")


if __name__ == "__main__":
    tried_queres = 0
    passed_queries = 0
    failed_queries = 0
    number_of_restarts = 0

    # TODO check caching
    dataset = load_dataset("neo4j/text2cypher-2025v1")
    all_items_iter = itertools.chain(dataset["train"], dataset["test"])
    total = len(dataset["train"]) + len(dataset["test"])

    expected_failures = set()  # TODO: load from disk

    unexpected_passes = set()  # Track queries that passed unexpectedly
    new_failures = defaultdict(set)  # Track queries failures that failed unexpected

    error_patterns = {
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

    def classify_failure(query, msg):
        for category, pattern in error_patterns.items():
            if pattern.search(msg):
                new_failures[category].add(query)
                return
        new_failures[msg].add(query)

    while True:
        try:
            with tempfile.TemporaryDirectory() as data_directory, memgraph_server(
                memgraph_binary,
                Path(data_directory),
                7687,
                logger,
                ["--storage-gc-cycle-sec=9999", "--bolt-num-workers=1"],
            ) as server:
                for i, item in enumerate(all_items_iter):
                    tried_queres += 1
                    query = item["cypher"].replace("\\n", " ")
                    res, msg = execute_query(query)
                    if res:
                        passed_queries += 1
                        if query in expected_failures:
                            unexpected_passes.add(query)
                    else:
                        failed_queries += 1
                        if query not in expected_failures:
                            classify_failure(query, msg)

                    percent = (i + 1) / total * 100
                    print(f"\rProgress: {percent:.2f}% [{i + 1}/{total}]", end="", flush=True)
            break  # success
        except KeyboardInterrupt:
            # TODO: KeyboardInterrupt still not working ATM
            #      use `pkill -9 python3; pkill -9 memgraph` for now
            logger.warning("KeyboardInterrupt received. Shutting down gracefully.")
            break
        except Exception as e:
            number_of_restarts += 1
            logger.error(f"Failure: {e}")

    print()

    print(f"The number of tried queries: {tried_queres}")
    print(f"The number of passed queries: {passed_queries}")
    print(f"The number of failed queries: {failed_queries}")
    print(f"The number of memgraph restarts: {number_of_restarts}")

    for categorry, queries in new_failures.items():
        print(f"Category: {categorry}")
        for query in queries:
            print(f"\tFAIL: {query}")
    print(f"# of categories: {len(new_failures)}")
    # if unexpected_passes:
    #     print(f"Some queries passed unexpectedly: {len(unexpected_passes)}")

    # TODO NOW: if new failure or unxpected pass are non empty print here
    # TODO NOW: write out the set of expected failures to disk (which reflect what we just tested)
