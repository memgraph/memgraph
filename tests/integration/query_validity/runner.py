import itertools
import logging
import tempfile
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
        return True
    except:
        return False


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parents[2]
memgraph_binary = PROJECT_DIR / "build" / "memgraph"

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    tried_queres = 0
    passed_queries = 0
    failed_queries = 0
    number_of_restarts = 0

    dataset = load_dataset("neo4j/text2cypher-2025v1")
    all_items_iter = itertools.chain(dataset["train"], dataset["test"])
    total = len(dataset["train"]) + len(dataset["test"])

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
                    query = "RETURN 1"  # item["cypher"].replace("\\n", " ")
                    print(f"Query: {query}", flush=True)
                    if execute_query(query):
                        passed_queries += 1
                    else:
                        failed_queries += 1

                    #
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

    print(f"The number of tried queries: {tried_queres}")
    print(f"The number of passed queries: {passed_queries}")
    print(f"The number of failed queries: {failed_queries}")
    print(f"The number of memgraph restarts: {number_of_restarts}")
