from typing import Set

import neo4j

MEMGRAPH_URL = "bolt://localhost:7687"

driver = neo4j.GraphDatabase.driver(MEMGRAPH_URL, auth=("", ""))


def fill_db(tx):
    res = tx.run("UNWIND range(1, 2000) AS i CREATE (n:Node {id: i}) RETURN n")


def run_queries(tx):
    big_result = tx.run("match (n) return n;")  # A query that forces the result to have has_more=true
    short_result = tx.run("match (n) return n limit 1;")  # Any query you can run
    print(f"Big result values: {big_result.values()}")
    print(f"Short result values: {short_result.values()}")


with driver.session() as session:
    tx = session.begin_transaction()
    fill_db(tx)
    run_queries(tx)
    tx.commit()
