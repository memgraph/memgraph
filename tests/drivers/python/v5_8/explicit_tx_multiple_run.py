from typing import Set

import neo4j

MEMGRAPH_URL = "bolt://localhost:7687"

driver = neo4j.GraphDatabase.driver(MEMGRAPH_URL, auth=("", ""))


def fill_db(tx):
    tx.run("UNWIND range(1, 2000) AS i CREATE (n:Node {id: i}) RETURN n")


def run_queries(tx):
    tx.run("match (n) return n;")  # A query that forces the result to have has_more=true
    tx.run("match (n) return n limit 1;")  # Any query you can run


with driver.session() as session:
    tx = session.begin_transaction()
    fill_db(tx)
    run_queries(tx)
    tx.commit()
