import sys
import threading

import pytest
from common import memgraph
from gqlalchemy import Memgraph


def execute_concurrent_write():
    mg = Memgraph()

    mg.execute("USING UNIQUE LOCK MATCH (n) MATCH (m) CREATE (n)-[:EDGE]->(m)")


def test_concurrent_write(memgraph):
    memgraph.execute("MATCH (n) DETACH DELETE n")
    memgraph.execute("UNWIND range(1, 1000) as x CREATE ()")

    t1 = threading.Thread(target=execute_concurrent_write)
    t2 = threading.Thread(target=execute_concurrent_write)

    t1.start()
    t2.start()

    t1.join()
    t2.join()

    assert [
        x["value"] for x in list(memgraph.execute_and_fetch("SHOW STORAGE INFO;")) if x["storage info"] == "edge_count"
    ][0] == 2000000


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
