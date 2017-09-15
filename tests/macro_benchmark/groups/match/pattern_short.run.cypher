UNWIND range(0, 5) AS i MATCH (n)-[r]->(m) RETURN r SKIP 10000000
