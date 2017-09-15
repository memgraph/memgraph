MATCH (a), (b) WITH a, b UNWIND range(1, 100000) AS x CREATE (a)-[:Type]->(b)
