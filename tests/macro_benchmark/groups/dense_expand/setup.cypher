UNWIND range(0, 499) AS i CREATE ();
MATCH (a), (b) CREATE (a)-[:Type]->(b);
