setup: |-
    CREATE TRIGGER pagerank_online BEFORE COMMIT EXECUTE CALL pagerank_online.update(createdVertices, createdEdges, deletedVertices, deletedEdges) YIELD *;
    CALL pagerank_online.set(100, 0.2) YIELD *;

queries:
    - |-
        MERGE (a:Node {id: 0}) MERGE (b:Node {id: 1}) CREATE (a)-[:RELATION]->(b);
        MERGE (a:Node {id: 1}) MERGE (b:Node {id: 2}) CREATE (a)-[:RELATION]->(b);
        MERGE (a:Node {id: 2}) MERGE (b:Node {id: 0}) CREATE (a)-[:RELATION]->(b);
        MERGE (a:Node {id: 3}) MERGE (b:Node {id: 3}) CREATE (a)-[:RELATION]->(b);
        MERGE (a:Node {id: 3}) MERGE (b:Node {id: 4}) CREATE (a)-[:RELATION]->(b);
        MERGE (a:Node {id: 3}) MERGE (b:Node {id: 5}) CREATE (a)-[:RELATION]->(b);
    - |-
        MERGE (a:Node {id: 4}) MERGE (b:Node {id: 6}) CREATE (a)-[:RELATION]->(b);
    - |-
        MATCH (n) DETACH DELETE n;

cleanup: |-
    CALL pagerank_online.reset() YIELD *;
    CALL mg.load('pagerank_online') YIELD *;
    DROP TRIGGER pagerank_online;
