setup: |-
        MERGE (a: Node {id: 0}) MERGE (b: Node {id: 1}) CREATE (a)-[:RELATION]->(b);
        MERGE (a: Node {id: 0}) MERGE (b: Node {id: 2}) CREATE (a)-[:RELATION]->(b);
        MERGE (a: Node {id: 1}) MERGE (b: Node {id: 2}) CREATE (a)-[:RELATION]->(b);
        MERGE (a: Node {id: 2}) MERGE (b: Node {id: 3}) CREATE (a)-[:RELATION]->(b);
        MERGE (a: Node {id: 3}) MERGE (b: Node {id: 4}) CREATE (a)-[:RELATION]->(b);
        MERGE (a: Node {id: 3}) MERGE (b: Node {id: 5}) CREATE (a)-[:RELATION]->(b);
        CALL betweenness_centrality_online.set() YIELD *;
        CREATE TRIGGER test_no_parallelization BEFORE COMMIT EXECUTE CALL betweenness_centrality_online.update(createdVertices, createdEdges, deletedVertices, deletedEdges, false, 1) YIELD *;

queries:
    - |-
        MERGE (a: Node {id: 4}) MERGE (b: Node {id: 5}) CREATE (a)-[:RELATION]->(b);

cleanup: |-
    DROP TRIGGER test_no_parallelization;
    CALL mg.load('betweenness_centrality_online') YIELD *;
