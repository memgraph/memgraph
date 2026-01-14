setup: |-
        MERGE (a: Node {id: 0}) MERGE (b: Node {id: 1}) CREATE (a)-[:RELATION]->(b);
        MERGE (a: Node {id: 0}) MERGE (b: Node {id: 2}) CREATE (a)-[:RELATION]->(b);
        MERGE (a: Node {id: 1}) MERGE (b: Node {id: 2}) CREATE (a)-[:RELATION]->(b);
        MERGE (a: Node {id: 2}) MERGE (b: Node {id: 3}) CREATE (a)-[:RELATION]->(b);
        MERGE (a: Node {id: 3}) MERGE (b: Node {id: 4}) CREATE (a)-[:RELATION]->(b);
        MERGE (a: Node {id: 3}) MERGE (b: Node {id: 5}) CREATE (a)-[:RELATION]->(b);
        MERGE (a: Node {id: 4}) MERGE (b: Node {id: 5}) CREATE (a)-[:RELATION]->(b);
        CREATE (n: Node {id: 6});
        CALL betweenness_centrality_online.set() YIELD *;
        CREATE TRIGGER test_delete_node BEFORE COMMIT EXECUTE CALL betweenness_centrality_online.update(createdVertices, createdEdges, deletedVertices, deletedEdges) YIELD *;

queries:
    - |-
        MATCH (n: Node {id: 6}) DETACH DELETE n;

cleanup: |-
    DROP TRIGGER test_delete_node;
    CALL mg.load('betweenness_centrality_online') YIELD *;
