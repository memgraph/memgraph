setup: |-
    CALL community_detection_online.reset() YIELD message;
    CREATE TRIGGER test_nodes_changed BEFORE COMMIT EXECUTE CALL community_detection_online.update(createdVertices, createdEdges, updatedVertices, updatedEdges, deletedVertices, deletedEdges) YIELD *;
queries:
    - |-
        MERGE (a: Node {id: 0}) MERGE (b: Node {id: 1}) CREATE (a)-[r: Relation]->(b);
        MERGE (a: Node {id: 0}) MERGE (b: Node {id: 2}) CREATE (a)-[r: Relation]->(b);
        MERGE (a: Node {id: 0}) MERGE (b: Node {id: 3}) CREATE (a)-[r: Relation]->(b);
        MERGE (a: Node {id: 1}) MERGE (b: Node {id: 2}) CREATE (a)-[r: Relation]->(b);
        MERGE (a: Node {id: 1}) MERGE (b: Node {id: 4}) CREATE (a)-[r: Relation]->(b);
        MERGE (a: Node {id: 2}) MERGE (b: Node {id: 3}) CREATE (a)-[r: Relation]->(b);
        MERGE (a: Node {id: 2}) MERGE (b: Node {id: 9}) CREATE (a)-[r: Relation]->(b);
        MERGE (a: Node {id: 3}) MERGE (b: Node {id: 13}) CREATE (a)-[r: Relation]->(b);
        MERGE (a: Node {id: 4}) MERGE (b: Node {id: 5}) CREATE (a)-[r: Relation]->(b);
        MERGE (a: Node {id: 4}) MERGE (b: Node {id: 6}) CREATE (a)-[r: Relation]->(b);
        MERGE (a: Node {id: 4}) MERGE (b: Node {id: 7}) CREATE (a)-[r: Relation]->(b);
        MERGE (a: Node {id: 4}) MERGE (b: Node {id: 8}) CREATE (a)-[r: Relation]->(b);
        MERGE (a: Node {id: 5}) MERGE (b: Node {id: 7}) CREATE (a)-[r: Relation]->(b);
        MERGE (a: Node {id: 5}) MERGE (b: Node {id: 8}) CREATE (a)-[r: Relation]->(b);
        MERGE (a: Node {id: 6}) MERGE (b: Node {id: 7}) CREATE (a)-[r: Relation]->(b);
        MERGE (a: Node {id: 6}) MERGE (b: Node {id: 8}) CREATE (a)-[r: Relation]->(b);
        MERGE (a: Node {id: 8}) MERGE (b: Node {id: 10}) CREATE (a)-[r: Relation]->(b);
        MERGE (a: Node {id: 9}) MERGE (b: Node {id: 10}) CREATE (a)-[r: Relation]->(b);
        MERGE (a: Node {id: 9}) MERGE (b: Node {id: 12}) CREATE (a)-[r: Relation]->(b);
        MERGE (a: Node {id: 9}) MERGE (b: Node {id: 13}) CREATE (a)-[r: Relation]->(b);
        MERGE (a: Node {id: 9}) MERGE (b: Node {id: 14}) CREATE (a)-[r: Relation]->(b);
        MERGE (a: Node {id: 10}) MERGE (b: Node {id: 11}) CREATE (a)-[r: Relation]->(b);
        MERGE (a: Node {id: 10}) MERGE (b: Node {id: 13}) CREATE (a)-[r: Relation]->(b);
        MERGE (a: Node {id: 10}) MERGE (b: Node {id: 14}) CREATE (a)-[r: Relation]->(b);
        MERGE (a: Node {id: 11}) MERGE (b: Node {id: 12}) CREATE (a)-[r: Relation]->(b);
        MERGE (a: Node {id: 11}) MERGE (b: Node {id: 13}) CREATE (a)-[r: Relation]->(b);
        MERGE (a: Node {id: 11}) MERGE (b: Node {id: 14}) CREATE (a)-[r: Relation]->(b);
        MERGE (a: Node {id: 12}) MERGE (b: Node {id: 14}) CREATE (a)-[r: Relation]->(b);
    - |-
        MATCH (a: Node {id: 9})-[r: Relation]->(b: Node {id: 12}) DELETE r;
        MATCH (a: Node {id: 9})-[r: Relation]->(b: Node {id: 14}) DELETE r;
        MATCH (a: Node {id: 10})-[r: Relation]->(b: Node {id: 13}) DELETE r;
        MERGE (a: Node {id: 0}) MERGE (b: Node {id: 13}) CREATE (a)-[r: Relation]->(b);
        MERGE (a: Node {id: 3}) MERGE (b: Node {id: 9}) CREATE (a)-[r: Relation]->(b);
        MERGE (a: Node {id: 10}) MERGE (b: Node {id: 12}) CREATE (a)-[r: Relation]->(b);
    - |-
        CREATE (n: Node {id: 15});
        MERGE (a: Node {id: 15}) MERGE (b: Node {id: 8}) CREATE (a)-[r: Relation]->(b);
        MERGE (a: Node {id: 15}) MERGE (b: Node {id: 10}) CREATE (a)-[r: Relation]->(b);
        MERGE (a: Node {id: 15}) MERGE (b: Node {id: 14}) CREATE (a)-[r: Relation]->(b);
        MERGE (a: Node {id: 2}) MERGE (b: Node {id: 4}) CREATE (a)-[r: Relation]->(b);
        MERGE (a: Node {id: 2}) MERGE (b: Node {id: 6}) CREATE (a)-[r: Relation]->(b);
        MERGE (a: Node {id: 1}) MERGE (b: Node {id: 7}) CREATE (a)-[r: Relation]->(b);
        MATCH (n: Node {id: 5}) DETACH DELETE n;
cleanup: |-
    DROP TRIGGER test_nodes_changed;
    CALL community_detection_online.reset() YIELD message;
