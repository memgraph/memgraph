setup: |-
    CREATE TRIGGER katz_online BEFORE COMMIT EXECUTE CALL katz_centrality_online.update(createdVertices, createdEdges, deletedVertices, deletedEdges) YIELD node, rank SET node.katz = rank;

queries:
    - |-
        CREATE (a:Node {id: 0});
        CREATE (a:Node {id: 1});
        CREATE (a:Node {id: 2});
        CREATE (a:Node {id: 3});
        CREATE (a:Node {id: 4});
        CREATE (a:Node {id: 5});
        CREATE (a:Node {id: 6});
        MATCH (a:Node {id: 0}) MATCH (b:Node {id: 5}) CREATE (a)-[:RELATION]->(b);
        MATCH (a:Node {id: 0}) MATCH (b:Node {id: 1}) CREATE (a)-[:RELATION]->(b);
        MATCH (a:Node {id: 1}) MATCH (b:Node {id: 4}) CREATE (a)-[:RELATION]->(b);
        MATCH (a:Node {id: 2}) MATCH (b:Node {id: 1}) CREATE (a)-[:RELATION]->(b);
        MATCH (a:Node {id: 2}) MATCH (b:Node {id: 6}) CREATE (a)-[:RELATION]->(b);
        MATCH (a:Node {id: 3}) MATCH (b:Node {id: 1}) CREATE (a)-[:RELATION]->(b);
        MATCH (a:Node {id: 3}) MATCH (b:Node {id: 4}) CREATE (a)-[:RELATION]->(b);
        MATCH (a:Node {id: 4}) MATCH (b:Node {id: 0}) CREATE (a)-[:RELATION]->(b);
        MATCH (a:Node {id: 4}) MATCH (b:Node {id: 3}) CREATE (a)-[:RELATION]->(b);
        MATCH (a:Node {id: 4}) MATCH (b:Node {id: 1}) CREATE (a)-[:RELATION]->(b);
        MATCH (a:Node {id: 5}) MATCH (b:Node {id: 0}) CREATE (a)-[:RELATION]->(b);
        MATCH (a:Node {id: 5}) MATCH (b:Node {id: 4}) CREATE (a)-[:RELATION]->(b);
        MATCH (a:Node {id: 6}) MATCH (b:Node {id: 0}) CREATE (a)-[:RELATION]->(b);
    - |-
        MATCH (a:Node {id: 6}) DETACH DELETE a;
    - |-
        MATCH (a:Node {id: 0}) MATCH (b:Node {id: 2}) CREATE (a)-[:RELATION]->(b);
        MATCH (a:Node {id: 3}) MATCH (b:Node {id: 2}) CREATE (a)-[:RELATION]->(b);
        MATCH (a:Node {id: 4}) MATCH (b:Node {id: 2}) CREATE (a)-[:RELATION]->(b);
    - |-
        MATCH (n) DETACH DELETE n;

cleanup: |-
    CALL katz_centrality_online.reset() YIELD *;
    CALL mg.load('katz_centrality_online') YIELD *;
    DROP TRIGGER katz_online;
