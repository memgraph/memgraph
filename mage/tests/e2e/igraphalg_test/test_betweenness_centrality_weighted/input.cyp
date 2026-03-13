MERGE (a:Node {id: 0}) MERGE (b:Node {id: 1}) CREATE (a)-[:RELATION {weight: 1}]->(b);
MERGE (a:Node {id: 0}) MERGE (b:Node {id: 2}) CREATE (a)-[:RELATION {weight: 10}]->(b);
MERGE (a:Node {id: 1}) MERGE (b:Node {id: 2}) CREATE (a)-[:RELATION {weight: 1}]->(b);
