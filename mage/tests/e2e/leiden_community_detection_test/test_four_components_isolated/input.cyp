MERGE (a:Node {id: 0}) MERGE (b:Node {id: 1}) MERGE (c:Node {id: 2}) CREATE (a)-[:RELATION]->(b) CREATE (a)-[:RELATION]->(c) CREATE (b)-[:RELATION]->(c);
MERGE (d:Node {id: 3});
MERGE (e:Node {id: 4});
