MERGE (a:Node {id: 0}) MERGE (b:Node {id: 1}) CREATE (a)-[:Relation]->(b);
MERGE (a:Node {id: 1}) MERGE (b:Node {id: 2}) CREATE (a)-[:Relation]->(b);
MERGE (a:Node {id: 2}) MERGE (b:Node {id: 0}) CREATE (a)-[:Relation]->(b);
MERGE (a:Node {id: 3}) MERGE (b:Node {id: 3}) CREATE (a)-[:Relation]->(b);
MERGE (a:Node {id: 3}) MERGE (b:Node {id: 4}) CREATE (a)-[:Relation]->(b);
MERGE (a:Node {id: 3}) MERGE (b:Node {id: 5}) CREATE (a)-[:Relation]->(b);
MATCH (:Node)-[r:Relation]-(:Node) DELETE r;
