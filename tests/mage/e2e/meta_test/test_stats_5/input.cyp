MERGE (a:node {id: 0}) MERGE (b:node {id: 1}) CREATE (a)-[:Relation {goat: true}]->(b);
MERGE (a:node {id: 1}) MERGE (b:node {id: 2}) CREATE (a)-[:Relation {flu: true}]->(b);
MERGE (a:node {id: 2}) MERGE (b:node {id: 0}) CREATE (a)-[:Relation {flower: true}]->(b);
MERGE (a:Node {id: 3}) MERGE (b:Node {id: 3}) CREATE (a)-[:Relation {disease: false}]->(b);
MERGE (a:Node {id: 3}) MERGE (b:Node {id: 4}) CREATE (a)-[:Relation {weather: "cold"}]->(b);
MERGE (a:Node {id: 3}) MERGE (b:Node {id: 5}) CREATE (a)-[:Relation]->(b);
MATCH (:Node)-[r:Relation]-(:Node) DELETE r;
MATCH (n:Node {id: 3}) DETACH DELETE n;
