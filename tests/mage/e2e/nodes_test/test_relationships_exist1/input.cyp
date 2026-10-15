CREATE (michael:Person {name: "Michael"})-[:KNOWS]->(p:Person {name: "Person30"});
MATCH (michael:Person {name: "Michael"}), (p:Person {name: "Person30"}) CREATE (michael)-[:FOLLOWS]->(p);
MATCH (michael:Person {name: "Michael"}) CREATE (michael)-[:KNOWS]->(p:Person {name: "Person60"});
