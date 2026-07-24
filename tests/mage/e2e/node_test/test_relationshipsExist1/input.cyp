CREATE (d:Dog {name: "Rex"})-[:KNOWS]->(d2:Dog {name: "Dog30"});
MATCH (d:Dog {name: "Rex"}), (d2:Dog {name: "Dog30"}) CREATE (d)-[:FOLLOWS]->(d2);
MATCH (d:Dog {name: "Rex"}) CREATE (d)-[:KNOWS]->(d3:Dog {name: "Dog60"});
