MATCH (n) DETACH DELETE n;
CREATE (:Person {name: "Alice"})-[:KNOWS {since: 2020}]->(:Person {name: "Bob"});
