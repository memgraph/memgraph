MATCH (n) DETACH DELETE n;
CREATE (:Person {name: "Alice"});
CREATE (:Person {name: "Bob"});
