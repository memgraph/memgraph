CREATE (alice:Person:Employee {name: "Alice"});
CREATE (bob:Person:Manager {name: "Bob"});
CREATE (company:Company {name: "TechCorp"});
MATCH (alice:Person {name: "Alice"}), (bob:Person {name: "Bob"}), (company:Company {name: "TechCorp"}) CREATE (alice)-[:WORKS_FOR]->(company), (bob)-[:WORKS_FOR]->(company);
