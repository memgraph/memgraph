CREATE (alice:Person:Employee {name: "Alice"}), (bob:Person:Manager {name: "Bob"}), (company:Company {name: "TechCorp"}), (alice)-[:WORKS_FOR]->(company), (bob)-[:WORKS_FOR]->(company);
