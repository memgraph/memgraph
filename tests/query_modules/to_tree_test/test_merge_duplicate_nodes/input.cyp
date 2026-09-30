CREATE (alice:Person:Employee {name: "Alice", age: 30});
CREATE (bob:Person:Employee {name: "Bob", age: 25});
CREATE (techcorp:Company:Organization {name: "TechCorp", industry: "Technology"});
MATCH (alice:Person {name: 'Alice'}), (bob:Person {name: 'Bob'}), (techcorp:Company {name: 'TechCorp'}) CREATE (alice)-[:WORKS_FOR {since: 2020}]->(techcorp), (bob)-[:WORKS_FOR {since: 2021}]->(techcorp);
