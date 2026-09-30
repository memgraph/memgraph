CREATE (company:Company {name: 'Acme Corp'});
CREATE (alice:Person:Employee { name: 'Alice', age: 30, start_date: '2020-01-15', salary: 75000 });
CREATE (bob:Person:Employee { name: 'Bob', age: 45, start_date: '2018-05-20', salary: 90000 });
MATCH (alice:Person {name: 'Alice'}), (bob:Person {name: 'Bob'}), (company:Company {name: 'Acme Corp'}) CREATE (alice)-[:WORKS_FOR]->(company), (bob)-[:WORKS_FOR]->(company);
