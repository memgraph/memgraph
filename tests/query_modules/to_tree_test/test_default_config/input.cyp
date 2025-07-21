CREATE (a:Student {name: 'Ana', age: 22});
CREATE (b:Student {name: 'Bob', age: 23});
MATCH (a:Student {name: 'Ana'}), (b:Student {name: 'Bob'}) CREATE (a)-[:FRIEND]->(b);
