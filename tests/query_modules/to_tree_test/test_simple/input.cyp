CREATE (a:Student {name: 'Ana'});
CREATE (b:Student {name: 'Bob'});
CREATE (c:Student {name: 'Carol'});
MATCH (a:Student {name: 'Ana'}), (b:Student {name: 'Bob'}) CREATE (a)-[:FRIEND]->(b);
MATCH (b:Student {name: 'Bob'}), (c:Student {name: 'Carol'}) CREATE (b)-[:FRIEND]->(c);
