CREATE (a:Student {name: 'Ana'}),
       (b:Student {name: 'Bob'}),
       (c:Student {name: 'Carol'}),
       (a)-[:FRIEND]->(b),
       (b)-[:FRIEND]->(c)
;
