CREATE (a:Student {name: 'Ana'}),
       (b:Student {name: 'Bob'}),
       (c:Student {name: 'Carol'}),
       (d:Student {name: 'Dave'}),
       (e:Student {name: 'Eve'}),
       (a)-[:FRIEND]->(b),
       (a)-[:FRIEND]->(c),
       (b)-[:COLLEAGUE]->(d),
       (c)-[:FRIEND]->(e),
       (d)-[:FRIEND]->(e)
;
