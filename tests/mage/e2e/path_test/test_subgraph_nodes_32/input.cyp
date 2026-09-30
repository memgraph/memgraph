CREATE (a:N {name: 'A'}), (b:N {name: 'B'}), (c:N {name: 'C'}), (d:N {name: 'D'}) CREATE (b)-[:R]->(a), (b)-[:R]->(c), (d)-[:R]->(b);
