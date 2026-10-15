CREATE (a:Node {name:'A'}), (b:Node {name:'B'}), (c:Node {name:'C'}), (d:Node {name:'D'}) CREATE (a)-[:R]->(b), (b)-[:R]->(c), (c)-[:R]->(d);
