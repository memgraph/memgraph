CREATE (s:Node {name:'S'}), (a:Node {name:'A'}), (b:Node {name:'B'}), (d:Node {name:'D'}) CREATE (s)-[:R]->(a), (s)-[:R]->(b), (a)-[:R]->(d), (b)-[:R]->(d), (d)-[:R]->(s);
