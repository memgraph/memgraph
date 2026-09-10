CREATE (s:Node {name:'S'}), (a:Node {name:'A'}), (b:Node {name:'B'}), (c:Node {name:'C'}), (z:Node {name:'Z'}) CREATE (s)-[:R]->(a), (a)-[:R]->(b), (b)-[:R]->(c), (s)-[:R]->(z);
