CREATE (a:Node {name:'A'}), (b:Node {name:'B'}), (c:Node {name:'C'}) CREATE (a)-[:R]->(b), (c)-[:OTHER]->(a);
