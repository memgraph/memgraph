CREATE (a:Node {name:'A'}), (b:Node:Mid {name:'B'}), (c:Node {name:'C'}) CREATE (a)-[:R]->(b), (b)-[:R]->(c);
