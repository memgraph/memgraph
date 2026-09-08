CREATE (s:Node {name:'S'}), (m:Node:B {name:'M'}), (x:Node:B {name:'X'}) CREATE (s)-[:R]->(x), (s)-[:R]->(m), (m)-[:R]->(x);
