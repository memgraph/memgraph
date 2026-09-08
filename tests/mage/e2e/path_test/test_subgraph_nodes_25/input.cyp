CREATE (a:Node {name:'A'}), (b:Node:MiddleLabelLongerThanSso {name:'B'}), (c:Node {name:'C'}), (x:Node {name:'X'}) CREATE (a)-[:R]->(b), (b)-[:R]->(c), (x)-[:R]->(a);
