CREATE (s:Node {name:'S'}), (t1:Node:Term {name:'T1'}), (x:Node:X {name:'X'}), (t2:Node:Term {name:'T2'}) CREATE (s)-[:R]->(t1), (t1)-[:R]->(x), (x)-[:R]->(t2);
