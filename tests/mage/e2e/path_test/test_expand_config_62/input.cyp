CREATE (s:Start {name:'S'}), (n1:A {name:'N1'}), (n2:B {name:'N2'}), (n3:A {name:'N3'}) CREATE (s)-[:R]->(n1), (n1)-[:R]->(n2), (n2)-[:R]->(n3);
