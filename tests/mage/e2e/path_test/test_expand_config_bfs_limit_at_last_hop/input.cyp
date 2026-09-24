CREATE (s:Node {name:'S'}), (a:Node {name:'A'}) CREATE (s)-[:R]->(a) WITH a UNWIND range(1, 4) AS i CREATE (a)-[:R]->(:Node {name:'X' + toString(i)});
