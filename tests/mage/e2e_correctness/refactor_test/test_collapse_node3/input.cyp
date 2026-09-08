CREATE (d:Dog {id: 1})-[w:WALKS]->(p:path {id: 2, prop: 5});
MATCH (p:path) CREATE (p)-[r:Runs]->(h:Home {id: 3});
CREATE (d:Dog2 {id: 4})-[w:WALKS2]->(p:path {id: 5, prop: 6});
MATCH (p:path {prop: 6}) CREATE (p)-[r:Runs2]->(h:Home2 {id: 6});
