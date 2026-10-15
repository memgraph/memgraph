CREATE (d:Dog {id:0})-[l:LOVES {property: 100}]->(h:Human {id:1});
MATCH (d:Dog),(h:Human) CREATE (d)-[i:IS_OWNED_BY {property2: 700}]->(h);
