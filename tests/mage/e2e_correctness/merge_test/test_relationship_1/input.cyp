MERGE (d:Dog {id:0})-[l:Loves {property: true}]->(h:Human {id:1}) MERGE (c:Cat {id:2})-[:Loves {property: true}]->(h);
