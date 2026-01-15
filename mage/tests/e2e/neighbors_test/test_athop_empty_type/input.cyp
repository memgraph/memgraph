MERGE (a:Rachel) MERGE (b:Monica) CREATE (a)-[f:FRIENDS]->(b);
MERGE (a:Monica) MERGE (b:Ross) CREATE (a)-[f:FRIENDS]->(b);
MERGE (a:Monica) MERGE (b:Chandler) CREATE (a)-[f:FRIENDS]->(b);
MERGE (a:Chandler) MERGE (b:Joey) CREATE (a)-[f:FRIENDS]->(b);
MERGE (a:Monica) MERGE (b:Phoebe) CREATE (a)-[f:FRIENDS]->(b);
MERGE (a:Ross) MERGE (b:Rachel) CREATE (a)-[f:LOVES]->(b);
