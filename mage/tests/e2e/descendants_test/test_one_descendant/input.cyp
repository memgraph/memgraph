CREATE (:A {id:1});
CREATE (:B {id:2});
MATCH (a:A), (b:B) MERGE (a)-[:CONNECTED_TO]->(b);
