CREATE (:A {id:1})-[:CONNECTED_TO]->(:B {id:2})-[:CONNECTED_TO]->(:C {id:3});
MATCH (b:B) MERGE (:D {id:4})-[:CONNECTED_TO]->(b);
