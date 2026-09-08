CREATE (:A);
CREATE (:B);
MATCH (a:A), (b:B) MERGE (a)-[:CONNECTED_TO]->(b);
