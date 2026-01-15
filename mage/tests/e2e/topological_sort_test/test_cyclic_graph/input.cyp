CREATE (a:A)-[:CONNECTED_TO]->(b:B)-[:CONNECTED_TO]->(c:C)
MATCH (c:C), (a:A) MERGE (c)-[:CONNECTED_TO]->(a);
