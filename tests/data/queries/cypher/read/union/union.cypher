# MATCH (a)-[:KNOWS]->(b) RETURN b.name UNION MATCH (a)-[:LOVES]->(b) RETURN b.name
