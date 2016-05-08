MATCH (a)-[:KNOWS]->(b) RETURN b.name UNION ALL MATCH (a)-[:LOVES]->(b) RETURN b.name
