MATCH (n:Person)-[:KNOWS]->(m:Person) WHERE n.name="Alice" RETURN m
