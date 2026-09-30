MERGE (s1:Student {name: 'Ana'}) MERGE (s2:Student {name: 'Marija'}) CREATE (s1)-[k:KNOWS]->(s2);
