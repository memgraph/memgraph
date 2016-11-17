MATCH (p:Personnel)-[:CREATED]->(o:Opportunity)-[:HAS_MATCH]->(c:Company {identifier: "321"}) RETURN (a:Account {prop: "123"})-[:IS]->(p)
