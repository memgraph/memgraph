# MATCH (p:Personnel)-[:CREATED]->(o:Opportunity)-[:HAS_MATCH]->(c:Company {id: "321"}) RETURN (a:Account {id: "123"})-[:IS]->(p)
