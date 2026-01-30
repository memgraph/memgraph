MERGE (croatia:Country {name: 'Croatia'}) MERGE (spain:Country {name: 'Spain'}) MERGE (madrid:City {name: 'Madrid'}) MERGE (madrid)-[:IN_COUNTRY]->(spain);
