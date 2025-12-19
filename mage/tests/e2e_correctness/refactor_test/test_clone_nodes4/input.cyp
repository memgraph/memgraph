MERGE (a:Ana {name: "Ana", age: 22, id:0}) MERGE (b:Marija {name: "Marija", age: 20, id:1}) MERGE(c:Ivan {occupation: "Pianist", id:2}) CREATE (a)-[:KNOWS]->(b) CREATE (a)-[:KNOWS]->(c);
