MERGE (a:Ana {name: "Ana", age: 22, id:0}) MERGE (b:Marija {name: "Marija", age: 20, id:1}) CREATE (a)-[r:KNOWS]->(b);
