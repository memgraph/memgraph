MERGE (a:Person {name: "Ana", age: 22, id: 0}) MERGE (b:Person {name: "Marija", age: 20, id: 1}) MERGE (c:Person {name: "Iva", age: 21, id: 2}) CREATE (a)-[r1:KNOWS]->(b) CREATE (c)-[r2:KNOWS]->(a);
