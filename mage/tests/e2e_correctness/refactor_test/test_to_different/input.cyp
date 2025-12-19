MERGE (a:Person {name: "Ivan", id:0}) MERGE (b:Person {name: "Matija", id:1}) MERGE (c:Person {name:"Idora", human:true, id:2}) CREATE (a)-[f:Friends]->(b);
