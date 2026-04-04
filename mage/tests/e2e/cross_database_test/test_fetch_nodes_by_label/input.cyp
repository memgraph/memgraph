CREATE (a:Person {name: 'Alice', age: 30, active: true});
CREATE (b:Person {name: 'Bob', age: 25, active: false});
CREATE (c:Person {name: 'Charlie', age: 35, active: true});
MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS {since: 2020}]->(b);
MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Charlie'}) CREATE (b)-[:KNOWS {since: 2021}]->(c);
