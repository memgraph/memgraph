CREATE (n1 {id: 1}) CREATE (n2 {id: 2}) CREATE (n3 {id: 3}) CREATE (n4 {id: 4})
CREATE (n1)-[:E {weight1: 1, weight2: 1}]->(n2)
CREATE (n1)-[:E {weight1: 2, weight2: 10}]->(n3)
CREATE (n2)-[:E {weight1: 3, weight2: 100}]->(n4)
CREATE (n3)-[:E {weight1: 4, weight2: 1}]->(n4);
