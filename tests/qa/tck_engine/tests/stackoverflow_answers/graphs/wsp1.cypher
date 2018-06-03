CREATE (n1 {id: 1}) CREATE (n2 {id: 2}) CREATE (n3 {id: 3}) CREATE (n4 {id: 4})
CREATE (n1)-[:E {pollution_level: 1, distance: 1.0, noise_level: 5}]->(n2)
CREATE (n1)-[:E {pollution_level: 2, distance: 2.0, noise_level: 4}]->(n3)
CREATE (n2)-[:E {pollution_level: 3, distance: 2.0, noise_level: 3}]->(n4)
CREATE (n3)-[:E {pollution_level: 4, distance: 1.0, noise_level: 2}]->(n4);
