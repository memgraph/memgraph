CREATE (a:Item {score: 1.5})-[:SIMILAR {weight: 0.9}]->(b:Item {score: 2.5})-[:SIMILAR {weight: 0.3}]->(c:Item {score: 3.5});
