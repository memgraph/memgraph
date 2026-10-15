CREATE (A:Node {name: 'A'}) CREATE (B:Node {name: 'B'}) CREATE (C:Node {name: 'C'}) CREATE (A)-[:R]->(B) CREATE (B)-[:R]->(A) CREATE (B)-[:R]->(C) CREATE (C)-[:R]->(B);
