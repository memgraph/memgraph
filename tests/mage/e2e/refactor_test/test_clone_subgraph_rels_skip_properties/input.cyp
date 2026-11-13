CREATE (a:Node {id: 'a', name: 'A'}), (b:Node {id: 'b', name: 'B'}) CREATE (a)-[:REL {id: 'r', w: 1}]->(b);
MATCH (a:Node {id: 'a'}) MATCH (b:Node {id: 'b'}) WITH [a, b] AS nodes MATCH (n)-[r]-(m) WHERE n IN nodes AND m IN nodes WITH nodes, collect(DISTINCT r) AS rels CALL refactor.clone_subgraph(nodes, rels, {skipProperties: ['id']}) YIELD output RETURN output;
