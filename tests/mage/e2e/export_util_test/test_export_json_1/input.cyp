queries: >
  CREATE (ivan: Intern {name: 'Ivan'})  CREATE (idora: Intern {name:'Idora'})
  CREATE (matija: Intern {name: 'Matija'})   MERGE (ivan)-[:KNOWS]->(idora)
  MERGE (matija)-[:HEARS {Loud: true}]->(idora)  MERGE (matija)-[:SEES]->(ivan);
nodes: |
  MATCH (key) RETURN key;
relationships: |
  MATCH (n)-[key]-() RETURN key;
