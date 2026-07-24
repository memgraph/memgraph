queries: >
  CREATE (ivan: Intern {name: 'Ivan'})  CREATE (idora: Intern {name:'Idora'})
nodes: |
  MATCH (key) RETURN key;
relationships: |
  MATCH (n)-[key]-() RETURN key;
