queries: >
  CREATE (d:Dog {prop1: 10, prop2: "string"})-[l:Loves {prop_rel1: 10, prop_rel2: "value"}]->(h:Human {prop1: "QUOTE"})
  CREATE (p:Plane)-[f:FLIES {height: "10000"}]->(de:Destination {name: "Zadar"});
nodes: |
  MATCH (d:Dog)-[l]->(h:Human) MATCH (de:Destination) RETURN [d, h, de];
relationships: |
  MATCH (d:Dog)-[l]->(h:Human) RETURN [l];
