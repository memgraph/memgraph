queries: >
  CREATE (:Person {birthday: date("1947-07-30")})
  CREATE (:School {Calculus: localTime("09:15:00")})
  CREATE (:Flights {AIR123: localDateTime("2021-10-05T14:15:00")})
  CREATE (:F1Laps {lap: duration({day:1, hour: 2, minute:3, second:4})})
  CREATE (d:Dog {prop1: 10, prop2: "string"})-[l:Loves {prop_rel1: 10, prop_rel2: "value"}]->(h:Human {prop1: "QUOTE"})
  CREATE (p:Plane)-[f:Flies {height : "10000"}]->(de:Destination {name: "Zadar"});
nodes: |
  MATCH (key) RETURN key;
relationships: |
  MATCH (n)-[key]-() RETURN key;
