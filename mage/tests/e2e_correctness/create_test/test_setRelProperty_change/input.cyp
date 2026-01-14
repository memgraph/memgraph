MERGE (station1:Station {id: 1, name: "Station 1"}) MERGE (station2:Station {id: 2, name: "Station 3"}) CREATE (station1)-[j:JOURNEY {id: 1, arrival: "0802", departure: "0803"}]->(station2)
