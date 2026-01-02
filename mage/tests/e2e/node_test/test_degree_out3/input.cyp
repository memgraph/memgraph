MERGE (d:Dog {name: "Rex"}) WITH d UNWIND range(0, 1000) AS id MERGE (h:Human {name: "Humie" + id}) MERGE (d)-[l:LOVES]->(h);
