MERGE (d:Dog {name: "Rex" ,id:0}) WITH d UNWIND range(0, 1000) AS id MERGE (h:Human {name: "Humie" + id, id:id}) MERGE (d)-[l:LOVES {id:id, how:"very"}]->(h);
