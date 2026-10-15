CREATE (d:Dog {id:0}),(h:Human {id:1}), (c:Car {id:2}),(d)-[r:RUNS {speed: 100, id:0}]->(h),(h)-[dr:DRIVES {speed: 150, id: 1}]->(c);
