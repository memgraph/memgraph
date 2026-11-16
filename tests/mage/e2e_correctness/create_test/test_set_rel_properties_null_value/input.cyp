CREATE (idora:Person {name: "Idora", id:0}) CREATE (ivan:Person {name: "Ivan", id:1}) MERGE (ivan)-[:FRIENDS {since: 'forever', good_friends: 'yes'}]-(idora);
