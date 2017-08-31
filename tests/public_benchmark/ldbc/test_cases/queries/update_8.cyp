// LdbcUpdate8AddFriendship{person1Id=21990232558208, person2Id=32985348841200, creationDate=Thu Sep 13 11:43:09 CEST 2012}
MATCH (p1:Person {id:"21990232558208"}),
      (p2:Person {id:"32985348841200"})
CREATE (p1)-[:KNOWS {creationDate:1347529389109}]->(p2);
