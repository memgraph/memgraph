// Message Forum
MATCH (m:Message {id:"1236950581249"})-[:REPLY_OF*0..]->(p:Post)<-[:CONTAINER_OF]-(f:Forum)-[:HAS_MODERATOR]->(mod:Person)
RETURN
  f.id AS forumId,
  f.title AS forumTitle,
  mod.id AS moderatorId,
  mod.firstName AS moderatorFirstName,
  mod.lastName AS moderatorLastName;
