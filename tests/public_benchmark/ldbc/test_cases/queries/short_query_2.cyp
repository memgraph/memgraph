// Person Posts
MATCH (:Person {id:"933"})<-[:HAS_CREATOR]-(m)-[:REPLY_OF*0..]->(p:Post)
MATCH (p)-[:HAS_CREATOR]->(c)
RETURN
  m.id as messageId,
  CASE exists(m.content)
    WHEN true THEN m.content
    ELSE m.imageFile
  END AS messageContent,
  m.creationDate AS messageCreationDate,
  p.id AS originalPostId,
  c.id AS originalPostAuthorId,
  c.firstName as originalPostAuthorFirstName,
  c.lastName as originalPostAuthorLastName
ORDER BY messageCreationDate DESC
LIMIT 10;
