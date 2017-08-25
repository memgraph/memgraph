// Message Replies
MATCH (m:Message {id:"2061584302089"})<-[:REPLY_OF]-(c:Comment)-[:HAS_CREATOR]->(p:Person)
OPTIONAL MATCH (m)-[:HAS_CREATOR]->(a:Person)-[r:KNOWS]-(p)
RETURN
  c.id AS commentId,
  c.content AS commentContent,
  c.creationDate AS commentCreationDate,
  p.id AS replyAuthorId,
  p.firstName AS replyAuthorFirstName,
  p.lastName AS replyAuthorLastName,
  CASE r
    WHEN null THEN false
    ELSE true
  END AS replyAuthorKnowsOriginalMessageAuthor
ORDER BY commentCreationDate DESC, toInteger(replyAuthorId) ASC;
