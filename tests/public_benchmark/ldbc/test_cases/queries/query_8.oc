MATCH
  (start:Person {id:"24189255818757"})<-[:HAS_CREATOR]-()<-[:REPLY_OF]-(comment:Comment)-[:HAS_CREATOR]->(person:Person)
RETURN
  person.id AS personId,
  person.firstName AS personFirstName,
  person.lastName AS personLastName,
  comment.creationDate AS commentCreationDate,
  comment.id AS commentId,
  comment.content AS commentContent
ORDER BY commentCreationDate DESC, toInteger(commentId) ASC
LIMIT 20;
