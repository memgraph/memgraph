MATCH (:Person {id:"17592186052613"})-[:KNOWS]-(friend:Person)<-[:HAS_CREATOR]-(message)
WHERE message.creationDate <= 1354060800000 AND (message:Post OR message:Comment)
RETURN
  friend.id AS personId,
  friend.firstName AS personFirstName,
  friend.lastName AS personLastName,
  message.id AS messageId,
  CASE exists(message.content)
    WHEN true THEN message.content
    ELSE message.imageFile
  END AS messageContent,
  message.creationDate AS messageDate
ORDER BY messageDate DESC, toInt(messageId) ASC
LIMIT 20;
