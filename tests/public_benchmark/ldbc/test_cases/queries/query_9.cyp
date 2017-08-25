MATCH (:Person {id:"13194139542834"})-[:KNOWS*1..2]-(friend:Person)<-[:HAS_CREATOR]-(message)
WHERE message.creationDate < 1324080000000
RETURN DISTINCT
  friend.id AS personId,
  friend.firstName AS personFirstName,
  friend.lastName AS personLastName,
  message.id AS messageId,
  CASE exists(message.content)
    WHEN true THEN message.content
    ELSE message.imageFile
  END AS messageContent,
  message.creationDate AS messageCreationDate
ORDER BY message.creationDate DESC, toInteger(message.id) ASC
LIMIT 20;
