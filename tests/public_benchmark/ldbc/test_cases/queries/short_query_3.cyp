// Person Friends
MATCH (n:Person {id:"933"})-[r:KNOWS]-(friend)
RETURN
  friend.id AS personId,
  friend.firstName AS firstName,
  friend.lastName AS lastName,
  r.creationDate AS friendshipCreationDate
ORDER BY friendshipCreationDate DESC, toInteger(personId) ASC;
