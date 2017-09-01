MATCH (person:Person {id:"17592186053137"})<-[:HAS_CREATOR]-(message)<-[like:LIKES]-(liker:Person)
WITH liker, message, like.creationDate AS likeTime, person
ORDER BY likeTime DESC, toInt(message.id) ASC
WITH
  liker,
  head(collect({msg: message, likeTime: likeTime})) AS latestLike,
  person
RETURN
  liker.id AS personId,
  liker.firstName AS personFirstName,
  liker.lastName AS personLastName,
  latestLike.likeTime AS likeTime,
  latestLike.msg.id AS messageId,
  CASE exists(latestLike.msg.content)
    WHEN true THEN latestLike.msg.content
    ELSE latestLike.msg.imageFile
  END AS messageContent,
  latestLike.likeTime - latestLike.msg.creationDate AS latencyAsMilli,
  not((liker)-[:KNOWS]-(person)) AS isNew
ORDER BY likeTime DESC, toInt(personId) ASC
LIMIT 10;
