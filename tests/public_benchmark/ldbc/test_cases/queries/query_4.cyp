MATCH (person:Person {id:"21990232559429"})-[:KNOWS]-(:Person)<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->(tag:Tag)
WHERE post.creationDate >= toInt(1335830400000) AND post.creationDate < toInt(1339027200000)
OPTIONAL MATCH (tag)<-[:HAS_TAG]-(oldPost:Post)-[:HAS_CREATOR]->(:Person)-[:KNOWS]-(person)
WHERE oldPost.creationDate < toInt(1335830400000)
WITH tag, post, length(collect(oldPost)) AS oldPostCount
WHERE oldPostCount=0
RETURN
  tag.name AS tagName,
  length(collect(post)) AS postCount
ORDER BY postCount DESC, tagName ASC
LIMIT toInt(10);
