MATCH (person:Person {id:"17592186055119"})-[:KNOWS*1..2]-(friend:Person)<-[:HAS_CREATOR]-(messageX),
(messageX)-[:IS_LOCATED_IN]->(countryX:Place)
WHERE
  not(person=friend)
  AND not((friend)-[:IS_LOCATED_IN]->()-[:IS_PART_OF]->(countryX))
  AND countryX.name="Laos" AND messageX.creationDate>=1306886400000
  AND messageX.creationDate<1306886400042
WITH friend, count(DISTINCT messageX) AS xCount
MATCH (friend)<-[:HAS_CREATOR]-(messageY)-[:IS_LOCATED_IN]->(countryY:Place)
WHERE
  countryY.name="Scotland"
  AND not((friend)-[:IS_LOCATED_IN]->()-[:IS_PART_OF]->(countryY))
  AND messageY.creationDate>={4}
  AND messageY.creationDate<{5}
WITH
  friend.id AS friendId,
  friend.firstName AS friendFirstName,
  friend.lastName AS friendLastName,
  xCount,
  count(DISTINCT messageY) AS yCount
RETURN
  friendId,
  friendFirstName,
  friendLastName,
  xCount,
  yCount,
  xCount + yCount AS xyCount
ORDER BY xyCount DESC, toInt(friendId) ASC
LIMIT 10;
