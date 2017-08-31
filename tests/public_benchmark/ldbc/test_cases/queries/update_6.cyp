// LdbcUpdate6AddPost{postId=2199025986581, imageFile='', creationDate=Thu Sep 13 11:41:43 CEST 2012, locationIp='61.16.220.210', browserUsed='Chrome', language='tk', content='About Abbas I of Persia, w Shah Mohammed in a coup and placed the 16-year-old Abbas on the th', length=93, authorPersonId=8796093029267, forumId=549755863266, countryId=0, tagIds=[3]}

// If the imageFile parameter is not empty, then we need to add it to the created node properties.

CREATE (m:Post:Message {id: "2199025986581", creationDate: 1347529303996, locationIP: '61.16.220.210', browserUsed: 'Chrome', language: 'tk', content: 'About Abbas I of Persia, w Shah Mohammed in a coup and placed the 16-year-old Abbas on the th', length: 93});

MATCH (m:Post {id: "2199025986581"}),
      (p:Person {id: "8796093029267"}),
      (f:Forum {id: "549755863266"}),
      (c:Place {id: "0"})
OPTIONAL MATCH (t:Tag)
WHERE t.id IN ["3"]
WITH m, p, f, c, collect(t) as tagSet
CREATE (m)-[:HAS_CREATOR]->(p),
       (m)<-[:CONTAINER_OF]-(f),
       (m)-[:IS_LOCATED_IN]->(c)
FOREACH (t IN tagSet| CREATE (m)-[:HAS_TAG]->(t));
