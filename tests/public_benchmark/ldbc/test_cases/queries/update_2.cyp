// Parameters are read from an arbitrary line in ldbc_snb_datagen/social_network/updateStream_0_0_forum.csv
// First 3 columns are skipped, then read in order and passed as arguments.
// Missing columns are filled with ""
// The easiest way is to run QueryTester and see the parameters.

// LdbcUpdate2AddPostLike{personId=26388279073665, postId=1236953235741, creationDate=Thu Sep 13 11:36:22 CEST 2012}
// Date is converted to the number of milliseconds since January 1, 1970, 00:00:00 GMT

// Add a Like to a Post of the social network.
MATCH (p:Person {id:"26388279073665"}),
      (m:Post {id:"1236953235741"})
CREATE (p)-[:LIKES {creationDate:1347528982194}]->(m);
