// LdbcUpdate5AddForumMembership{forumId=1786706433093, personId=32985348839299, joinDate=Thu Sep 13 11:36:02 CEST 2012}

MATCH (f:Forum {id:"1786706433093"}),
      (p:Person {id:"32985348839299"})
CREATE (f)-[:HAS_MEMBER {joinDate:1347528962967}]->(p);
