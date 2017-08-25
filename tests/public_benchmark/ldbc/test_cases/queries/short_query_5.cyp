// Message Creator
MATCH
  (m:Message {id:"2061584302101"})-[:HAS_CREATOR]->(p:Person)
RETURN
  p.id AS personId,
  p.firstName AS firstName,
  p.lastName AS lastName;
