// Person Profile
MATCH (n:Person {id:"933"})-[:IS_LOCATED_IN]-(p:Place)
RETURN
  n.firstName AS firstName,
  n.lastName AS lastName,
  n.birthday AS birthday,
  n.locationIP AS locationIp,
  n.browserUsed AS browserUsed,
  n.gender AS gender,
  n.creationDate AS creationDate,
  p.id AS cityId;
