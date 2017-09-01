MATCH (person:Person {id:"30786325583618"})-[:KNOWS*1..2]-(friend:Person)
WHERE not(person=friend)
WITH DISTINCT friend
MATCH (friend)-[worksAt:WORK_AT]->(company:Organisation)-[:IS_LOCATED_IN]->(:Place {name:"Laos"})
WHERE worksAt.workFrom < 2010
RETURN
  friend.id AS friendId,
  friend.firstName AS friendFirstName,
  friend.lastName AS friendLastName,
  company.name AS companyName,
  worksAt.workFrom AS workFromYear
ORDER BY workFromYear ASC, tointeger(friendId) ASC, companyName DESC
LIMIT 20;
