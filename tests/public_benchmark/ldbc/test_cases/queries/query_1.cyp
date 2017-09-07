MATCH (:Person {id:"30786325583618"})-[p:KNOWS*1..3]-(f:Person)
WHERE f.firstName = "Chau"
WITH f, min(length(p)) AS d
ORDER BY d ASC, f.lastName ASC, toint(f.id) ASC
LIMIT 20
MATCH (f)-[:IS_LOCATED_IN]->(fCity:Place)
OPTIONAL MATCH (f)-[studyAt:STUDY_AT]->(uni:Organisation)-[:IS_LOCATED_IN]->(uniCity:Place)
WITH
f,
collect(
CASE uni.name
WHEN null THEN null
ELSE [uni.name, studyAt.classYear, uniCity.name]
END
) AS unis,
fCity,
d
OPTIONAL MATCH (f)-[worksAt:WORK_AT]->(c:Organisation)-[:IS_LOCATED_IN]->(cCountry:Place)
WITH
f,
collect(
CASE c.name
WHEN null THEN null
ELSE [c.name, worksAt.workFrom, cCountry.name]
END
) AS companies,
unis,
fCity,
d
RETURN
f.id AS id,
f.lastName AS lastName,
d as distance,
f.birthday AS birthday,
f.creationDate AS creationDate,
f.gender AS gender,
f.browserUsed AS browser,
f.locationIP AS locationIp,
f.email AS emails,
f.speaks AS languages,
fCity.name AS cityName,
unis,
companies
ORDER BY distance ASC, f.lastName ASC, toint(f.id) ASC
LIMIT 20;
