from neo4j.v1 import GraphDatabase, basic_auth

driver = GraphDatabase.driver("bolt://localhost",
                              auth=basic_auth("neo4j", "neo4j"),
                              encrypted=0)

session = driver.session()
session.run("CREATE (a:Person {age:25})")
# result = session.run("MATCH (a:Person) RETURN a.age AS age")
for record in result:
    print(record["age"])
    session.close()
