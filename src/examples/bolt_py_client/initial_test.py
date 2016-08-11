from neo4j.v1 import GraphDatabase, basic_auth, types

# create session
driver = GraphDatabase.driver("bolt://localhost",
                              auth=basic_auth("neo4j", "neo4j"),
                              encrypted=0)
session = driver.session()

queries = [];

queries.append((True, "CREATE (n {name: \"Max\", age: 21}) RETURN n"))
queries.append((False, "CREATE (n {name: \"Paul\", age: 21}) RETURN n"))
queries.append((False, "CREATE (n:PERSON {name: \"Chris\", age: 20}) RETURN n"))
queries.append((False, "CREATE (n:PERSON:STUDENT {name: \"Marko\", age: 19}) RETURN n"))
queries.append((False, "CREATE (n:TEST {string: \"Properties test\", integer: 100, float: 232.2323, bool: True}) RETURN n"))

queries.append((False, "MATCH (n) WHERE ID(n)=0 RETURN n"))
queries.append((False, "MATCH (n) WHERE ID(n)=1 RETURN n"))
queries.append((False, "MATCH (n) WHERE ID(n)=2 RETURN n"))
queries.append((False, "MATCH (n) WHERE ID(n)=3 RETURN n"))
queries.append((False, "MATCH (n) WHERE ID(n)=4 RETURN n"))

queries.append((False, "MATCH (n1), (n2) WHERE ID(n1)=0 AND ID(n2)=1 CREATE (n1)-[r:IS]->(n2) RETURN r"))
queries.append((False, "MATCH (n1), (n2) WHERE ID(n1)=0 AND ID(n2)=1 CREATE (n1)-[r:IS {name: \"test\", age: 23}]->(n2) RETURN r"))
queries.append((False, "MATCH (n1), (n2) WHERE ID(n1)=0 AND ID(n2)=1 CREATE (n1)-[r:IS {name: \"test\", age: 23}]->(n2) RETURN r"))

queries.append((False, "MATCH ()-[r]-() WHERE ID(r)=0 RETURN r"))
queries.append((False, "MATCH ()-[r]-() WHERE ID(r)=1 RETURN r"))
queries.append((False, "MATCH ()-[r]-() WHERE ID(r)=2 RETURN r"))

queries.append((False, "MATCH (n) WHERE ID(n)=1 SET n.name = \"updated_name\" RETURN n"))
queries.append((False, "MATCH (n) WHERE ID(n)=1 RETURN n"))
queries.append((False, "MATCH ()-[r]-() WHERE ID(r)=1 SET r.name = \"TEST100\" RETURN r"))
queries.append((False, "MATCH ()-[r]-() WHERE ID(r)=1 RETURN r"))

for active, query in queries:
    if active:
        session.run(query)
