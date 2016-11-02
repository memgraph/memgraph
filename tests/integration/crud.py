from neo4j.v1 import GraphDatabase, basic_auth, types

# create session
driver = GraphDatabase.driver("bolt://localhost",
                              auth=basic_auth("neo4j", "neo4j"),
                              encrypted=0)
session = driver.session()

queries = [];

queries.append((True, "CREATE (n {age: 32}) RETURN n"))
queries.append((True, "CREATE (n {name: \"Max\", age: 21}) RETURN n"))
queries.append((True, "CREATE (n {name: \"Paul\", age: 21}) RETURN n"))
queries.append((True, "CREATE (n:PERSON {name: \"Chris\", age: 20}) RETURN n"))
queries.append((True, "CREATE (n:PERSON:STUDENT {name: \"Marko\", age: 19}) RETURN n"))
queries.append((True, "CREATE (n:TEST {string: \"Properties test\", integer: 100, float: 232.2323, bool: True}) RETURN n"))

queries.append((True, "MATCH (n) WHERE ID(n)=0 RETURN n"))
queries.append((True, "MATCH (n) WHERE ID(n)=1 RETURN n"))
queries.append((True, "MATCH (n) WHERE ID(n)=2 RETURN n"))
queries.append((True, "MATCH (n) WHERE ID(n)=3 RETURN n"))
queries.append((True, "MATCH (n) WHERE ID(n)=4 RETURN n"))
queries.append((True, "MATCH (n) WHERE ID(n)=5 RETURN n"))
queries.append((True, "MATCH (n) RETURN n"));
queries.append((True, "MATCH (n:PERSON) RETURN n"));

queries.append((True, "MATCH (n1), (n2) WHERE ID(n1)=0 AND ID(n2)=1 CREATE (n1)-[r:IS]->(n2) RETURN r"))
queries.append((True, "MATCH (n1), (n2) WHERE ID(n1)=1 AND ID(n2)=2 CREATE (n1)-[r:IS {name: \"test\", age: 23}]->(n2) RETURN r"))
queries.append((True, "MATCH (n1), (n2) WHERE ID(n1)=2 AND ID(n2)=0 CREATE (n1)-[r:IS {name: \"test\", age: 23}]->(n2) RETURN r"))
queries.append((True, "MATCH (n1), (n2) WHERE ID(n1)=2 AND ID(n2)=0 CREATE (n1)-[r:ARE {name: \"test\", age: 23}]->(n2) RETURN r"))

queries.append((True, "MATCH ()-[r]-() WHERE ID(r)=0 RETURN r"))
queries.append((True, "MATCH ()-[r]-() WHERE ID(r)=1 RETURN r"))
queries.append((True, "MATCH ()-[r]-() WHERE ID(r)=2 RETURN r"))
queries.append((True, "MATCH ()-[r:IS]-() RETURN r"))
queries.append((True, "MATCH ()-[r:ARE]-() RETURN r"))
queries.append((True, "MATCH ()-[r]-() RETURN r"))

queries.append((True, "MATCH (n) WHERE ID(n)=1 SET n.name = \"updated_name\" RETURN n"))
queries.append((True, "MATCH (n) WHERE ID(n)=1 RETURN n"))
queries.append((True, "MATCH ()-[r]-() WHERE ID(r)=1 SET r.name = \"TEST100\" RETURN r"))
queries.append((True, "MATCH ()-[r]-() WHERE ID(r)=1 RETURN r"))

for i in range(1):
    for active, query in queries:
        if active:
            for record in session.run(query):
                print(record)
