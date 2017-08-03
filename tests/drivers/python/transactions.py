#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from neo4j.v1 import GraphDatabase, basic_auth, CypherError

driver = GraphDatabase.driver("bolt://localhost:7687",
                              auth=basic_auth("", ""),
                              encrypted=False)

def tx_error(tx, name, name2):
    a = tx.run("CREATE (a:Person {name: $name}) RETURN a", name=name).data()
    print(a[0]['a'])
    tx.run("CREATE (").consume()
    a = tx.run("CREATE (a:Person {name: $name}) RETURN a", name=name2).data()
    print(a[0]['a'])

def tx_good(tx, name, name2):
    a = tx.run("CREATE (a:Person {name: $name}) RETURN a", name=name).data()
    print(a[0]['a'])
    a = tx.run("CREATE (a:Person {name: $name}) RETURN a", name=name2).data()
    print(a[0]['a'])

def add_person(f, name, name2):
    with driver.session() as session:
        session.write_transaction(f, name, name2)

try:
    add_person(tx_error, "mirko", "slavko")
except CypherError:
    pass

add_person(tx_good, "mirka", "slavka")

driver.close()

print("All ok!")
