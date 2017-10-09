#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from neo4j.v1 import GraphDatabase, basic_auth, ClientError, TransientError

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

def tx_too_long(tx):
    a = tx.run("MATCH (a), (b), (c), (d), (e), (f) RETURN COUNT(*) AS cnt")
    print(a[0]['cnt'])

def add_person(f, name, name2):
    with driver.session() as session:
        session.write_transaction(f, name, name2)

# Wrong query.
try:
    add_person(tx_error, "mirko", "slavko")
except ClientError:
    pass

# Correct query.
add_person(tx_good, "mirka", "slavka")

# Setup for next query.
with driver.session() as session:
    session.write_transaction(
        lambda tx: tx.run("UNWIND range(1, 100000) AS x CREATE ()"))

# Query that will run for a very long time, transient error expected.
try:
    session = driver.session()
    session.read_transaction(tx_too_long)
except TransientError:
    print("transient error")
except:
    assert False, "TransientError should have been thrown"
finally:
    session.close()

driver.close()

print("All ok!")
