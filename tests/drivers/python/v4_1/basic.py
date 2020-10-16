#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from neo4j import GraphDatabase, basic_auth

driver = GraphDatabase.driver("bolt://localhost:7687",
                              auth=basic_auth("", ""),
                              encrypted=False)
session = driver.session()

session.run('MATCH (n) DETACH DELETE n').consume()
session.run('CREATE (alice:Person {name: "Alice", age: 22})').consume()

returned_result_set = session.run('MATCH (n) RETURN n')
returned_result = returned_result_set.single()
alice = returned_result["n"]

print(alice['name'])
print(set(alice.labels))
print(alice['age'])

session.close()
driver.close()

print("All ok!")
