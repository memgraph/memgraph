#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
from neo4j import GraphDatabase, basic_auth

driver = GraphDatabase.driver('bolt://localhost:7687',
                              auth=basic_auth('', ''),
                              encrypted=False)
session = driver.session()

session.run('MATCH (n) DETACH DELETE n').consume()
print('Database cleared.')

session.run('CREATE (alice:Person {name: "Alice", age: 22})').consume()
print('Record created.')

node = session.run('MATCH (n) RETURN n').single()['n']
print('Record matched.')

label = list(node.labels)[0]
name = node['name']
age = node['age']

if label != 'Person' or name != 'Alice' or age != 22:
    print('Data does not match')
    sys.exit(1)

print('Label: %s' % label)
print('name: %s' % name)
print('age: %s' % age)

session.close()
driver.close()

print('All ok!')
