#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright 2021 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import sys

from neo4j import GraphDatabase, basic_auth

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("", ""), encrypted=False)
session = driver.session()

session.run("MATCH (n) DETACH DELETE n").consume()
print("Database cleared.")

session.run('CREATE (alice:Person {name: "Alice", age: 22})').consume()
print("Record created.")

node = session.run("MATCH (n) RETURN n").single()["n"]
print("Record matched.")

label = list(node.labels)[0]
name = node["name"]
age = node["age"]

if label != "Person" or name != "Alice" or age != 22:
    print("Data does not match")
    sys.exit(1)

print("Label: %s" % label)
print("name: %s" % name)
print("age: %s" % age)

session.close()
driver.close()

print("All ok!")
