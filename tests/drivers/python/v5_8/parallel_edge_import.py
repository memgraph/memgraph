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
import threading

from neo4j import GraphDatabase, basic_auth

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("", ""), encrypted=False)
session = driver.session()

node_count = 100


def add_edge_tx(tx):
    result = tx.run("MATCH (root:Root) CREATE (n:Node) CREATE (n)-[:TROUBLING_EDGE]->(root) RETURN root").single()
    pass


def add_edge():
    sess = driver.session()
    sess.execute_write(add_edge_tx)
    print("Transaction got through!")
    sess.close()


session.run("MATCH (n) DETACH DELETE n").consume()
print("Database cleared.")

session.run("MERGE (root:Root);").consume()
print("Root created.")

threads = [threading.Thread(target=add_edge) for x in range(node_count - 1)]
[x.start() for x in threads]
[x.join() for x in threads]

count = session.run("MATCH (n) RETURN count(n) AS cnt").single()["cnt"]
assert count == node_count

session.close()
driver.close()

print("All ok!")
