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

from neo4j import GraphDatabase, basic_auth
from neo4j.exceptions import ClientError, TransientError

with GraphDatabase.driver("bolt://localhost:7687", auth=None, encrypted=False) as driver:
    # Setup the graph.
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n").consume()
        session.run(
            "CREATE (:Node{id:1})-[:IS_CONNECTED]->(:Node{id:2})-[:IS_CONNECTED]->(:Node{id:3})-[:IS_CONNECTED]->(:Node{id:4})"
        ).consume()

    # Return a path of length 2
    with driver.session() as session:
        res = session.run("MATCH p = (:Node)-[:IS_CONNECTED*..2]->(:Node) RETURN p").values()
        assert len(res) == 5, "Didn't find all the paths"

    # Return a path of length 3
    with driver.session() as session:
        res = session.run("MATCH p = (:Node)-[:IS_CONNECTED*3]->(:Node) RETURN p").values()
        assert len(res) == 1, "Didn't find the path"

    print("All ok!")
