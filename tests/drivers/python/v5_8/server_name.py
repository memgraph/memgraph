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


def get_server_name(tx):
    res = tx.run("SHOW DATABASE SETTINGS").values()
    for setting in res:
        if setting[0] == "server.name":
            return setting[1]
    assert False, "No setting named server.name"


def set_server_name(tx, name):
    tx.run("SET DATABASE SETTING 'server.name' TO '{}'".format(name)).consume()


# Connect, check name, set a new name and recheck
with GraphDatabase.driver("bolt://localhost:7687", auth=None, encrypted=False) as driver:
    with driver.session() as session:
        default_name = get_server_name(session)
    assert driver.get_server_info().agent == default_name, "Wrong server name! Expected {} and got {}".format(
        default_name, driver.get_server_info().agent
    )

    with driver.session() as session:
        set_server_name(session, "Neo4j/1.1 compatible database")


with GraphDatabase.driver("bolt://localhost:7687", auth=None, encrypted=False) as driver:
    assert (
        driver.get_server_info().agent == "Neo4j/1.1 compatible database"
    ), 'Wrong server name! Expected "Neo4j/1.1 compatible database" and got {}'.format(driver.get_server_info().agent)

    with driver.session() as session:
        set_server_name(session, default_name)


print("All ok!")
