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

print("Checking user impersonation...")

# Setup
with GraphDatabase.driver("bolt://localhost:7687", auth=None, encrypted=False) as driver:
    with driver.session() as session:
        session.run("CREATE USER admin;").consume()
        session.run("GRANT IMPERSONATE_USER * TO admin;").consume()
        session.run("CREATE USER user;").consume()
        session.run("CREATE USER user2;").consume()
        session.run("CREATE USER user3;").consume()


# Impersonate during a session
with GraphDatabase.driver("bolt://localhost:7687", auth=("admin", ""), encrypted=False) as driver:
    with driver.session() as session:
        assert session.run("SHOW CURRENT USER;").values()[0][0] == "admin"

    with driver.session(impersonated_user="user") as session:
        assert session.run("SHOW CURRENT USER;").values()[0][0] == "user"

    with driver.session() as session:
        assert session.run("SHOW CURRENT USER;").values()[0][0] == "admin"

# Try to impersonate a non-existent user
with GraphDatabase.driver("bolt://localhost:7687", auth=("admin", ""), encrypted=False) as driver:
    failed = False
    try:
        with driver.session(impersonated_user="does not exist") as session:
            assert session.run("SHOW CURRENT USER;").values()[0][0] == "admin"
    except:
        failed = True
    assert failed

# Try to impersonate a user you don't have access to
with GraphDatabase.driver("bolt://localhost:7687", auth=("admin", ""), encrypted=False) as driver:
    with driver.session() as session:
        session.run("GRANT IMPERSONATE_USER user2,user3 TO admin;").consume()

with GraphDatabase.driver("bolt://localhost:7687", auth=("admin", ""), encrypted=False) as driver:
    failed = False
    try:
        with driver.session(impersonated_user="user") as session:
            assert session.run("SHOW CURRENT USER;").values()[0][0] == "user"
    except:
        failed = True
    assert failed

    with driver.session(impersonated_user="user2") as session:
        assert session.run("SHOW CURRENT USER;").values()[0][0] == "user2"

# Try to impersonate a user you are explicitly denied
with GraphDatabase.driver("bolt://localhost:7687", auth=("admin", ""), encrypted=False) as driver:
    with driver.session() as session:
        session.run("DENY IMPERSONATE_USER user3 TO admin;").consume()

with GraphDatabase.driver("bolt://localhost:7687", auth=("admin", ""), encrypted=False) as driver:
    failed = False
    try:
        with driver.session(impersonated_user="user3") as session:
            assert session.run("SHOW CURRENT USER;").values()[0][0] == "user3"
    except:
        failed = True
    assert failed

    with driver.session(impersonated_user="user2") as session:
        assert session.run("SHOW CURRENT USER;").values()[0][0] == "user2"


# Try to impersonate a user without the correct permissions
with GraphDatabase.driver("bolt://localhost:7687", auth=("admin", ""), encrypted=False) as driver:
    with driver.session() as session:
        session.run("REVOKE IMPERSONATE_USER FROM admin;").consume()

with GraphDatabase.driver("bolt://localhost:7687", auth=("admin", ""), encrypted=False) as driver:
    failed = False
    try:
        with driver.session(impersonated_user="user2") as session:
            assert session.run("SHOW CURRENT USER;").values()[0][0] == "user2"
    except:
        failed = True

    assert failed


print("All ok!")
