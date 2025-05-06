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


def assert_exception(func):
    try:
        func()
    except:
        return
    assert False, "Expected exception not raised"


def assert_no_exception(func):
    try:
        func()
    except:
        assert False, "Exception raised"


print("Checking user impersonation...")

# Check user-less state
with GraphDatabase.driver("bolt://localhost:7687", auth=None, encrypted=False) as driver:
    with driver.session() as session:
        assert session.run("SHOW CURRENT USER;").values()[0][0] == None
    failed = False
    try:
        with driver.session(impersonated_user="user") as session:
            assert session.run("SHOW CURRENT USER;").values()[0][0] == "user"
    except:
        failed = True
    assert failed
    with driver.session() as session:
        assert session.run("SHOW CURRENT USER;").values()[0][0] == None


# Setup
with GraphDatabase.driver("bolt://localhost:7687", auth=None, encrypted=False) as driver:
    with driver.session() as session:
        session.run("CREATE USER admin;").consume()  # Has all permissions
        session.run("GRANT IMPERSONATE_USER * TO admin;").consume()
        session.run("CREATE USER user;").consume()
        session.run("GRANT MATCH, SET, DELETE TO user;").consume()
        session.run("CREATE USER user2;").consume()
        session.run("GRANT AUTH TO user2;").consume()
        session.run("CREATE USER user3;").consume()
        session.run("DENY AUTH TO user3;").consume()


# Impersonate during a session
with GraphDatabase.driver("bolt://localhost:7687", auth=("admin", ""), encrypted=False) as driver:
    with driver.session() as session:
        assert session.run("SHOW CURRENT USER;").values()[0][0] == "admin"

    with driver.session(impersonated_user="user") as session:
        assert session.run("SHOW CURRENT USER;").values()[0][0] == "user"
        assert_exception(lambda: session.run("CREATE (n:Node)"))
        assert_exception(lambda: session.run("CREATE USER abc"))
        assert_no_exception(lambda: session.run("MATCH(n) SET n.p = 1"))

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
            assert_exception(lambda: session.run("CREATE (n:Node)"))
            assert_exception(lambda: session.run("CREATE USER abc"))
            assert_no_exception(lambda: session.run("MATCH(n) SET n.p = 2"))
    except:
        failed = True
    assert failed

    with driver.session(impersonated_user="user2") as session:
        assert session.run("SHOW CURRENT USER;").values()[0][0] == "user2"
        assert_exception(lambda: session.run("CREATE (n:Node)"))
        assert_no_exception(lambda: session.run("CREATE USER abc"))
        assert_exception(lambda: session.run("MATCH(n) SET n.p = 3"))

# Try to impersonate a user you are explicitly denied
with GraphDatabase.driver("bolt://localhost:7687", auth=("admin", ""), encrypted=False) as driver:
    with driver.session() as session:
        session.run("DENY IMPERSONATE_USER user3 TO admin;").consume()

with GraphDatabase.driver("bolt://localhost:7687", auth=("admin", ""), encrypted=False) as driver:
    failed = False
    try:
        with driver.session(impersonated_user="user3") as session:
            assert session.run("SHOW CURRENT USER;").values()[0][0] == "user3"
            assert_exception(lambda: session.run("CREATE (n:Node)"))
            assert_exception(lambda: session.run("CREATE USER abc"))
            assert_exception(lambda: session.run("MATCH(n) SET n.p = 4"))
    except:
        failed = True
    assert failed

    with driver.session(impersonated_user="user2") as session:
        assert session.run("SHOW CURRENT USER;").values()[0][0] == "user2"
        assert_exception(lambda: session.run("CREATE (n:Node)"))
        assert_no_exception(lambda: session.run("CREATE USER abc"))
        assert_exception(lambda: session.run("MATCH(n) SET n.p = 5"))


# Try to impersonate a user without the correct permissions
with GraphDatabase.driver("bolt://localhost:7687", auth=("admin", ""), encrypted=False) as driver:
    with driver.session() as session:
        session.run("REVOKE IMPERSONATE_USER FROM admin;").consume()

with GraphDatabase.driver("bolt://localhost:7687", auth=("admin", ""), encrypted=False) as driver:
    failed = False
    try:
        with driver.session(impersonated_user="user2") as session:
            assert session.run("SHOW CURRENT USER;").values()[0][0] == "user2"
            assert_exception(lambda: session.run("CREATE (n:Node)"))
            assert_no_exception(lambda: session.run("CREATE USER abc"))
            assert_exception(lambda: session.run("MATCH(n) SET n.p = 6"))
    except:
        failed = True

    assert failed


# Try to impersonate a different user with the same username
with GraphDatabase.driver("bolt://localhost:7687", auth=("admin", ""), encrypted=False) as driver:
    with driver.session() as session:
        session.run("GRANT IMPERSONATE_USER user,user2,user3 TO admin;").consume()


with GraphDatabase.driver("bolt://localhost:7687", auth=("admin", ""), encrypted=False) as driver:
    with driver.session() as session:
        session.run("DROP USER user2;").consume()
        session.run("CREATE USER user2;").consume()

with GraphDatabase.driver("bolt://localhost:7687", auth=("admin", ""), encrypted=False) as driver:
    failed = False
    try:
        with driver.session(impersonated_user="user2") as session:
            assert session.run("SHOW CURRENT USER;").values()[0][0] == "user2"
            assert_exception(lambda: session.run("CREATE (n:Node)"))
            assert_exception(lambda: session.run("CREATE USER abc"))
            assert_exception(lambda: session.run("MATCH(n) SET n.p = 7"))
    except:
        failed = True
    assert failed

    with driver.session(impersonated_user="user") as session:
        assert session.run("SHOW CURRENT USER;").values()[0][0] == "user"
        assert_exception(lambda: session.run("CREATE (n:Node)"))
        assert_exception(lambda: session.run("CREATE USER abc"))
        assert_no_exception(lambda: session.run("MATCH(n) SET n.p = 8"))
    with driver.session(impersonated_user="user3") as session:
        assert session.run("SHOW CURRENT USER;").values()[0][0] == "user3"
        assert_exception(lambda: session.run("CREATE (n:Node)"))
        assert_exception(lambda: session.run("CREATE USER abc"))
        assert_exception(lambda: session.run("MATCH(n) SET n.p = 9"))

print("Checking multi-tenancy and user impersonation...")

with GraphDatabase.driver("bolt://localhost:7687", auth=("admin", ""), encrypted=False) as driver:
    with driver.session() as session:
        session.run("CREATE DATABASE db1").consume()
        session.run("CREATE DATABASE db2").consume()

        session.run("GRANT DATABASE memgraph TO user").consume()
        session.run("GRANT DATABASE db1 TO user").consume()
        session.run("GRANT DATABASE db2 TO user").consume()
        session.run("REVOKE DATABASE memgraph FROM user2").consume()
        session.run("GRANT DATABASE db1 TO user2").consume()
        session.run("GRANT DATABASE db2 TO user2").consume()
        session.run("REVOKE DATABASE memgraph FROM user3").consume()
        session.run("REVOKE DATABASE db1 FROM user3").consume()
        session.run("GRANT DATABASE db2 TO user3").consume()

        session.run("SET MAIN DATABASE memgraph FOR user").consume()
        session.run("SET MAIN DATABASE db1 FOR user2").consume()
        session.run("SET MAIN DATABASE db2 FOR user3").consume()

        session.run("GRANT IMPERSONATE_USER * TO admin;").consume()
        session.run("GRANT MULTI_DATABASE_USE TO user;").consume()
        session.run("GRANT MULTI_DATABASE_USE TO user2;").consume()
        session.run("GRANT MULTI_DATABASE_USE TO user3;").consume()

# Reconnect to update all auth data
with GraphDatabase.driver("bolt://localhost:7687", auth=("admin", ""), encrypted=False) as driver:
    # land on the default database of the impersonated user
    with driver.session(impersonated_user="user") as session:
        assert session.run("SHOW DATABASE").values()[0][0] == "memgraph"
        assert session.run("SHOW CURRENT USER;").values()[0][0] == "user"
        session.run("USE DATABASE db1").consume()
        assert session.run("SHOW DATABASE").values()[0][0] == "db1"
        session.run("USE DATABASE db2").consume()
        assert session.run("SHOW DATABASE").values()[0][0] == "db2"
    with driver.session(impersonated_user="user2") as session:
        assert session.run("SHOW DATABASE").values()[0][0] == "db1"
        assert session.run("SHOW CURRENT USER;").values()[0][0] == "user2"
        assert_exception(lambda: session.run("USE DATABASE memgraph"))
        session.run("USE DATABASE db2").consume()
        assert session.run("SHOW DATABASE").values()[0][0] == "db2"
    with driver.session(impersonated_user="user3") as session:
        assert session.run("SHOW DATABASE").values()[0][0] == "db2"
        assert session.run("SHOW CURRENT USER;").values()[0][0] == "user3"
        assert_exception(lambda: session.run("USE DATABASE memgraph"))
        assert_exception(lambda: session.run("USE DATABASE db1"))

    # try landing on other dbs
    with driver.session(impersonated_user="user", database="memgraph") as session:
        assert session.run("SHOW DATABASE").values()[0][0] == "memgraph"
        assert session.run("SHOW CURRENT USER;").values()[0][0] == "user"
    with driver.session(impersonated_user="user", database="db1") as session:
        assert session.run("SHOW DATABASE").values()[0][0] == "db1"
        assert session.run("SHOW CURRENT USER;").values()[0][0] == "user"
    with driver.session(impersonated_user="user", database="db2") as session:
        assert session.run("SHOW DATABASE").values()[0][0] == "db2"
        assert session.run("SHOW CURRENT USER;").values()[0][0] == "user"

    with driver.session(impersonated_user="user2", database="memgraph") as session:
        assert_exception(lambda: session.run("SHOW DATABASE"))
    with driver.session(impersonated_user="user2", database="db1") as session:
        assert session.run("SHOW DATABASE").values()[0][0] == "db1"
        assert session.run("SHOW CURRENT USER;").values()[0][0] == "user2"
    with driver.session(impersonated_user="user2", database="db2") as session:
        assert session.run("SHOW DATABASE").values()[0][0] == "db2"
        assert session.run("SHOW CURRENT USER;").values()[0][0] == "user2"

    with driver.session(impersonated_user="user3", database="memgraph") as session:
        assert_exception(lambda: session.run("SHOW DATABASE"))
    with driver.session(impersonated_user="user3", database="db1") as session:
        assert_exception(lambda: session.run("SHOW DATABASE"))
    with driver.session(impersonated_user="user3", database="db2") as session:
        assert session.run("SHOW DATABASE").values()[0][0] == "db2"
        assert session.run("SHOW CURRENT USER;").values()[0][0] == "user3"

    # check connection user still is correct
    with driver.session() as session:
        assert session.run("SHOW DATABASE").values()[0][0] == "memgraph"
        assert session.run("SHOW CURRENT USER;").values()[0][0] == "admin"

print("All ok!")

# Cleanup
with GraphDatabase.driver("bolt://localhost:7687", auth=("admin", ""), encrypted=False) as driver:
    with driver.session() as session:
        session.run("DROP USER admin;").consume()
        session.run("DROP USER user;").consume()
        session.run("DROP USER user2;").consume()
        session.run("DROP USER user3;").consume()
