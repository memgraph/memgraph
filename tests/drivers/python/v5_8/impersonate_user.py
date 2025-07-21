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

print("Testing database-specific impersonation permissions...")

# Note: Database-specific impersonation is achieved through roles with specific database access
# rather than direct database qualifiers on GRANT IMPERSONATE_USER statements.
# This approach provides proper database isolation for impersonation permissions.

# Test database-specific impersonation permissions
with GraphDatabase.driver("bolt://localhost:7687", auth=("admin", ""), encrypted=False) as driver:
    with driver.session() as session:
        # Create roles for database-specific access
        session.run("CREATE ROLE db1_role;").consume()
        session.run("CREATE ROLE db2_role;").consume()
        session.run("CREATE ROLE memgraph_role;").consume()

        # Grant database access to roles
        session.run("GRANT DATABASE db1 TO db1_role;").consume()
        session.run("GRANT DATABASE db2 TO db2_role;").consume()
        session.run("GRANT DATABASE memgraph TO memgraph_role;").consume()

        # Grant privileges to roles
        session.run("GRANT CREATE, MATCH, SET TO db1_role;").consume()
        session.run("GRANT CREATE, MATCH, SET TO db2_role;").consume()
        session.run("GRANT CREATE, MATCH, SET TO memgraph_role;").consume()

        # Grant impersonation permissions to roles
        session.run("GRANT IMPERSONATE_USER user TO db1_role;").consume()
        session.run("GRANT IMPERSONATE_USER user2 TO db2_role;").consume()
        session.run("GRANT IMPERSONATE_USER user3 TO memgraph_role;").consume()

        # Add roles to admin
        session.run("SET ROLE FOR admin TO db1_role, db2_role, memgraph_role;").consume()

# Test database-specific impersonation through roles
with GraphDatabase.driver("bolt://localhost:7687", auth=("admin", ""), encrypted=False) as driver:
    # Admin should be able to impersonate user when using db1_role context (db1)
    with driver.session(impersonated_user="user", database="db1") as session:
        assert session.run("SHOW CURRENT USER;").values()[0][0] == "user"
        assert session.run("SHOW DATABASE").values()[0][0] == "db1"

    # Admin should NOT be able to impersonate user on db2 (no permission through roles)
    try:
        with driver.session(impersonated_user="user", database="db2") as session:
            session.run("SHOW CURRENT USER;").consume()
        assert False, "Should not be able to impersonate user on db2"
    except:
        pass

    # Admin should be able to impersonate user2 when using db2_role context (db2)
    with driver.session(impersonated_user="user2", database="db2") as session:
        assert session.run("SHOW CURRENT USER;").values()[0][0] == "user2"
        assert session.run("SHOW DATABASE").values()[0][0] == "db2"

    # Admin should NOT be able to impersonate user2 on db1 (no permission through roles)
    try:
        with driver.session(impersonated_user="user2", database="db1") as session:
            session.run("SHOW CURRENT USER;").consume()
        assert False, "Should not be able to impersonate user2 on db1"
    except:
        pass

    # Admin should be able to impersonate user3 when using memgraph_role context (memgraph)
    # However, user3 doesn't have access to memgraph, so it should fail
    with driver.session(impersonated_user="user3", database="memgraph") as session:
        assert_exception(lambda: session.run("SHOW DATABASE"))

print("Testing main database selection during impersonation...")

# Test that impersonation uses the target user's main database when no database is specified
with GraphDatabase.driver("bolt://localhost:7687", auth=("admin", ""), encrypted=False) as driver:
    # Impersonate user (main database is memgraph)
    with driver.session(impersonated_user="user") as session:
        assert session.run("SHOW CURRENT USER;").values()[0][0] == "user"
        assert session.run("SHOW DATABASE").values()[0][0] == "memgraph"

    # Impersonate user2 (main database is db1)
    with driver.session(impersonated_user="user2") as session:
        assert session.run("SHOW CURRENT USER;").values()[0][0] == "user2"
        assert session.run("SHOW DATABASE").values()[0][0] == "db1"

    # Impersonate user3 (main database is db2)
    with driver.session(impersonated_user="user3") as session:
        assert session.run("SHOW CURRENT USER;").values()[0][0] == "user3"
        assert session.run("SHOW DATABASE").values()[0][0] == "db2"

print("Testing database access control during impersonation...")

# Test that impersonated users can only access databases they have permissions for
with GraphDatabase.driver("bolt://localhost:7687", auth=("admin", ""), encrypted=False) as driver:
    with driver.session() as session:
        # Grant all impersonation permissions back for the rest of the tests
        session.run("GRANT IMPERSONATE_USER * TO admin;").consume()

    # Test user access (has access to memgraph, db1, db2)
    with driver.session(impersonated_user="user") as session:
        # Should be able to access all granted databases
        session.run("USE DATABASE memgraph").consume()
        session.run("USE DATABASE db1").consume()
        session.run("USE DATABASE db2").consume()

    # Test user2 access (has access to db1, db2 only)
    with driver.session(impersonated_user="user2") as session:
        # Should NOT be able to access memgraph
        assert_exception(lambda: session.run("USE DATABASE memgraph"))
        # Should be able to access granted databases
        session.run("USE DATABASE db1").consume()
        session.run("USE DATABASE db2").consume()

    # Test user3 access (has access to db2 only)
    with driver.session(impersonated_user="user3") as session:
        # Should NOT be able to access memgraph or db1
        assert_exception(lambda: session.run("USE DATABASE memgraph"))
        assert_exception(lambda: session.run("USE DATABASE db1"))
        # Should be able to access granted database
        session.run("USE DATABASE db2").consume()

print("Testing database switching during impersonation...")

# Test that impersonated users can switch between databases they have access to
with GraphDatabase.driver("bolt://localhost:7687", auth=("admin", ""), encrypted=False) as driver:
    with driver.session(impersonated_user="user") as session:
        # Start on main database (memgraph)
        assert session.run("SHOW DATABASE").values()[0][0] == "memgraph"

        # Switch to db1
        session.run("USE DATABASE db1").consume()
        assert session.run("SHOW DATABASE").values()[0][0] == "db1"

        # Switch to db2
        session.run("USE DATABASE db2").consume()
        assert session.run("SHOW DATABASE").values()[0][0] == "db2"

        # Switch back to memgraph
        session.run("USE DATABASE memgraph").consume()
        assert session.run("SHOW DATABASE").values()[0][0] == "memgraph"

    with driver.session(impersonated_user="user2") as session:
        # Start on main database (db1)
        assert session.run("SHOW DATABASE").values()[0][0] == "db1"

        # Switch to db2
        session.run("USE DATABASE db2").consume()
        assert session.run("SHOW DATABASE").values()[0][0] == "db2"

        # Switch back to db1
        session.run("USE DATABASE db1").consume()
        assert session.run("SHOW DATABASE").values()[0][0] == "db1"

print("Testing database-specific permissions during impersonation...")

# Test that impersonated users have the correct permissions on different databases
with GraphDatabase.driver("bolt://localhost:7687", auth=("admin", ""), encrypted=False) as driver:
    with driver.session() as session:
        # Grant different permissions per database
        session.run("DROP ROLE db1_role;").consume()
        session.run("DROP ROLE db2_role;").consume()

        session.run("CREATE ROLE db1_role;").consume()
        session.run("CREATE ROLE db1_role2;").consume()
        session.run("GRANT CREATE, MATCH, SET TO db1_role;").consume()
        session.run("GRANT CREATE, MATCH, SET TO db1_role2;").consume()
        session.run("GRANT DATABASE db1 TO db1_role;").consume()
        session.run("REVOKE DATABASE memgraph FROM db1_role;").consume()
        session.run("GRANT DATABASE db1 TO db1_role2;").consume()
        session.run("REVOKE DATABASE memgraph FROM db1_role2;").consume()

        session.run("CREATE ROLE db2_role;").consume()
        session.run("CREATE ROLE db2_role2;").consume()
        session.run("CREATE ROLE db2_role3;").consume()
        session.run("GRANT MATCH, SET TO db2_role;").consume()
        session.run("GRANT MATCH TO db2_role2;").consume()
        session.run("GRANT CREATE, MATCH, SET TO db2_role3;").consume()
        session.run("GRANT DATABASE db2 TO db2_role;").consume()
        session.run("REVOKE DATABASE memgraph FROM db2_role;").consume()
        session.run("GRANT DATABASE db2 TO db2_role2;").consume()
        session.run("REVOKE DATABASE memgraph FROM db2_role2;").consume()
        session.run("GRANT DATABASE db2 TO db2_role3;").consume()
        session.run("REVOKE DATABASE memgraph FROM db2_role3;").consume()

        session.run("SET ROLE FOR user TO db1_role, db2_role;").consume()
        session.run("SET ROLE FOR user2 TO db1_role2, db2_role2;").consume()
        session.run("SET ROLE FOR user3 TO db2_role3;").consume()

    # Test user permissions on db1 (has CREATE, MATCH, SET)
    with driver.session(impersonated_user="user", database="db1") as session:
        assert session.run("SHOW CURRENT USER;").values()[0][0] == "user"
        assert session.run("SHOW DATABASE").values()[0][0] == "db1"

        # Should be able to create nodes
        session.run("CREATE ({name: 'test'})").consume()

        # Should be able to match and set properties
        session.run("MATCH (n) SET n.updated = true").consume()

        # Should be able to read data
        result = session.run("MATCH (n) RETURN n.name").single()
        assert result["n.name"] == "test"

    # Test user permissions on db2 (has MATCH, SET only)
    with driver.session(impersonated_user="user", database="db2") as session:
        assert session.run("SHOW CURRENT USER;").values()[0][0] == "user"
        assert session.run("SHOW DATABASE").values()[0][0] == "db2"

        # Should NOT be able to create nodes
        assert_exception(lambda: session.run("CREATE ({name: 'test'})"))

        # Should be able to match and set properties
        session.run("MATCH (n) SET n.updated = true").consume()

    # Test user2 permissions on db1 (has CREATE, MATCH, SET)
    with driver.session(impersonated_user="user2", database="db1") as session:
        assert session.run("SHOW CURRENT USER;").values()[0][0] == "user2"
        assert session.run("SHOW DATABASE").values()[0][0] == "db1"

        # Should be able to create nodes
        session.run("CREATE (n {name: 'test2'})").consume()

        # Should be able to match and set properties
        session.run("MATCH (n) SET n.updated = true").consume()

    # Test user2 permissions on db2 (has MATCH only)
    with driver.session(impersonated_user="user2", database="db2") as session:
        assert session.run("SHOW CURRENT USER;").values()[0][0] == "user2"
        assert session.run("SHOW DATABASE").values()[0][0] == "db2"

        # Should NOT be able to create nodes
        assert_exception(lambda: session.run("CREATE (n {name: 'test'})"))

        # Should NOT be able to set properties
        assert_exception(lambda: session.run("MATCH (n) SET n.updated = true"))

        # Should be able to read data
        session.run("MATCH (n) RETURN n").consume()

    # Test user3 permissions on db2 (has CREATE, MATCH, SET)
    with driver.session(impersonated_user="user3", database="db2") as session:
        assert session.run("SHOW CURRENT USER;").values()[0][0] == "user3"
        assert session.run("SHOW DATABASE").values()[0][0] == "db2"

        # Should be able to create nodes
        session.run("CREATE (n {name: 'test3'})").consume()

        # Should be able to match and set properties
        session.run("MATCH (n) SET n.updated = true").consume()

print("Testing database-specific impersonation with main database fallback...")

# Test that when no database is specified, impersonation uses the target user's main database
with GraphDatabase.driver("bolt://localhost:7687", auth=("admin", ""), encrypted=False) as driver:
    # Test user (main database is memgraph)
    with driver.session(impersonated_user="user") as session:
        assert session.run("SHOW CURRENT USER;").values()[0][0] == "user"
        assert session.run("SHOW DATABASE").values()[0][0] == "memgraph"

    # Test user2 (main database is db1)
    with driver.session(impersonated_user="user2") as session:
        assert session.run("SHOW CURRENT USER;").values()[0][0] == "user2"
        assert session.run("SHOW DATABASE").values()[0][0] == "db1"

    # Test user3 (main database is db2)
    with driver.session(impersonated_user="user3") as session:
        assert session.run("SHOW CURRENT USER;").values()[0][0] == "user3"
        assert session.run("SHOW DATABASE").values()[0][0] == "db2"

print("Testing database access denial during impersonation...")

# Test that impersonated users cannot access databases they don't have permissions for
with GraphDatabase.driver("bolt://localhost:7687", auth=("admin", ""), encrypted=False) as driver:
    # Test user2 trying to access memgraph (denied)
    try:
        with driver.session(impersonated_user="user2", database="memgraph") as session:
            session.run("SHOW DATABASE").consume()
        assert False, "Should not be able to access memgraph"
    except:
        pass

    # Test user3 trying to access memgraph (denied)
    try:
        with driver.session(impersonated_user="user3", database="memgraph") as session:
            session.run("SHOW DATABASE").consume()
        assert False, "Should not be able to access memgraph"
    except:
        pass

    # Test user3 trying to access db1 (denied)
    try:
        with driver.session(impersonated_user="user3", database="db1") as session:
            session.run("SHOW DATABASE").consume()
        assert False, "Should not be able to access db1"
    except:
        pass

print("Testing session isolation between impersonated and non-impersonated sessions...")

# Test that sessions are properly isolated
with GraphDatabase.driver("bolt://localhost:7687", auth=("admin", ""), encrypted=False) as driver:
    # Regular admin session
    with driver.session() as session:
        assert session.run("SHOW CURRENT USER;").values()[0][0] == "admin"
        assert session.run("SHOW DATABASE").values()[0][0] == "memgraph"

    # Impersonated session
    with driver.session(impersonated_user="user") as session:
        assert session.run("SHOW CURRENT USER;").values()[0][0] == "user"
        assert session.run("SHOW DATABASE").values()[0][0] == "memgraph"

    # Back to regular admin session
    with driver.session() as session:
        assert session.run("SHOW CURRENT USER;").values()[0][0] == "admin"
        assert session.run("SHOW DATABASE").values()[0][0] == "memgraph"

print("All ok!")

# Cleanup
with GraphDatabase.driver("bolt://localhost:7687", auth=("admin", ""), encrypted=False) as driver:
    with driver.session() as session:
        session.run("DROP USER admin;").consume()
        session.run("DROP USER user;").consume()
        session.run("DROP USER user2;").consume()
        session.run("DROP USER user3;").consume()
