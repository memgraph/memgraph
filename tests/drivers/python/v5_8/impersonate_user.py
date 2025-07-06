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


print("Checking user limits with impersonated users...")

# Setup users and profiles for limit testing
with GraphDatabase.driver("bolt://localhost:7687", auth=("admin", ""), encrypted=False) as driver:
    with driver.session() as session:
        # Create users for limit testing
        session.run("CREATE USER limit_user1;").consume()
        session.run("GRANT MATCH, CREATE, PROFILE_RESTRICTION TO limit_user1;").consume()
        session.run("CREATE USER limit_user2;").consume()
        session.run("GRANT MATCH, CREATE, PROFILE_RESTRICTION TO limit_user2;").consume()

        # Create profiles with different limits
        session.run("CREATE PROFILE limit_profile1 LIMIT SESSIONS 2, TRANSACTIONS_MEMORY 100MB;").consume()
        session.run("CREATE PROFILE limit_profile2 LIMIT SESSIONS 1, TRANSACTIONS_MEMORY 50MB;").consume()
        session.run("CREATE PROFILE admin_profile LIMIT SESSIONS 10, TRANSACTIONS_MEMORY 500MB;").consume()

        # Assign profiles to users
        session.run("SET PROFILE FOR limit_user1 TO limit_profile1;").consume()
        session.run("SET PROFILE FOR limit_user2 TO limit_profile2;").consume()
        session.run("SET PROFILE FOR admin TO admin_profile;").consume()

        # Grant impersonation permissions
        session.run("GRANT IMPERSONATE_USER limit_user1,limit_user2 TO admin;").consume()

# Test user limits with impersonated users
with GraphDatabase.driver("bolt://localhost:7687", auth=("admin", ""), encrypted=False) as driver:
    # Test that impersonated users inherit their profile limits
    with driver.session(impersonated_user="limit_user1") as session:
        assert session.run("SHOW CURRENT USER;").values()[0][0] == "limit_user1"

        # Check resource usage shows the profile limits
        resource_usage = session.run("SHOW RESOURCE USAGE FOR limit_user1;").values()
        sessions_limit = None
        memory_limit = None
        for row in resource_usage:
            if row[0] == "sessions":
                sessions_limit = row[2]
            elif row[0] == "transactions_memory":
                memory_limit = row[2]

        assert sessions_limit == 2, f"Expected sessions limit 2, got {sessions_limit}"
        assert memory_limit == "100.00MiB", f"Expected memory limit 100.00MiB, got {memory_limit}"

        # Test that we can perform operations within limits
        assert_no_exception(lambda: session.run("CREATE ()").consume())
        assert_exception(lambda: session.run("UNWIND range(1, 10000000) AS i CREATE ()").consume())

        # Check that admin resources remain at 0 during impersonation
        admin_usage = session.run("SHOW RESOURCE USAGE FOR admin;").values()
        admin_sessions = None
        admin_memory = None
        for row in admin_usage:
            if row[0] == "sessions":
                admin_sessions = row[1]
            elif row[0] == "transactions_memory":
                admin_memory = row[1]

        assert admin_sessions == 0, f"Admin sessions should be 0, got {admin_sessions}"
        assert admin_memory == "0B", f"Admin memory should be 0B, got {admin_memory}"

    with driver.session(impersonated_user="limit_user2") as session:
        assert session.run("SHOW CURRENT USER;").values()[0][0] == "limit_user2"

        # Check resource usage shows the profile limits
        resource_usage = session.run("SHOW RESOURCE USAGE FOR limit_user2;").values()
        sessions_limit = None
        memory_limit = None
        for row in resource_usage:
            if row[0] == "sessions":
                sessions_limit = row[2]
            elif row[0] == "transactions_memory":
                memory_limit = row[2]

        assert sessions_limit == 1, f"Expected sessions limit 1, got {sessions_limit}"
        assert memory_limit == "50.00MiB", f"Expected memory limit 50.00MiB, got {memory_limit}"

        # Test that we can perform operations within limits
        assert_no_exception(lambda: session.run("CREATE ()").consume())
        assert_exception(lambda: session.run("UNWIND range(1, 10000000) AS i CREATE ()").consume())

        # Check that admin resources remain at 0 during impersonation
        admin_usage = session.run("SHOW RESOURCE USAGE FOR admin;").values()
        admin_sessions = None
        admin_memory = None
        for row in admin_usage:
            if row[0] == "sessions":
                admin_sessions = row[1]
            elif row[0] == "transactions_memory":
                admin_memory = row[1]

        assert admin_sessions == 0, f"Admin sessions should be 0, got {admin_sessions}"
        assert admin_memory == "0B", f"Admin memory should be 0B, got {admin_memory}"


# Test that multiple impersonated sessions respect user limits
with GraphDatabase.driver("bolt://localhost:7687", auth=("admin", ""), encrypted=False) as driver:
    # Create multiple sessions for limit_user1 (limit: 2 sessions)
    session1 = driver.session(impersonated_user="limit_user1")
    session2 = driver.session(impersonated_user="limit_user1")

    # Both sessions should work (within 2 session limit)
    assert_no_exception(lambda: session1.run("CREATE ()"))
    assert_no_exception(lambda: session2.run("CREATE ()"))

    # Check admin resources remain at 0 during multiple impersonated sessions
    admin_usage = session1.run("SHOW RESOURCE USAGE FOR admin;").values()
    admin_sessions = None
    admin_memory = None
    for row in admin_usage:
        if row[0] == "sessions":
            admin_sessions = row[1]
        elif row[0] == "transactions_memory":
            admin_memory = row[1]

    assert admin_sessions == 0, f"Admin sessions should be 0, got {admin_sessions}"
    assert admin_memory == "0B", f"Admin memory should be 0B, got {admin_memory}"

    # Test that we can perform operations within limits using transactions
    tx1 = session1.begin_transaction()
    tx1.run("CREATE ()")
    tx2 = session2.begin_transaction()
    tx2.run("CREATE ()")

    admin_session = driver.session()
    admin_usage = admin_session.run("SHOW RESOURCE USAGE FOR admin;").values()
    admin_sessions = None
    admin_memory = None
    for row in admin_usage:
        if row[0] == "sessions":
            admin_sessions = row[1]
        elif row[0] == "transactions_memory":
            admin_memory = row[1]
    assert admin_sessions == 1, f"Admin sessions should be 1, got {admin_sessions}"
    assert admin_memory == "0B", f"Admin memory should be 0B, got {admin_memory}"

    usage = admin_session.run("SHOW RESOURCE USAGE FOR limit_user2;").values()
    sessions = None
    memory = None
    for row in usage:
        if row[0] == "sessions":
            sessions = row[1]
        elif row[0] == "transactions_memory":
            memory = row[1]
    assert sessions == 0, f"Sessions should be 0, got {sessions}"
    assert memory == "0B", f"Memory should be 0B, got {memory}"

    usage = admin_session.run("SHOW RESOURCE USAGE FOR limit_user1;").values()
    sessions = None
    memory = None
    for row in usage:
        if row[0] == "sessions":
            sessions = row[1]
        elif row[0] == "transactions_memory":
            memory = row[1]
    assert sessions == 2, f"Sessions should be 2, got {sessions}"
    assert memory != "0B", f"Memory should be non-zero, got {memory}"

    # Go over the sessions limit - this should fail when trying to create a session
    try:
        session3 = driver.session(impersonated_user="limit_user1")
        session3.run("CREATE ()").consume()
        session3.close()
        assert False, "Should have failed due to session limit"
    except Exception as e:
        # Expected to fail due to session limit
        pass

    tx1.commit()
    tx2.commit()

    session1.close()
    session2.close()
    admin_session.close()

# Test memory limits with impersonated users
with GraphDatabase.driver("bolt://localhost:7687", auth=("admin", ""), encrypted=False) as driver:
    with driver.session(impersonated_user="limit_user2") as session:  # 50MB limit
        # Try to create data that might exceed the 50MB limit
        try:
            # Create a large number of nodes to test memory limit
            for i in range(10000):
                session.run(f"CREATE ()")
        except Exception as e:
            # Expected behavior - memory limit should be enforced
            assert "memory" in str(e).lower() or "limit" in str(e).lower(), f"Unexpected error: {e}"

# Test profile changes affect impersonated users
with GraphDatabase.driver("bolt://localhost:7687", auth=("admin", ""), encrypted=False) as driver:
    with driver.session() as session:
        # Update profile limits
        session.run("UPDATE PROFILE limit_profile1 LIMIT SESSIONS 3, TRANSACTIONS_MEMORY 200MB;").consume()

    # Test that impersonated user gets updated limits
    with driver.session(impersonated_user="limit_user1") as session:
        resource_usage = session.run("SHOW RESOURCE USAGE FOR limit_user1;").values()
        sessions_limit = None
        memory_limit = None
        for row in resource_usage:
            if row[0] == "sessions":
                sessions_limit = row[2]
            elif row[0] == "transactions_memory":
                memory_limit = row[2]

        assert sessions_limit == 3, f"Expected sessions limit 3, got {sessions_limit}"
        assert memory_limit == "200.00MiB", f"Expected memory limit 200.00MiB, got {memory_limit}"

# Test that clearing profile affects impersonated users
with GraphDatabase.driver("bolt://localhost:7687", auth=("admin", ""), encrypted=False) as driver:
    with driver.session() as session:
        # Clear profile for limit_user2
        session.run("CLEAR PROFILE FOR limit_user2;").consume()

    # Test that impersonated user gets unlimited limits
    with driver.session(impersonated_user="limit_user2") as session:
        resource_usage = session.run("SHOW RESOURCE USAGE FOR limit_user2;").values()
        sessions_limit = None
        memory_limit = None
        for row in resource_usage:
            if row[0] == "sessions":
                sessions_limit = row[2]
            elif row[0] == "transactions_memory":
                memory_limit = row[2]

        assert sessions_limit == "UNLIMITED", f"Expected sessions limit UNLIMITED, got {sessions_limit}"
        assert memory_limit == "UNLIMITED", f"Expected memory limit UNLIMITED, got {memory_limit}"

# Test role-based profile inheritance with impersonation
with GraphDatabase.driver("bolt://localhost:7687", auth=("admin", ""), encrypted=False) as driver:
    with driver.session() as session:
        # Create role and assign profile
        session.run("CREATE ROLE limit_role;").consume()
        session.run("CREATE PROFILE role_profile LIMIT SESSIONS 5, TRANSACTIONS_MEMORY 300MB;").consume()
        session.run("SET PROFILE FOR limit_role TO role_profile;").consume()
        session.run("SET ROLE FOR limit_user2 TO limit_role;").consume()

    # Test that impersonated user inherits role profile when no direct profile
    with driver.session(impersonated_user="limit_user2") as session:
        resource_usage = session.run("SHOW RESOURCE USAGE FOR limit_user2;").values()
        sessions_limit = None
        memory_limit = None
        for row in resource_usage:
            if row[0] == "sessions":
                sessions_limit = row[2]
            elif row[0] == "transactions_memory":
                memory_limit = row[2]

        # Should inherit from role profile (5 sessions, 300MB)
        assert sessions_limit == 5, f"Expected sessions limit 5, got {sessions_limit}"
        assert memory_limit == "300.00MiB", f"Expected memory limit 300.00MiB, got {memory_limit}"

# Cleanup limit test users and profiles
with GraphDatabase.driver("bolt://localhost:7687", auth=("admin", ""), encrypted=False) as driver:
    with driver.session() as session:
        session.run("DROP USER limit_user1;").consume()
        session.run("DROP USER limit_user2;").consume()
        session.run("DROP ROLE limit_role;").consume()
        session.run("DROP PROFILE limit_profile1;").consume()
        session.run("DROP PROFILE limit_profile2;").consume()
        session.run("DROP PROFILE role_profile;").consume()
        session.run("DROP PROFILE admin_profile;").consume()


print("All ok!")

# Cleanup
with GraphDatabase.driver("bolt://localhost:7687", auth=("admin", ""), encrypted=False) as driver:
    with driver.session() as session:
        session.run("DROP USER admin;").consume()
        session.run("DROP USER user;").consume()
        session.run("DROP USER user2;").consume()
        session.run("DROP USER user3;").consume()
