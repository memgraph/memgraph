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


def assert_db(session, db):
    assert session.run("SHOW DATABASE").values() == [
        [
            db,
        ]
    ]


def assert_data(session, db):
    assert session.run("MATCH(n:Node) RETURN n.db").values() == [
        [
            db,
        ]
    ]


with GraphDatabase.driver("bolt://localhost:7687", auth=None, encrypted=False) as driver:
    print("Checking user-defined landing database...")
    # Setup
    with driver.session() as session:
        assert_db(session, "memgraph")
        session.run("MATCH(n) DETACH DELETE n;").consume()
        try:
            session.run("DROP DATABASE db1;").consume()
        except:
            pass
        try:
            session.run("DROP DATABASE db2;").consume()
        except:
            pass
        session.run('CREATE (:Node{db:"memgraph"})').consume()

        session.run("CREATE DATABASE db1").consume()
        session.run("USE DATABASE db1").consume()
        assert_db(session, "db1")
        session.run('CREATE (:Node{db:"db1"})').consume()

        session.run("CREATE DATABASE db2").consume()
        session.run("USE DATABASE db2").consume()
        assert_db(session, "db2")
        session.run('CREATE (:Node{db:"db2"})').consume()

    # Test cases
    # Default <- allows for db switches
    with driver.session() as session:
        session.run("USE DATABASE db1").consume()
        assert_db(session, "db1")
        assert_data(session, "db1")
        session.run("USE DATABASE memgraph").consume()
        assert_db(session, "memgraph")
        assert_data(session, "memgraph")
        session.run("USE DATABASE db2").consume()
        assert_db(session, "db2")
        assert_data(session, "db2")

    # memgraph
    with driver.session(database="memgraph") as session:
        assert_db(session, "memgraph")
        assert_data(session, "memgraph")
        failed = False
        try:
            session.run("USE DATABASE db1").consume()
        except:
            failed = True
        assert failed

    # db1
    with driver.session(database="db1") as session:
        assert_db(session, "db1")
        assert_data(session, "db1")
        failed = False
        try:
            session.run("USE DATABASE db2").consume()
        except:
            failed = True
        assert failed

    # Default again <- allows for db switches
    with driver.session() as session:
        session.run("USE DATABASE db1").consume()
        assert_db(session, "db1")
        assert_data(session, "db1")
        session.run("USE DATABASE memgraph").consume()
        assert_db(session, "memgraph")
        assert_data(session, "memgraph")
        session.run("USE DATABASE db2").consume()
        assert_db(session, "db2")
        assert_data(session, "db2")

    # db2
    with driver.session(database="db2") as session:
        assert_db(session, "db2")
        assert_data(session, "db2")
        failed = False
        try:
            session.run("USE DATABASE memgraph").consume()
        except:
            failed = True
        assert failed

    # Test database rename functionality
    print("Testing database rename functionality...")

    # Create a test database for renaming
    with driver.session() as session:
        try:
            session.run("DROP DATABASE rename_test").consume()
        except:
            pass
        session.run("CREATE DATABASE rename_test").consume()
        session.run("USE DATABASE rename_test").consume()
        session.run('CREATE (:Node{name:"rename_test_node", value:42})').consume()

        # Verify initial state
        assert_db(session, "rename_test")
        results = session.run("MATCH (n:Node) RETURN n.name, n.value").values()
        assert len(results) == 1
        assert results[0] == ["rename_test_node", 42]

        # Rename the database
        session.run("RENAME DATABASE rename_test TO renamed_test").consume()

        # Verify rename was successful
        session.run("USE DATABASE renamed_test").consume()
        assert_db(session, "renamed_test")
        results = session.run("MATCH (n:Node) RETURN n.name, n.value").values()
        assert len(results) == 1
        assert results[0] == ["rename_test_node", 42]

        # Verify old name no longer exists
        failed = False
        try:
            session.run("USE DATABASE rename_test").consume()
        except:
            failed = True
        assert failed

        # Test error conditions
        print("Testing rename error conditions...")

        # Test renaming default database (should fail)
        failed = False
        try:
            session.run("RENAME DATABASE memgraph TO new_name").consume()
        except:
            failed = True
        assert failed

        # Test renaming non-existent database (should fail)
        failed = False
        try:
            session.run("RENAME DATABASE non_existent TO new_name").consume()
        except:
            failed = True
        assert failed

        # Test renaming to existing database name (should fail)
        session.run("CREATE DATABASE conflict_db").consume()
        failed = False
        try:
            session.run("RENAME DATABASE renamed_test TO conflict_db").consume()
        except:
            failed = True
        assert failed

        # Test successful rename after stopping use
        session.run("USE DATABASE memgraph").consume()
        session.run("RENAME DATABASE renamed_test TO final_test").consume()

        # Verify final state
        session.run("USE DATABASE final_test").consume()
        assert_db(session, "final_test")
        results = session.run("MATCH (n:Node) RETURN n.name, n.value").values()
        assert len(results) == 1
        assert results[0] == ["rename_test_node", 42]

        # Clean up
        session.run("USE DATABASE memgraph").consume()
        session.run("DROP DATABASE final_test").consume()
        session.run("DROP DATABASE conflict_db").consume()

    print("Database rename tests completed successfully!")
