# Copyright 2023 Memgraph Ltd.
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

import pytest
from neo4j import GraphDatabase
from neo4j.exceptions import AuthError

URI = "bolt://localhost:7687"


def execute_query(query: str, AUTH):
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        with driver.session() as session:
            return list(session.run(query))


def test_admin():
    try:
        AUTH = ("", "")
        execute_query("CREATE USER IF NOT EXISTS admin IDENTIFIED BY 'testpassword';", AUTH)
    except Exception as e:
        print("User not created properly. Error: ", e)
        assert False
    AUTH = ("admin", "testpassword")
    execute_query("SET PASSWORD TO 'newPassword' REPLACE 'testpassword';", AUTH)


def test_wrong_credentials():
    AUTH = ("admin", "testpassword")

    with pytest.raises(Exception):
        assert "admin" == execute_query("SHOW CURRENT USER;", AUTH)[0]["user"]
        print("Authentication failed. Error: ", e)
        assert False


def test_right_credentials():
    try:
        AUTH = ("admin", "newPassword")
        assert "admin" == execute_query("SHOW CURRENT USER;", AUTH)[0]["user"]
    except AuthError as e:
        print("Authentication failed. Error: ", e)
        assert False


# First user is admin (has all permissions), second user has no permissions
def test_user():
    try:
        AUTH = ("admin", "newPassword")
        execute_query("CREATE USER IF NOT EXISTS user IDENTIFIED BY 'testpassword';", AUTH)
    except Exception as e:
        print("User not created properly.")
        assert False
    AUTH = ("user", "testpassword")
    execute_query("SET PASSWORD TO 'newPassword' REPLACE 'testpassword';", AUTH)
    AUTH = ("user", "newPassword")
    assert "user" == execute_query("SHOW CURRENT USER;", AUTH)[0]["user"]


def test_wrong_credentials_user():
    AUTH = ("user", "testpassword")

    with pytest.raises(Exception):
        assert "user" == execute_query("SHOW CURRENT USER;", AUTH)[0]["user"]
        print("Authentication failed")


def test_right_credentials_user():
    try:
        AUTH = ("user", "newPassword")
        assert "user" == execute_query("SHOW CURRENT USER;", AUTH)[0]["user"]
    except AuthError as e:
        print("Authentication failed. Error: ", e)
        assert False


def test_multi_tenant():
    try:
        AUTH = ("admin", "newPassword")
        execute_query("CREATE DATABASE db1;", AUTH)
        execute_query("GRANT DATABASE db1 TO user;", AUTH)
        execute_query("SET MAIN DATABASE db1 FOR user;", AUTH)
        execute_query("DENY DATABASE memgraph FROM user;", AUTH)
        AUTH = ("user", "newPassword")
        execute_query("SET PASSWORD TO 'anotherPassword' REPLACE 'newPassword';", AUTH)
        AUTH = ("user", "anotherPassword")
        assert "user" == execute_query("SHOW CURRENT USER;", AUTH)[0]["user"]
    except Exception as e:
        print("Multi-tenant test failed. Error: ", e)
        assert False


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
