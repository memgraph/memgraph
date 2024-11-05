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
from common import memgraph
from neo4j import GraphDatabase
from neo4j.exceptions import AuthError

URI = "bolt://localhost:7687"


def execute_query(query: str, AUTH):
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        with driver.session() as session:
            session.run(query)


def test_create_user():
    try:
        AUTH = ("", "")
        execute_query("CREATE USER IF NOT EXISTS testuser IDENTIFIED BY 'testpassword';", AUTH)
    except Exception as e:
        print("User not created properly.")
    AUTH = ("testuser", "testpassword")
    execute_query("SET PASSWORD TO 'newPassword' REPLACE 'testpassword';", AUTH)


def test_wrong_credentials():
    AUTH = ("testuser", "testpassword")

    with pytest.raises(Exception):
        execute_query("SHOW DATABASE SETTINGS;", AUTH)
        print("Authentication failed")


def test_right_credentials():
    try:
        AUTH = ("testuser", "newPassword")
        execute_query("SHOW DATABASE SETTINGS;", AUTH)
    except AuthError as e:
        print("Authentication failed")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
