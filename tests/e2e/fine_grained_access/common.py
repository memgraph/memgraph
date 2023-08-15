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

import mgclient


def switch_db(cursor):
    execute_and_fetch_all(cursor, "USE DATABASE clean;")


def create_multi_db(cursor, switch):
    execute_and_fetch_all(cursor, "USE DATABASE memgraph;")
    try:
        execute_and_fetch_all(cursor, "DROP DATABASE clean;")
    except:
        pass
    execute_and_fetch_all(cursor, "CREATE DATABASE clean;")
    if switch:
        switch_db(cursor)
        reset_and_prepare(cursor)
        execute_and_fetch_all(cursor, "USE DATABASE memgraph;")


def reset_and_prepare(admin_cursor):
    execute_and_fetch_all(admin_cursor, "REVOKE LABELS * FROM user;")
    execute_and_fetch_all(admin_cursor, "REVOKE EDGE_TYPES * FROM user;")
    execute_and_fetch_all(admin_cursor, "MATCH(n) DETACH DELETE n;")
    execute_and_fetch_all(admin_cursor, "CREATE (n:test_delete {name: 'test1'});")
    execute_and_fetch_all(admin_cursor, "CREATE (n:test_delete_1)-[r:edge_type_delete]->(m:test_delete_2);")


def execute_and_fetch_all(cursor, query):
    cursor.execute(query)
    return cursor.fetchall()


def connect(**kwargs):
    connection = mgclient.connect(host="localhost", port=7687, **kwargs)
    connection.autocommit = True
    return connection
