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

import typing

import mgclient


def execute_and_fetch_all(cursor: mgclient.Cursor, query: str, params: dict = {}) -> typing.List[tuple]:
    cursor.execute(query, params)
    return cursor.fetchall()


def connect(**kwargs) -> mgclient.Connection:
    connection = mgclient.connect(host="localhost", port=7687, **kwargs)
    connection.autocommit = True
    return connection


def switch_db(cursor):
    execute_and_fetch_all(cursor, "USE DATABASE clean;")


def create_multi_db(cursor):
    execute_and_fetch_all(cursor, "USE DATABASE memgraph;")
    try:
        execute_and_fetch_all(cursor, "DROP DATABASE clean;")
    except:
        pass
    execute_and_fetch_all(cursor, "CREATE DATABASE clean;")


def reset_permissions(admin_cursor: mgclient.Cursor, create_index: bool = False):
    execute_and_fetch_all(admin_cursor, "REVOKE LABELS * FROM user;")
    execute_and_fetch_all(admin_cursor, "REVOKE EDGE_TYPES * FROM user;")
    execute_and_fetch_all(admin_cursor, "MATCH(n) DETACH DELETE n;")
    execute_and_fetch_all(admin_cursor, "DROP INDEX ON :read_label(prop);")
    execute_and_fetch_all(admin_cursor, "DROP INDEX ON :read_label;")

    execute_and_fetch_all(admin_cursor, "CREATE (n:read_label {prop: 5});")
    execute_and_fetch_all(
        admin_cursor, "CREATE (n:read_label_1 {prop: 5})-[r:read_edge_type]->(m:read_label_2 {prop: 5});"
    )

    if create_index:
        execute_and_fetch_all(admin_cursor, "CREATE INDEX ON :read_label;")
        execute_and_fetch_all(admin_cursor, "CREATE INDEX ON :read_label(prop);")


def reset_update_permissions(admin_cursor: mgclient.Cursor):
    execute_and_fetch_all(admin_cursor, "REVOKE LABELS * FROM user;")
    execute_and_fetch_all(admin_cursor, "REVOKE EDGE_TYPES * FROM user;")
    execute_and_fetch_all(admin_cursor, "MATCH (n) DETACH DELETE n;")

    execute_and_fetch_all(admin_cursor, "CREATE (n:update_label {prop: 1});")
    execute_and_fetch_all(
        admin_cursor,
        "CREATE (n:update_label_1)-[r:update_edge_type {prop: 1}]->(m:update_label_2);",
    )


def reset_create_delete_permissions(admin_cursor: mgclient.Cursor):
    execute_and_fetch_all(admin_cursor, "REVOKE LABELS * FROM user;")
    execute_and_fetch_all(admin_cursor, "REVOKE EDGE_TYPES * FROM user;")

    execute_and_fetch_all(admin_cursor, "GRANT READ ON LABELS * TO user;")
    execute_and_fetch_all(admin_cursor, "GRANT READ ON EDGE_TYPES * TO user;")

    execute_and_fetch_all(admin_cursor, "MATCH (n) DETACH DELETE n;")

    execute_and_fetch_all(admin_cursor, "CREATE (n:create_delete_label);")
    execute_and_fetch_all(
        admin_cursor,
        "CREATE (n:create_delete_label_1)-[r:create_delete_edge_type]->(m:create_delete_label_2);",
    )
