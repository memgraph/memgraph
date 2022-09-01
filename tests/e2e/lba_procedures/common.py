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

import mgclient
import typing


def execute_and_fetch_all(cursor: mgclient.Cursor, query: str, params: dict = {}) -> typing.List[tuple]:
    cursor.execute(query, params)
    return cursor.fetchall()


def connect(**kwargs) -> mgclient.Connection:
    connection = mgclient.connect(host="localhost", port=7687, **kwargs)
    connection.autocommit = True
    return connection


def reset_permissions(admin_cursor: mgclient.Cursor, create_index: bool):
    execute_and_fetch_all(admin_cursor, "REVOKE LABELS * FROM user;")
    execute_and_fetch_all(admin_cursor, "REVOKE EDGE_TYPES * FROM user;")
    execute_and_fetch_all(admin_cursor, "MATCH(n) DETACH DELETE n;")
    execute_and_fetch_all(admin_cursor, "DROP INDEX ON :read_label(prop);")
    execute_and_fetch_all(admin_cursor, "DROP INDEX ON :read_label;")

    execute_and_fetch_all(admin_cursor, "CREATE (n:read_label {prop: 5});")

    if create_index:
        execute_and_fetch_all(admin_cursor, "CREATE INDEX ON :read_label;")
        execute_and_fetch_all(admin_cursor, "CREATE INDEX ON :read_label(prop);")
