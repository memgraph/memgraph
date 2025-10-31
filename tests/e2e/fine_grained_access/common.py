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


def setup_db():
    admin_connection = connect(username="admin", password="test")
    admin_cursor = admin_connection.cursor()
    execute_and_fetch_all(admin_cursor, "CREATE DATABASE clean;")


def switch_db(cursor):
    execute_and_fetch_all(cursor, "USE DATABASE clean;")


def create_multi_db(cursor, switch, reset_func):
    execute_and_fetch_all(cursor, "USE DATABASE memgraph;")
    if switch:
        switch_db(cursor)
        reset_func(cursor)
        execute_and_fetch_all(cursor, "USE DATABASE memgraph;")


def execute_and_fetch_all(cursor, query):
    cursor.execute(query)
    return cursor.fetchall()


def connect(**kwargs):
    connection = mgclient.connect(host="localhost", port=7687, **kwargs)
    connection.autocommit = True
    return connection
