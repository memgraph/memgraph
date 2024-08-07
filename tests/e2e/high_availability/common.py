# Copyright 2022 Memgraph Ltd.
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


# Elapse time is the last element in results hence such slicing works.
def ignore_elapsed_time_from_results(results: typing.List[tuple]) -> typing.List[tuple]:
    return [result[:-1] for result in results]


def execute_and_fetch_all(cursor: mgclient.Cursor, query: str, params: dict = {}) -> typing.List[tuple]:
    cursor.execute(query, params)
    return cursor.fetchall()


def connect(**kwargs) -> mgclient.Connection:
    connection = mgclient.connect(**kwargs)
    connection.autocommit = True
    return connection


def safe_execute(function, *args):
    try:
        function(*args)
    except Exception as e:
        print("Exception occurred: ", str(e))
        pass
