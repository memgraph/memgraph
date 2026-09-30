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

import time
import typing

import mgclient


def get_data_path(file: str, test: str):
    """
    Data is stored in system replication folder.
    """
    return f"system_replication/{file}/{test}"


def get_logs_path(file: str, test: str):
    """
    Logs are stored in system replication folder.
    """
    return f"system_replication/{file}/{test}"


def execute_and_fetch_all(cursor: mgclient.Cursor, query: str, params: dict = {}) -> typing.List[tuple]:
    cursor.execute(query, params)
    return cursor.fetchall()


def drop_database_with_retry(
    cursor: mgclient.Cursor, db_name: str, max_duration: float = 20, time_between_attempt: float = 0.2
) -> None:
    """Drop a database, retrying on transient 'currently being used' errors."""
    start_time = time.time()
    while True:
        try:
            cursor.execute(f"DROP DATABASE {db_name};")
            cursor.fetchall()
            return
        except mgclient.DatabaseError:
            if time.time() - start_time > max_duration:
                raise
            time.sleep(time_between_attempt)


def connect(**kwargs) -> mgclient.Connection:
    connection = mgclient.connect(**kwargs)
    connection.autocommit = True
    return connection
