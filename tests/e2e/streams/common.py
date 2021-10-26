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
import time

# These are the indices of the different values in the result of SHOW STREAM
# query
NAME = 0
TOPICS = 1
CONSUMER_GROUP = 2
BATCH_INTERVAL = 3
BATCH_SIZE = 4
TRANSFORM = 5
OWNER = 6
BOOTSTRAP_SERVERS = 7
IS_RUNNING = 8


def execute_and_fetch_all(cursor, query):
    cursor.execute(query)
    return cursor.fetchall()


def connect(**kwargs):
    connection = mgclient.connect(host="localhost", port=7687, **kwargs)
    connection.autocommit = True
    return connection


def timed_wait(fun):
    start_time = time.time()
    seconds = 10

    while True:
        current_time = time.time()
        elapsed_time = current_time - start_time

        if elapsed_time > seconds:
            return False

        if fun():
            return True

        time.sleep(0.1)


def check_one_result_row(cursor, query):
    start_time = time.time()
    seconds = 10

    while True:
        current_time = time.time()
        elapsed_time = current_time - start_time

        if elapsed_time > seconds:
            return False

        cursor.execute(query)
        results = cursor.fetchall()
        if len(results) < 1:
            time.sleep(0.1)
            continue

        return len(results) == 1


def check_vertex_exists_with_topic_and_payload(cursor, topic, payload_bytes):
    assert check_one_result_row(cursor,
                                "MATCH (n: MESSAGE {"
                                f"payload: '{payload_bytes.decode('utf-8')}',"
                                f"topic: '{topic}'"
                                "}) RETURN n")


def get_stream_info(cursor, stream_name):
    stream_infos = execute_and_fetch_all(cursor, "SHOW STREAMS")
    for stream_info in stream_infos:
        if (stream_info[NAME] == stream_name):
            return stream_info

    return None


def get_is_running(cursor, stream_name):
    stream_info = get_stream_info(cursor, stream_name)

    assert stream_info
    return stream_info[IS_RUNNING]


def start_stream(cursor, stream_name):
    execute_and_fetch_all(cursor, f"START STREAM {stream_name}")

    assert get_is_running(cursor, stream_name)


def stop_stream(cursor, stream_name):
    execute_and_fetch_all(cursor, f"STOP STREAM {stream_name}")

    assert not get_is_running(cursor, stream_name)


def drop_stream(cursor, stream_name):
    execute_and_fetch_all(cursor, f"DROP STREAM {stream_name}")

    assert get_stream_info(cursor, stream_name) is None


def check_stream_info(cursor, stream_name, expected_stream_info):
    stream_info = get_stream_info(cursor, stream_name)
    assert len(stream_info) == len(expected_stream_info)
    for info, expected_info in zip(stream_info, expected_stream_info):
        assert info == expected_info
