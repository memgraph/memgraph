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

from multiprocessing import Process, Value

# These are the indices of the different values in the result of SHOW STREAM
# query
NAME = 0
TYPE = 1
BATCH_INTERVAL = 2
BATCH_SIZE = 3
TRANSFORM = 4
OWNER = 5
IS_RUNNING = 6

# These are the indices of the query and parameters in the result of CHECK
# STREAM query
QUERY = 0
PARAMS = 1

SIMPLE_MSG = b"message"


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


def check_vertex_exists_with_properties(cursor, properties):
    properties_string = ', '.join([f'{k}: {v}' for k, v in properties.items()])
    assert check_one_result_row(
        cursor,
        "MATCH (n: MESSAGE {"
        f"{properties_string}"
        "}) RETURN n",
    )


def get_stream_info(cursor, stream_name):
    stream_infos = execute_and_fetch_all(cursor, "SHOW STREAMS")
    for stream_info in stream_infos:
        if stream_info[NAME] == stream_name:
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

def kafka_check_vertex_exists_with_topic_and_payload(cursor, topic, payload_bytes):
    decoded_payload = payload_bytes.decode('utf-8')
    check_vertex_exists_with_properties(
            cursor, {'topic': f'"{topic}"', 'payload': f'"{decoded_payload}"', 'type': "'kafka'"})


def pulsar_default_namespace_topic(topic):
    return f'persistent://public/default/{topic}'


def test_start_and_stop_during_check(
        operation,
        connection,
        stream_creator,
        message_sender,
        already_stopped_error):
    # This test is quite complex. The goal is to call START/STOP queries
    # while a CHECK query is waiting for its result. Because the Global
    # Interpreter Lock, running queries on multiple threads is not useful,
    # because only one of them can call Cursor::execute at a time. Therefore
    # multiple processes are used to execute the queries, because different
    # processes have different GILs.
    # The counter variables are thread- and process-safe variables to
    # synchronize between the different processes. Each value represents a
    # specific phase of the execution of the processes.
    assert operation in ["START", "STOP"]
    cursor = connection.cursor()
    execute_and_fetch_all(
        cursor,
        stream_creator('test_stream')
    )

    check_counter = Value("i", 0)
    check_result_len = Value("i", 0)
    operation_counter = Value("i", 0)

    CHECK_BEFORE_EXECUTE = 1
    CHECK_AFTER_FETCHALL = 2
    CHECK_CORRECT_RESULT = 3
    CHECK_INCORRECT_RESULT = 4

    def call_check(counter, result_len):
        # This process will call the CHECK query and increment the counter
        # based on its progress and expected behavior
        connection = connect()
        cursor = connection.cursor()
        counter.value = CHECK_BEFORE_EXECUTE
        result = execute_and_fetch_all(cursor, "CHECK STREAM test_stream")
        result_len.value = len(result)
        counter.value = CHECK_AFTER_FETCHALL
        if len(result) > 0 and "payload: 'message'" in result[0][QUERY]:
            counter.value = CHECK_CORRECT_RESULT
        else:
            counter.value = CHECK_INCORRECT_RESULT

    OP_BEFORE_EXECUTE = 1
    OP_AFTER_FETCHALL = 2
    OP_ALREADY_STOPPED_EXCEPTION = 3
    OP_INCORRECT_ALREADY_STOPPED_EXCEPTION = 4
    OP_UNEXPECTED_EXCEPTION = 5

    def call_operation(counter):
        # This porcess will call the query with the specified operation and
        # increment the counter based on its progress and expected behavior
        connection = connect()
        cursor = connection.cursor()
        counter.value = OP_BEFORE_EXECUTE
        try:
            execute_and_fetch_all(cursor, f"{operation} STREAM test_stream")
            counter.value = OP_AFTER_FETCHALL
        except mgclient.DatabaseError as e:
            if already_stopped_error in str(e):
                counter.value = OP_ALREADY_STOPPED_EXCEPTION
            else:
                counter.value = OP_INCORRECT_ALREADY_STOPPED_EXCEPTION
        except Exception:
            counter.value = OP_UNEXPECTED_EXCEPTION

    check_stream_proc = Process(
        target=call_check, daemon=True, args=(check_counter, check_result_len)
    )
    operation_proc = Process(
        target=call_operation, daemon=True, args=(operation_counter,)
    )

    try:
        check_stream_proc.start()

        time.sleep(0.5)

        assert timed_wait(lambda: check_counter.value == CHECK_BEFORE_EXECUTE)
        assert timed_wait(lambda: get_is_running(cursor, "test_stream"))
        assert check_counter.value == CHECK_BEFORE_EXECUTE, (
            "SHOW STREAMS " "was blocked until the end of CHECK STREAM"
        )
        operation_proc.start()
        assert timed_wait(lambda: operation_counter.value == OP_BEFORE_EXECUTE)

        message_sender(SIMPLE_MSG)
        assert timed_wait(lambda: check_counter.value > CHECK_AFTER_FETCHALL)
        assert check_counter.value == CHECK_CORRECT_RESULT
        assert check_result_len.value == 1
        check_stream_proc.join()

        operation_proc.join()
        if operation == "START":
            assert operation_counter.value == OP_AFTER_FETCHALL
            assert get_is_running(cursor, "test_stream")
        else:
            assert operation_counter.value == OP_ALREADY_STOPPED_EXCEPTION
            assert not get_is_running(cursor, "test_stream")

    finally:
        # to make sure CHECK STREAM finishes
        message_sender(SIMPLE_MSG)
        if check_stream_proc.is_alive():
            check_stream_proc.terminate()
        if operation_proc.is_alive():
            operation_proc.terminate()

def test_start_checked_stream_after_timeout(connection, stream_creator):
    cursor = connection.cursor()
    execute_and_fetch_all(
        cursor,
        stream_creator('test_stream')
    )

    timeout_ms = 2000

    def call_check():
        execute_and_fetch_all(
            connect().cursor(),
            f"CHECK STREAM test_stream TIMEOUT {timeout_ms}")

    check_stream_proc = Process(target=call_check, daemon=True)

    start = time.time()
    check_stream_proc.start()
    assert timed_wait(
        lambda: get_is_running(
            cursor, "test_stream"))
    start_stream(cursor, "test_stream")
    end = time.time()

    assert (end - start) < 1.3 * \
        timeout_ms, "The START STREAM was blocked too long"
    assert get_is_running(cursor, "test_stream")
    stop_stream(cursor, "test_stream")
