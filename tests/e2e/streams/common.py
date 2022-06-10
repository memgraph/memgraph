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

from multiprocessing import Manager, Process, Value

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
QUERIES = 0
RAWMESSAGES = 1
PARAMETERS_LITERAL = "parameters"
QUERY_LITERAL = "query"

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
    SECONDS = 10

    while True:
        current_time = time.time()
        elapsed_time = current_time - start_time

        if elapsed_time > SECONDS:
            return False

        if fun():
            return True

        time.sleep(0.1)


def check_one_result_row(cursor, query):
    start_time = time.time()
    SECONDS = 10

    while True:
        current_time = time.time()
        elapsed_time = current_time - start_time

        if elapsed_time > SECONDS:
            return False

        cursor.execute(query)
        results = cursor.fetchall()
        if len(results) < 1:
            time.sleep(0.1)
            continue

        return len(results) == 1


def check_vertex_exists_with_properties(cursor, properties):
    properties_string = ", ".join([f"{k}: {v}" for k, v in properties.items()])
    assert check_one_result_row(
        cursor,
        f"MATCH (n: MESSAGE {{{properties_string}}}) RETURN n",
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


def validate_info(actual_stream_info, expected_stream_info):
    assert len(actual_stream_info) == len(expected_stream_info)
    for info, expected_info in zip(actual_stream_info, expected_stream_info):
        assert info == expected_info


def check_stream_info(cursor, stream_name, expected_stream_info):
    stream_info = get_stream_info(cursor, stream_name)
    validate_info(stream_info, expected_stream_info)


def kafka_check_vertex_exists_with_topic_and_payload(cursor, topic, payload_bytes):
    decoded_payload = payload_bytes.decode("utf-8")
    check_vertex_exists_with_properties(cursor, {"topic": f'"{topic}"', "payload": f'"{decoded_payload}"'})


PULSAR_SERVICE_URL = "pulsar://127.0.0.1:6650"


def pulsar_default_namespace_topic(topic):
    return f"persistent://public/default/{topic}"


def test_start_and_stop_during_check(
    operation, connection, stream_creator, message_sender, already_stopped_error, batchSize
):
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
    assert batchSize == 1
    cursor = connection.cursor()
    execute_and_fetch_all(cursor, stream_creator("test_stream"))

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
        if (
            len(result) > 0 and "payload: 'message'" in result[0][QUERIES][0][QUERY_LITERAL]
        ):  # The 0 is only correct because batchSize is 1
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

    check_stream_proc = Process(target=call_check, daemon=True, args=(check_counter, check_result_len))
    operation_proc = Process(target=call_operation, daemon=True, args=(operation_counter,))

    try:
        check_stream_proc.start()

        time.sleep(0.5)

        assert timed_wait(lambda: check_counter.value == CHECK_BEFORE_EXECUTE)
        assert timed_wait(lambda: get_is_running(cursor, "test_stream"))
        assert check_counter.value == CHECK_BEFORE_EXECUTE, "SHOW STREAMS " "was blocked until the end of CHECK STREAM"
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
    execute_and_fetch_all(cursor, stream_creator("test_stream"))

    TIMEOUT_MS = 2000

    def call_check():
        execute_and_fetch_all(connect().cursor(), f"CHECK STREAM test_stream TIMEOUT {TIMEOUT_MS}")

    check_stream_proc = Process(target=call_check, daemon=True)

    start = time.time()
    check_stream_proc.start()
    assert timed_wait(lambda: get_is_running(cursor, "test_stream"))
    start_stream(cursor, "test_stream")
    end = time.time()

    assert (end - start) < 1.3 * TIMEOUT_MS, "The START STREAM was blocked too long"
    assert get_is_running(cursor, "test_stream")
    stop_stream(cursor, "test_stream")


def test_check_stream_same_number_of_queries_than_messages(connection, stream_creator, message_sender):
    BATCH_SIZE = 2
    BATCH_LIMIT = 3
    STREAM_NAME = "test_stream"
    cursor = connection.cursor()
    execute_and_fetch_all(cursor, stream_creator(STREAM_NAME, BATCH_SIZE))
    time.sleep(2)

    test_results = Manager().Namespace()

    def check_stream(stream_name, batch_limit):
        connection = connect()
        cursor = connection.cursor()
        test_results.value = execute_and_fetch_all(cursor, f"CHECK STREAM {stream_name} BATCH_LIMIT {batch_limit} ")

    check_stream_proc = Process(target=check_stream, args=(STREAM_NAME, BATCH_LIMIT))
    check_stream_proc.start()
    time.sleep(2)

    MESSAGES = [b"01", b"02", b"03", b"04", b"05", b"06"]
    for message in MESSAGES:
        message_sender(message)

    check_stream_proc.join()

    # # Transformation does not do any filtering and simply create queries as "Messages: {contentOfMessage}". Queries should be like:
    # # -Batch 1: [{parameters: {"value": "Parameter: 01"}, query: "Message: 01"},
    # #            {parameters: {"value": "Parameter: 02"}, query: "Message: 02"}]
    # # -Batch 2: [{parameters: {"value": "Parameter: 03"}, query: "Message: 03"},
    # #            {parameters: {"value": "Parameter: 04"}, query: "Message: 04"}]
    # # -Batch 3: [{parameters: {"value": "Parameter: 05"}, query: "Message: 05"},
    # #            {parameters: {"value": "Parameter: 06"}, query: "Message: 06"}]

    assert len(test_results.value) == BATCH_LIMIT

    expected_queries_and_raw_messages_1 = (
        [  # queries
            {PARAMETERS_LITERAL: {"value": "Parameter: 01"}, QUERY_LITERAL: "Message: 01"},
            {PARAMETERS_LITERAL: {"value": "Parameter: 02"}, QUERY_LITERAL: "Message: 02"},
        ],
        ["01", "02"],  # raw message
    )

    expected_queries_and_raw_messages_2 = (
        [  # queries
            {PARAMETERS_LITERAL: {"value": "Parameter: 03"}, QUERY_LITERAL: "Message: 03"},
            {PARAMETERS_LITERAL: {"value": "Parameter: 04"}, QUERY_LITERAL: "Message: 04"},
        ],
        ["03", "04"],  # raw message
    )

    expected_queries_and_raw_messages_3 = (
        [  # queries
            {PARAMETERS_LITERAL: {"value": "Parameter: 05"}, QUERY_LITERAL: "Message: 05"},
            {PARAMETERS_LITERAL: {"value": "Parameter: 06"}, QUERY_LITERAL: "Message: 06"},
        ],
        ["05", "06"],  # raw message
    )

    assert expected_queries_and_raw_messages_1 == test_results.value[0]
    assert expected_queries_and_raw_messages_2 == test_results.value[1]
    assert expected_queries_and_raw_messages_3 == test_results.value[2]


def test_check_stream_different_number_of_queries_than_messages(connection, stream_creator, message_sender):
    BATCH_SIZE = 2
    BATCH_LIMIT = 3
    STREAM_NAME = "test_stream"
    cursor = connection.cursor()
    execute_and_fetch_all(cursor, stream_creator(STREAM_NAME, BATCH_SIZE))
    time.sleep(2)

    results = Manager().Namespace()

    def check_stream(stream_name, batch_limit):
        connection = connect()
        cursor = connection.cursor()
        results.value = execute_and_fetch_all(cursor, f"CHECK STREAM {stream_name} BATCH_LIMIT {batch_limit} ")

    check_stream_proc = Process(target=check_stream, args=(STREAM_NAME, BATCH_LIMIT))
    check_stream_proc.start()
    time.sleep(2)

    MESSAGES = [b"a_01", b"a_02", b"03", b"04", b"b_05", b"06"]
    for message in MESSAGES:
        message_sender(message)

    check_stream_proc.join()

    # Transformation does some filtering: if message contains "a", it is ignored.
    # Transformation also has special rule to create query if message is "b": it create more queries.
    #
    # Queries should be like:
    # -Batch 1: []
    # -Batch 2: [{parameters: {"value": "Parameter: 03"}, query: "Message: 03"},
    #            {parameters: {"value": "Parameter: 04"}, query: "Message: 04"}]
    # -Batch 3: [{parameters: {"value": "Parameter: 05"}, query: "Message: 05"},
    #            {parameters: {"value": "Parameter: extra_05"}, query: "Message: extra_05"}
    #            {parameters: {"value": "Parameter: 06"}, query: "Message: 06"}]

    assert len(results.value) == BATCH_LIMIT

    expected_queries_and_raw_messages_1 = (
        [],  # queries
        ["a_01", "a_02"],  # raw message
    )

    expected_queries_and_raw_messages_2 = (
        [  # queries
            {PARAMETERS_LITERAL: {"value": "Parameter: 03"}, QUERY_LITERAL: "Message: 03"},
            {PARAMETERS_LITERAL: {"value": "Parameter: 04"}, QUERY_LITERAL: "Message: 04"},
        ],
        ["03", "04"],  # raw message
    )

    expected_queries_and_raw_messages_3 = (
        [  # queries
            {PARAMETERS_LITERAL: {"value": "Parameter: b_05"}, QUERY_LITERAL: "Message: b_05"},
            {
                PARAMETERS_LITERAL: {"value": "Parameter: extra_b_05"},
                QUERY_LITERAL: "Message: extra_b_05",
            },
            {PARAMETERS_LITERAL: {"value": "Parameter: 06"}, QUERY_LITERAL: "Message: 06"},
        ],
        ["b_05", "06"],  # raw message
    )

    assert expected_queries_and_raw_messages_1 == results.value[0]
    assert expected_queries_and_raw_messages_2 == results.value[1]
    assert expected_queries_and_raw_messages_3 == results.value[2]
