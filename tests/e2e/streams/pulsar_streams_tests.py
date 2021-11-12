#!/usr/bin/python3

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

import sys
import pytest
import mgclient
import time
from multiprocessing import Process, Value
import common

# These are the indices of the query and parameters in the result of CHECK
# STREAM query
QUERY = 0
PARAMS = 1

TRANSFORMATIONS_TO_CHECK = [
    "pulsar_transform.simple",
    "pulsar_transform.with_parameters"]

SIMPLE_MSG = b"message"


def check_vertex_exists_with_topic_and_payload(cursor, topic, payload_byte):
    decoded_payload = payload_byte.decode('utf-8')
    common.check_vertex_exists_with_properties(
        cursor, {
            'topic': f'"{common.pulsar_default_namespace_topic(topic)}"', 'payload': f'"{decoded_payload}"'})


#@pytest.mark.skip(reason="no way of currently testing this")
@pytest.mark.parametrize("transformation", TRANSFORMATIONS_TO_CHECK)
def test_simple(pulsar_client, pulsar_topics, connection, transformation):
    assert len(pulsar_topics) > 0
    cursor = connection.cursor()
    common.execute_and_fetch_all(
        cursor,
        "CREATE PULSAR STREAM test "
        f"TOPICS '{','.join(pulsar_topics)}' "
        f"TRANSFORM {transformation}",
    )
    common.start_stream(cursor, "test")
    time.sleep(5)

    for topic in pulsar_topics:
        producer = pulsar_client.create_producer(
            common.pulsar_default_namespace_topic(topic),
            send_timeout_millis=60000)
        producer.send(SIMPLE_MSG)

    for topic in pulsar_topics:
        check_vertex_exists_with_topic_and_payload(cursor, topic, SIMPLE_MSG)


#@pytest.mark.skip(reason="no way of currently testing this")
@pytest.mark.parametrize("transformation", TRANSFORMATIONS_TO_CHECK)
def test_separate_consumers(
        pulsar_client,
        pulsar_topics,
        connection,
        transformation):
    assert len(pulsar_topics) > 0
    cursor = connection.cursor()

    stream_names = []
    for topic in pulsar_topics:
        stream_name = "stream_" + topic
        stream_names.append(stream_name)
        common.execute_and_fetch_all(
            cursor,
            f"CREATE PULSAR STREAM {stream_name} "
            f"TOPICS {topic} "
            f"TRANSFORM {transformation}",
        )

    for stream_name in stream_names:
        common.start_stream(cursor, stream_name)

    time.sleep(5)

    for topic in pulsar_topics:
        producer = pulsar_client.create_producer(
            topic, send_timeout_millis=60000)
        producer.send(SIMPLE_MSG)

    for topic in pulsar_topics:
        check_vertex_exists_with_topic_and_payload(cursor, topic, SIMPLE_MSG)


#@pytest.mark.skip(reason="no way of currently testing this")
def test_start_from_latest_messages(pulsar_client, pulsar_topics, connection):
    # This test creates a stream, consumes a message, then destroys the stream. A new message is sent before the
    # stream is recreated, and additional messages after the stream was recreated. Pulsar consumer
    # should only receive message that were sent after the consumer was created. Everything
    # inbetween should be lost.
    assert len(pulsar_topics) > 0
    cursor = connection.cursor()
    common.execute_and_fetch_all(
        cursor, "CREATE PULSAR STREAM test "
        f"TOPICS {pulsar_topics[0]} "
        "TRANSFORM pulsar_transform.simple", )
    common.start_stream(cursor, "test")
    time.sleep(1)

    def assert_message_not_consumed(message):
        vertices_with_msg = common.execute_and_fetch_all(
            cursor,
            "MATCH (n: MESSAGE {" f"payload: '{message.decode('utf-8')}'" "}) RETURN n",
        )

        assert len(vertices_with_msg) == 0

    producer = pulsar_client.create_producer(
        common.pulsar_default_namespace_topic(
            pulsar_topics[0]), send_timeout_millis=60000)
    producer.send(SIMPLE_MSG)

    check_vertex_exists_with_topic_and_payload(
        cursor, pulsar_topics[0], SIMPLE_MSG)

    common.stop_stream(cursor, "test")

    next_message = b"NEXT"
    producer.send(next_message)

    assert_message_not_consumed(next_message)

    common.start_stream(cursor, "test")
    check_vertex_exists_with_topic_and_payload(
        cursor, pulsar_topics[0], next_message)
    common.stop_stream(cursor, "test")

    common.drop_stream(cursor, "test")

    lost_message = b"LOST"
    valid_messages = [b"second message", b"third message"]

    producer.send(lost_message)

    assert_message_not_consumed(lost_message)

    common.execute_and_fetch_all(
        cursor, "CREATE PULSAR STREAM test "
        f"TOPICS {pulsar_topics[0]} "
        "TRANSFORM pulsar_transform.simple", )

    for message in valid_messages:
        producer.send(message)
        assert_message_not_consumed(message)

    common.start_stream(cursor, "test")

    assert_message_not_consumed(lost_message)

    for message in valid_messages:
        check_vertex_exists_with_topic_and_payload(
            cursor, pulsar_topics[0], message)


#@pytest.mark.skip(reason="no way of currently testing this")
@pytest.mark.parametrize("transformation", TRANSFORMATIONS_TO_CHECK)
def test_check_stream(
        pulsar_client,
        pulsar_topics,
        connection,
        transformation):
    assert len(pulsar_topics) > 0
    cursor = connection.cursor()
    common.execute_and_fetch_all(
        cursor,
        "CREATE PULSAR STREAM test "
        f"TOPICS {pulsar_topics[0]} "
        f"TRANSFORM {transformation} "
        "BATCH_SIZE 1",
    )
    common.start_stream(cursor, "test")
    time.sleep(1)

    producer = pulsar_client.create_producer(
        common.pulsar_default_namespace_topic(
            pulsar_topics[0]), send_timeout_millis=60000)
    producer.send(SIMPLE_MSG)
    check_vertex_exists_with_topic_and_payload(
        cursor, pulsar_topics[0], SIMPLE_MSG)
    common.stop_stream(cursor, "test")

    messages = [b"first message", b"second message", b"third message"]
    for message in messages:
        producer.send(message)

    def check_check_stream(batch_limit):
        assert (
            transformation == "pulsar_transform.simple"
            or transformation == "pulsar_transform.with_parameters"
        )
        test_results = common.execute_and_fetch_all(
            cursor, f"CHECK STREAM test BATCH_LIMIT {batch_limit}"
        )
        assert len(test_results) == batch_limit

        for i in range(batch_limit):
            message_as_str = messages[i].decode("utf-8")
            if transformation == "pulsar_transform.simple":
                assert f"payload: '{message_as_str}'" in test_results[i][QUERY]
                assert test_results[i][PARAMS] is None
            else:
                assert test_results[i][QUERY] == (
                    "CREATE (n:MESSAGE "
                    "{payload: $payload, "
                    "topic: $topic})"
                )
                parameters = test_results[i][PARAMS]
                assert parameters["topic"] == common.pulsar_default_namespace_topic(
                    pulsar_topics[0])
                assert parameters["payload"] == message_as_str

    check_check_stream(1)
    check_check_stream(2)
    check_check_stream(3)
    common.start_stream(cursor, "test")

    for message in messages:
        check_vertex_exists_with_topic_and_payload(
            cursor, pulsar_topics[0], message)


#@pytest.mark.skip(reason="no way of currently testing this")
def test_show_streams(pulsar_client, pulsar_topics, connection):
    assert len(pulsar_topics) > 1
    cursor = connection.cursor()
    common.execute_and_fetch_all(
        cursor,
        "CREATE PULSAR STREAM default_values "
        f"TOPICS {pulsar_topics[0]} "
        f"TRANSFORM pulsar_transform.simple ",
    )

    batch_interval = 42
    batch_size = 3
    common.execute_and_fetch_all(
        cursor,
        "CREATE PULSAR STREAM complex_values "
        f"TOPICS {','.join(pulsar_topics)} "
        f"TRANSFORM pulsar_transform.with_parameters "
        f"BATCH_INTERVAL {batch_interval} "
        f"BATCH_SIZE {batch_size} ",
    )

    assert len(common.execute_and_fetch_all(cursor, "SHOW STREAMS")) == 2

    common.check_stream_info(
        cursor,
        "default_values",
        ("default_values", None, None, "pulsar_transform.simple", None, False),
    )

    common.check_stream_info(
        cursor,
        "complex_values",
        (
            "complex_values",
            batch_interval,
            batch_size,
            "pulsar_transform.with_parameters",
            None,
            False,
        ),
    )


#@pytest.mark.skip(reason="no way of currently testing this")
@pytest.mark.parametrize("operation", ["START", "STOP"])
def test_start_and_stop_during_check(
        pulsar_client,
        pulsar_topics,
        connection,
        operation):
    # This test is quite complex. The goal is to call START/STOP queries
    # while a CHECK query is waiting for its result. Because the Global
    # Interpreter Lock, running queries on multiple threads is not useful,
    # because only one of them can call Cursor::execute at a time. Therefore
    # multiple processes are used to execute the queries, because different
    # processes have different GILs.
    # The counter variables are thread- and process-safe variables to
    # synchronize between the different processes. Each value represents a
    # specific phase of the execution of the processes.
    assert len(pulsar_topics) > 1
    assert operation == "START" or operation == "STOP"
    cursor = connection.cursor()
    common.execute_and_fetch_all(
        cursor,
        "CREATE PULSAR STREAM test_stream "
        f"TOPICS {pulsar_topics[0]} "
        f"TRANSFORM pulsar_transform.simple",
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
        connection = common.connect()
        cursor = connection.cursor()
        counter.value = CHECK_BEFORE_EXECUTE
        result = common.execute_and_fetch_all(
            cursor, "CHECK STREAM test_stream")
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
        connection = common.connect()
        cursor = connection.cursor()
        counter.value = OP_BEFORE_EXECUTE
        try:
            common.execute_and_fetch_all(
                cursor, f"{operation} STREAM test_stream")
            counter.value = OP_AFTER_FETCHALL
        except mgclient.DatabaseError as e:
            if "Pulsar consumer test_stream is already stopped" in str(e):
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

        assert common.timed_wait(
            lambda: check_counter.value == CHECK_BEFORE_EXECUTE)
        assert common.timed_wait(
            lambda: common.get_is_running(
                cursor, "test_stream"))
        assert check_counter.value == CHECK_BEFORE_EXECUTE, (
            "SHOW STREAMS " "was blocked until the end of CHECK STREAM"
        )
        operation_proc.start()
        assert common.timed_wait(
            lambda: operation_counter.value == OP_BEFORE_EXECUTE)

        producer = pulsar_client.create_producer(
            common.pulsar_default_namespace_topic(
                pulsar_topics[0]), send_timeout_millis=60000)
        producer.send(SIMPLE_MSG)
        assert common.timed_wait(
            lambda: check_counter.value > CHECK_AFTER_FETCHALL)
        assert check_counter.value == CHECK_CORRECT_RESULT
        assert check_result_len.value == 1
        check_stream_proc.join()

        operation_proc.join()
        if operation == "START":
            assert operation_counter.value == OP_AFTER_FETCHALL
            assert common.get_is_running(cursor, "test_stream")
        else:
            assert operation_counter.value == OP_ALREADY_STOPPED_EXCEPTION
            assert not common.get_is_running(cursor, "test_stream")

    finally:
        # to make sure CHECK STREAM finishes
        producer.send(SIMPLE_MSG)
        if check_stream_proc.is_alive():
            check_stream_proc.terminate()
        if operation_proc.is_alive():
            operation_proc.terminate()


#@pytest.mark.skip(reason="no way of currently testing this")
def test_check_already_started_stream(pulsar_topics, connection):
    assert len(pulsar_topics) > 0
    cursor = connection.cursor()

    common.execute_and_fetch_all(
        cursor,
        "CREATE PULSAR STREAM started_stream "
        f"TOPICS {pulsar_topics[0]} "
        f"TRANSFORM pulsar_transform.simple",
    )
    common.start_stream(cursor, "started_stream")

    with pytest.raises(mgclient.DatabaseError):
        common.execute_and_fetch_all(cursor, "CHECK STREAM started_stream")


#@pytest.mark.skip(reason="no way of currently testing this")
def test_start_checked_stream_after_timeout(pulsar_topics, connection):
    cursor = connection.cursor()
    common.execute_and_fetch_all(
        cursor,
        "CREATE PULSAR STREAM test_stream "
        f"TOPICS {pulsar_topics[0]} "
        f"TRANSFORM pulsar_transform.simple",
    )

    timeout_ms = 2000

    def call_check():
        common.execute_and_fetch_all(
            common.connect().cursor(),
            f"CHECK STREAM test_stream TIMEOUT {timeout_ms}")

    check_stream_proc = Process(target=call_check, daemon=True)

    start = time.time()
    check_stream_proc.start()
    assert common.timed_wait(
        lambda: common.get_is_running(
            cursor, "test_stream"))
    common.start_stream(cursor, "test_stream")
    end = time.time()

    assert (end - start) < 1.3 * \
        timeout_ms, "The START STREAM was blocked too long"
    assert common.get_is_running(cursor, "test_stream")
    common.stop_stream(cursor, "test_stream")


#@pytest.mark.skip(reason="no way of currently testing this")
def test_restart_after_error(pulsar_client, pulsar_topics, connection):
    cursor = connection.cursor()
    common.execute_and_fetch_all(
        cursor,
        "CREATE PULSAR STREAM test_stream "
        f"TOPICS {pulsar_topics[0]} "
        f"TRANSFORM pulsar_transform.query",
    )

    common.start_stream(cursor, "test_stream")
    time.sleep(1)

    producer = pulsar_client.create_producer(
        common.pulsar_default_namespace_topic(
            pulsar_topics[0]), send_timeout_millis=60000)
    producer.send(SIMPLE_MSG)
    assert common.timed_wait(
        lambda: not common.get_is_running(
            cursor, "test_stream"))

    common.start_stream(cursor, "test_stream")
    time.sleep(1)
    producer.send(b"CREATE (n:VERTEX { id : 42 })")
    assert common.check_one_result_row(
        cursor, "MATCH (n:VERTEX { id : 42 }) RETURN n")


#@pytest.mark.skip(reason="no way of currently testing this")
@pytest.mark.parametrize("transformation", TRANSFORMATIONS_TO_CHECK)
def test_service_url(pulsar_client, pulsar_topics, connection, transformation):
    assert len(pulsar_topics) > 0
    cursor = connection.cursor()
    local = "pulsar://127.0.0.1:6650"
    common.execute_and_fetch_all(
        cursor,
        "CREATE PULSAR STREAM test "
        f"TOPICS {','.join(pulsar_topics)} "
        f"TRANSFORM {transformation} "
        f"SERVICE_URL '{local}'",
    )
    common.start_stream(cursor, "test")
    time.sleep(5)

    for topic in pulsar_topics:
        producer = pulsar_client.create_producer(
            common.pulsar_default_namespace_topic(topic),
            send_timeout_millis=60000)
        producer.send(SIMPLE_MSG)

    for topic in pulsar_topics:
        check_vertex_exists_with_topic_and_payload(cursor, topic, SIMPLE_MSG)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
