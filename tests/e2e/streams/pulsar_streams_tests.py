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

import os
import sys
import time
from multiprocessing import Process, Value

import common
import mgclient
import pytest

TRANSFORMATIONS_TO_CHECK = ["pulsar_transform.simple", "pulsar_transform.with_parameters"]
PULSAR_HOSTNAME=os.getenv("PULSAR_HOSTNAME", "127.0.0.1")


def check_vertex_exists_with_topic_and_payload(cursor, topic, payload_byte):
    decoded_payload = payload_byte.decode("utf-8")
    common.check_vertex_exists_with_properties(
        cursor, {"topic": f'"{common.pulsar_default_namespace_topic(topic)}"', "payload": f'"{decoded_payload}"'}
    )


@pytest.mark.parametrize("transformation", TRANSFORMATIONS_TO_CHECK)
def test_simple(pulsar_client, pulsar_topics, connection, transformation):
    assert len(pulsar_topics) > 0
    cursor = connection.cursor()
    common.execute_and_fetch_all(
        cursor,
        f"CREATE PULSAR STREAM test TOPICS '{','.join(pulsar_topics)}' TRANSFORM {transformation}",
    )
    common.start_stream(cursor, "test")

    for topic in pulsar_topics:
        producer = pulsar_client.create_producer(
            common.pulsar_default_namespace_topic(topic), send_timeout_millis=60000
        )
        producer.send(common.SIMPLE_MSG)

    for topic in pulsar_topics:
        check_vertex_exists_with_topic_and_payload(cursor, topic, common.SIMPLE_MSG)


@pytest.mark.parametrize("transformation", TRANSFORMATIONS_TO_CHECK)
def test_separate_consumers(pulsar_client, pulsar_topics, connection, transformation):
    assert len(pulsar_topics) > 0
    cursor = connection.cursor()

    stream_names = []
    for topic in pulsar_topics:
        stream_name = "stream_" + topic
        stream_names.append(stream_name)
        common.execute_and_fetch_all(
            cursor,
            f"CREATE PULSAR STREAM {stream_name} TOPICS {topic} TRANSFORM {transformation}",
        )

    for stream_name in stream_names:
        common.start_stream(cursor, stream_name)

    for topic in pulsar_topics:
        producer = pulsar_client.create_producer(topic, send_timeout_millis=60000)
        producer.send(common.SIMPLE_MSG)

    for topic in pulsar_topics:
        check_vertex_exists_with_topic_and_payload(cursor, topic, common.SIMPLE_MSG)


def test_start_from_latest_messages(pulsar_client, pulsar_topics, connection):
    # This test creates a stream, consumes a message, then destroys the stream. A new message is sent before the
    # stream is recreated, and additional messages after the stream was recreated. Pulsar consumer
    # should only receive message that were sent after the consumer was created. Everything
    # inbetween should be lost. Additionally, we check that consumer continues from the correct message
    # after stopping and starting again.
    assert len(pulsar_topics) > 0

    def assert_message_not_consumed(message):
        vertices_with_msg = common.execute_and_fetch_all(
            cursor,
            f"MATCH (n: MESSAGE {{payload: '{message.decode('utf-8')}'}}) RETURN n",
        )

        assert len(vertices_with_msg) == 0

    cursor = connection.cursor()
    common.execute_and_fetch_all(
        cursor,
        f"CREATE PULSAR STREAM test TOPICS {pulsar_topics[0]} TRANSFORM pulsar_transform.simple",
    )
    common.start_stream(cursor, "test")

    producer = pulsar_client.create_producer(
        common.pulsar_default_namespace_topic(pulsar_topics[0]), send_timeout_millis=60000
    )
    producer.send(common.SIMPLE_MSG)

    check_vertex_exists_with_topic_and_payload(cursor, pulsar_topics[0], common.SIMPLE_MSG)

    common.stop_stream(cursor, "test")

    NEXT_MESSAGE = b"NEXT"
    producer.send(NEXT_MESSAGE)

    assert_message_not_consumed(NEXT_MESSAGE)

    common.start_stream(cursor, "test")
    check_vertex_exists_with_topic_and_payload(cursor, pulsar_topics[0], NEXT_MESSAGE)
    common.stop_stream(cursor, "test")

    common.drop_stream(cursor, "test")

    LOST_MESSAGE = b"LOST"
    VALID_MESSAGES = [b"second message", b"third message"]

    producer.send(LOST_MESSAGE)

    assert_message_not_consumed(LOST_MESSAGE)

    common.execute_and_fetch_all(
        cursor,
        f"CREATE PULSAR STREAM test TOPICS {pulsar_topics[0]} TRANSFORM pulsar_transform.simple",
    )

    for message in VALID_MESSAGES:
        producer.send(message)
        assert_message_not_consumed(message)

    common.start_stream(cursor, "test", sleep=False)

    assert_message_not_consumed(LOST_MESSAGE)

    for message in VALID_MESSAGES:
        check_vertex_exists_with_topic_and_payload(cursor, pulsar_topics[0], message)


@pytest.mark.parametrize("transformation", TRANSFORMATIONS_TO_CHECK)
def test_check_stream(pulsar_client, pulsar_topics, connection, transformation):
    assert len(pulsar_topics) > 0
    BATCH_SIZE = 1
    INDEX_Of_FIRST_BATCH = 0
    cursor = connection.cursor()
    common.execute_and_fetch_all(
        cursor,
        f"CREATE PULSAR STREAM test TOPICS {pulsar_topics[0]} TRANSFORM {transformation} BATCH_SIZE {BATCH_SIZE}",
    )
    common.start_stream(cursor, "test")

    producer = pulsar_client.create_producer(
        common.pulsar_default_namespace_topic(pulsar_topics[0]), send_timeout_millis=60000
    )
    producer.send(common.SIMPLE_MSG)
    check_vertex_exists_with_topic_and_payload(cursor, pulsar_topics[0], common.SIMPLE_MSG)
    common.stop_stream(cursor, "test")

    MESSAGES = [b"first message", b"second message", b"third message"]
    for message in MESSAGES:
        producer.send(message)

    def check_check_stream(batch_limit):
        assert transformation == "pulsar_transform.simple" or transformation == "pulsar_transform.with_parameters"
        test_results = common.execute_and_fetch_all(cursor, f"CHECK STREAM test BATCH_LIMIT {batch_limit}")
        assert len(test_results) == batch_limit

        for i in range(batch_limit):
            message_as_str = MESSAGES[i].decode("utf-8")
            assert (
                BATCH_SIZE == 1
            )  # If batch size != 1, then the usage of INDEX_Of_FIRST_BATCH must change: the result will have a list of queries (pair<parameters,query>)

            if transformation == "pulsar_transform.simple":
                assert (
                    f"payload: '{message_as_str}'"
                    in test_results[i][common.QUERIES][INDEX_Of_FIRST_BATCH][common.QUERY_LITERAL]
                )
                assert test_results[i][common.QUERIES][INDEX_Of_FIRST_BATCH][common.PARAMETERS_LITERAL] is None
            else:
                assert (
                    f"payload: $payload" in test_results[i][common.QUERIES][INDEX_Of_FIRST_BATCH][common.QUERY_LITERAL]
                    and f"topic: $topic" in test_results[i][common.QUERIES][INDEX_Of_FIRST_BATCH][common.QUERY_LITERAL]
                )
                parameters = test_results[i][common.QUERIES][INDEX_Of_FIRST_BATCH][common.PARAMETERS_LITERAL]
                assert parameters["topic"] == common.pulsar_default_namespace_topic(pulsar_topics[0])
                assert parameters["payload"] == message_as_str

    check_check_stream(1)
    check_check_stream(2)
    check_check_stream(3)
    common.start_stream(cursor, "test")

    for message in MESSAGES:
        check_vertex_exists_with_topic_and_payload(cursor, pulsar_topics[0], message)


def test_info_procedure(pulsar_client, pulsar_topics, connection):
    cursor = connection.cursor()
    STREAM_NAME = "test_stream"
    common.execute_and_fetch_all(
        cursor,
        f"CREATE PULSAR STREAM {STREAM_NAME} TOPICS {','.join(pulsar_topics)} TRANSFORM pulsar_transform.simple ",
    )

    stream_info = common.execute_and_fetch_all(cursor, f"CALL mg.pulsar_stream_info('{STREAM_NAME}') YIELD *")

    expected_stream_info = [(common.PULSAR_SERVICE_URL, pulsar_topics)]
    common.validate_info(stream_info, expected_stream_info)


def test_show_streams(pulsar_client, pulsar_topics, connection):
    assert len(pulsar_topics) > 1
    cursor = connection.cursor()
    common.execute_and_fetch_all(
        cursor,
        f"CREATE PULSAR STREAM default_values TOPICS {pulsar_topics[0]} TRANSFORM pulsar_transform.simple ",
    )

    BATCH_INTERVAL = 42
    BATCH_SIZE = 3
    common.execute_and_fetch_all(
        cursor,
        f"CREATE PULSAR STREAM complex_values TOPICS {','.join(pulsar_topics)} TRANSFORM pulsar_transform.with_parameters BATCH_INTERVAL {BATCH_INTERVAL} BATCH_SIZE {BATCH_SIZE} ",
    )

    assert len(common.execute_and_fetch_all(cursor, "SHOW STREAMS")) == 2

    common.check_stream_info(
        cursor,
        "default_values",
        ("default_values", "pulsar", 100, 1000, "pulsar_transform.simple", None, False),
    )

    common.check_stream_info(
        cursor,
        "complex_values",
        (
            "complex_values",
            "pulsar",
            BATCH_INTERVAL,
            BATCH_SIZE,
            "pulsar_transform.with_parameters",
            None,
            False,
        ),
    )


@pytest.mark.parametrize("operation", ["START", "STOP"])
def test_start_and_stop_during_check(pulsar_client, pulsar_topics, connection, operation):
    assert len(pulsar_topics) > 1
    BATCH_SIZE = 1

    def stream_creator(stream_name):
        return f"CREATE PULSAR STREAM {stream_name} TOPICS {pulsar_topics[0]} TRANSFORM pulsar_transform.simple BATCH_SIZE {BATCH_SIZE}"

    producer = pulsar_client.create_producer(
        common.pulsar_default_namespace_topic(pulsar_topics[0]), send_timeout_millis=60000
    )

    def message_sender(msg):
        producer.send(msg)

    common.test_start_and_stop_during_check(
        operation,
        connection,
        stream_creator,
        message_sender,
        "Pulsar consumer test_stream is already stopped",
        BATCH_SIZE,
    )


def test_check_already_started_stream(pulsar_topics, connection):
    assert len(pulsar_topics) > 0
    cursor = connection.cursor()

    common.execute_and_fetch_all(
        cursor,
        f"CREATE PULSAR STREAM started_stream TOPICS {pulsar_topics[0]} TRANSFORM pulsar_transform.simple",
    )
    common.start_stream(cursor, "started_stream")

    with pytest.raises(mgclient.DatabaseError):
        common.execute_and_fetch_all(cursor, "CHECK STREAM started_stream")


def test_start_checked_stream_after_timeout(pulsar_topics, connection):
    def stream_creator(stream_name):
        return f"CREATE PULSAR STREAM {stream_name} TOPICS {pulsar_topics[0]} TRANSFORM pulsar_transform.simple"

    common.test_start_checked_stream_after_timeout(connection, stream_creator)


def test_restart_after_error(pulsar_client, pulsar_topics, connection):
    cursor = connection.cursor()
    common.execute_and_fetch_all(
        cursor,
        f"CREATE PULSAR STREAM test_stream TOPICS {pulsar_topics[0]} TRANSFORM pulsar_transform.query",
    )

    common.start_stream(cursor, "test_stream")

    producer = pulsar_client.create_producer(
        common.pulsar_default_namespace_topic(pulsar_topics[0]), send_timeout_millis=60000
    )
    producer.send(common.SIMPLE_MSG)
    assert common.timed_wait(lambda: not common.get_is_running(cursor, "test_stream"))

    common.start_stream(cursor, "test_stream")
    producer.send(b"CREATE (n:VERTEX { id : 42 })")
    assert common.check_one_result_row(cursor, "MATCH (n:VERTEX { id : 42 }) RETURN n")


@pytest.mark.parametrize("transformation", TRANSFORMATIONS_TO_CHECK)
def test_service_url(pulsar_client, pulsar_topics, connection, transformation):
    assert len(pulsar_topics) > 0
    cursor = connection.cursor()
    LOCAL = f"pulsar://{PULSAR_HOSTNAME}:6650"
    common.execute_and_fetch_all(
        cursor,
        f"CREATE PULSAR STREAM test TOPICS {','.join(pulsar_topics)} TRANSFORM {transformation} SERVICE_URL '{LOCAL}'",
    )
    common.start_stream(cursor, "test")

    for topic in pulsar_topics:
        producer = pulsar_client.create_producer(
            common.pulsar_default_namespace_topic(topic), send_timeout_millis=60000
        )
        producer.send(common.SIMPLE_MSG)

    for topic in pulsar_topics:
        check_vertex_exists_with_topic_and_payload(cursor, topic, common.SIMPLE_MSG)


def test_start_stream_with_batch_limit(pulsar_client, pulsar_topics, connection):
    assert len(pulsar_topics) > 1
    STREAM_NAME = "test_start_stream_with_batch_limit"

    def stream_creator():
        return f"CREATE PULSAR STREAM {STREAM_NAME} TOPICS {pulsar_topics[0]} TRANSFORM pulsar_transform.simple BATCH_SIZE 1"

    producer = pulsar_client.create_producer(
        common.pulsar_default_namespace_topic(pulsar_topics[0]), send_timeout_millis=60000
    )

    def messages_sender(nof_messages):
        for _ in range(nof_messages):
            producer.send(common.SIMPLE_MSG)

    common.test_start_stream_with_batch_limit(connection, STREAM_NAME, stream_creator, messages_sender)


def test_start_stream_with_batch_limit_timeout(pulsar_client, pulsar_topics, connection):
    assert len(pulsar_topics) > 1

    def stream_creator(stream_name):
        return f"CREATE PULSAR STREAM {stream_name} TOPICS {pulsar_topics[0]} TRANSFORM pulsar_transform.simple BATCH_SIZE 1"

    common.test_start_stream_with_batch_limit_timeout(connection, stream_creator)


def test_start_stream_with_batch_limit_reaching_timeout(pulsar_client, pulsar_topics, connection):
    assert len(pulsar_topics) > 1

    def stream_creator(stream_name, batch_size):
        return f"CREATE PULSAR STREAM {stream_name} TOPICS {pulsar_topics[0]} TRANSFORM pulsar_transform.simple BATCH_SIZE {batch_size}"

    common.test_start_stream_with_batch_limit_reaching_timeout(connection, stream_creator)


def test_start_stream_with_batch_limit_while_check_running(pulsar_client, pulsar_topics, connection):
    assert len(pulsar_topics) > 0

    def stream_creator(stream_name):
        return f"CREATE PULSAR STREAM {stream_name} TOPICS {pulsar_topics[0]} TRANSFORM pulsar_transform.simple BATCH_SIZE 1"

    producer = pulsar_client.create_producer(
        common.pulsar_default_namespace_topic(pulsar_topics[0]), send_timeout_millis=60000
    )

    def message_sender(message):
        producer.send(message)

    common.test_start_stream_with_batch_limit_while_check_running(connection, stream_creator, message_sender)


def test_check_while_stream_with_batch_limit_running(pulsar_client, pulsar_topics, connection):
    assert len(pulsar_topics) > 0

    def stream_creator(stream_name):
        return f"CREATE PULSAR STREAM {stream_name} TOPICS {pulsar_topics[0]} TRANSFORM pulsar_transform.simple BATCH_SIZE 1"

    producer = pulsar_client.create_producer(
        common.pulsar_default_namespace_topic(pulsar_topics[0]), send_timeout_millis=60000
    )

    def message_sender(message):
        producer.send(message)

    common.test_check_while_stream_with_batch_limit_running(connection, stream_creator, message_sender)


def test_check_stream_same_number_of_queries_than_messages(pulsar_client, pulsar_topics, connection):
    assert len(pulsar_topics) > 0

    TRANSFORMATION = "common_transform.check_stream_no_filtering"

    def stream_creator(stream_name, batch_size):
        return f"CREATE PULSAR STREAM {stream_name} TOPICS {pulsar_topics[0]} TRANSFORM {TRANSFORMATION} BATCH_INTERVAL 3000 BATCH_SIZE {batch_size} "

    producer = pulsar_client.create_producer(
        common.pulsar_default_namespace_topic(pulsar_topics[0]), send_timeout_millis=60000
    )

    def message_sender(msg):
        producer.send(msg)

    common.test_check_stream_same_number_of_queries_than_messages(connection, stream_creator, message_sender)


def test_check_stream_different_number_of_queries_than_messages(pulsar_client, pulsar_topics, connection):
    assert len(pulsar_topics) > 0

    TRANSFORMATION = "common_transform.check_stream_with_filtering"

    def stream_creator(stream_name, batch_size):
        return f"CREATE PULSAR STREAM {stream_name} TOPICS {pulsar_topics[0]} TRANSFORM {TRANSFORMATION} BATCH_INTERVAL 3000 BATCH_SIZE  {batch_size} "

    producer = pulsar_client.create_producer(
        common.pulsar_default_namespace_topic(pulsar_topics[0]), send_timeout_millis=60000
    )

    def message_sender(msg):
        producer.send(msg)

    common.test_check_stream_different_number_of_queries_than_messages(connection, stream_creator, message_sender)


def test_start_stream_with_batch_limit_with_invalid_batch_limit(pulsar_client, pulsar_topics, connection):
    assert len(pulsar_topics) > 0

    def stream_creator(stream_name):
        return f"CREATE PULSAR STREAM {stream_name} TOPICS {pulsar_topics[0]} TRANSFORM pulsar_transform.simple BATCH_SIZE 1"

    common.test_start_stream_with_batch_limit_with_invalid_batch_limit(connection, stream_creator)


def test_check_stream_with_batch_limit_with_invalid_batch_limit(pulsar_client, pulsar_topics, connection):
    assert len(pulsar_topics) > 0

    def stream_creator(stream_name):
        return f"CREATE PULSAR STREAM {stream_name} TOPICS {pulsar_topics[0]} TRANSFORM pulsar_transform.simple BATCH_SIZE 1"

    common.test_check_stream_with_batch_limit_with_invalid_batch_limit(connection, stream_creator)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
