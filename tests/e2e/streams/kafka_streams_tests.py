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
from mg_utils import mg_sleep_and_assert
from multiprocessing import Process, Value
import common

TRANSFORMATIONS_TO_CHECK_C = ["c_transformations.empty_transformation"]

TRANSFORMATIONS_TO_CHECK_PY = ["kafka_transform.simple", "kafka_transform.with_parameters"]


@pytest.mark.parametrize("transformation", TRANSFORMATIONS_TO_CHECK_PY)
def test_simple(kafka_producer, kafka_topics, connection, transformation):
    assert len(kafka_topics) > 0
    cursor = connection.cursor()
    common.execute_and_fetch_all(
        cursor,
        f"CREATE KAFKA STREAM test TOPICS {','.join(kafka_topics)} TRANSFORM {transformation}",
    )
    common.start_stream(cursor, "test")
    time.sleep(5)

    for topic in kafka_topics:
        kafka_producer.send(topic, common.SIMPLE_MSG).get(timeout=60)

    for topic in kafka_topics:
        common.kafka_check_vertex_exists_with_topic_and_payload(cursor, topic, common.SIMPLE_MSG)


@pytest.mark.parametrize("transformation", TRANSFORMATIONS_TO_CHECK_PY)
def test_separate_consumers(kafka_producer, kafka_topics, connection, transformation):
    assert len(kafka_topics) > 0
    cursor = connection.cursor()

    stream_names = []
    for topic in kafka_topics:
        stream_name = "stream_" + topic
        stream_names.append(stream_name)
        common.execute_and_fetch_all(
            cursor,
            f"CREATE KAFKA STREAM {stream_name} TOPICS {topic} TRANSFORM {transformation}",
        )

    for stream_name in stream_names:
        common.start_stream(cursor, stream_name)

    time.sleep(5)

    for topic in kafka_topics:
        kafka_producer.send(topic, common.SIMPLE_MSG).get(timeout=60)

    for topic in kafka_topics:
        common.kafka_check_vertex_exists_with_topic_and_payload(cursor, topic, common.SIMPLE_MSG)


def test_start_from_last_committed_offset(kafka_producer, kafka_topics, connection):
    # This test creates a stream, consumes a message to have a committed
    # offset, then destroys the stream. A new message is sent before the
    # stream is recreated and then restarted. This simulates when Memgraph is
    # stopped (stream is destroyed) and then restarted (stream is recreated).
    # This is of course not as good as restarting memgraph would be, but
    # restarting Memgraph during a single workload cannot be done currently.
    assert len(kafka_topics) > 0
    cursor = connection.cursor()
    common.execute_and_fetch_all(
        cursor,
        f"CREATE KAFKA STREAM test TOPICS {kafka_topics[0]} TRANSFORM kafka_transform.simple",
    )
    common.start_stream(cursor, "test")
    time.sleep(1)

    kafka_producer.send(kafka_topics[0], common.SIMPLE_MSG).get(timeout=60)

    common.kafka_check_vertex_exists_with_topic_and_payload(cursor, kafka_topics[0], common.SIMPLE_MSG)

    common.stop_stream(cursor, "test")
    common.drop_stream(cursor, "test")

    messages = [b"second message", b"third message"]
    for message in messages:
        kafka_producer.send(kafka_topics[0], message).get(timeout=60)

    for message in messages:
        vertices_with_msg = common.execute_and_fetch_all(
            cursor,
            f"MATCH (n: MESSAGE {{payload: '{message.decode('utf-8')}'}}) RETURN n",
        )

        assert len(vertices_with_msg) == 0

    common.execute_and_fetch_all(
        cursor,
        f"CREATE KAFKA STREAM test TOPICS {kafka_topics[0]} TRANSFORM kafka_transform.simple",
    )
    common.start_stream(cursor, "test")

    for message in messages:
        common.kafka_check_vertex_exists_with_topic_and_payload(cursor, kafka_topics[0], message)


@pytest.mark.parametrize("transformation", TRANSFORMATIONS_TO_CHECK_PY)
def test_check_stream(kafka_producer, kafka_topics, connection, transformation):
    assert len(kafka_topics) > 0
    BATCH_SIZE = 1
    INDEX_OF_FIRST_BATCH = 0
    cursor = connection.cursor()
    common.execute_and_fetch_all(
        cursor,
        f"CREATE KAFKA STREAM test TOPICS {kafka_topics[0]} TRANSFORM {transformation} BATCH_SIZE {BATCH_SIZE}",
    )
    common.start_stream(cursor, "test")
    time.sleep(1)

    kafka_producer.send(kafka_topics[0], common.SIMPLE_MSG).get(timeout=60)
    common.stop_stream(cursor, "test")

    messages = [b"first message", b"second message", b"third message"]
    for message in messages:
        kafka_producer.send(kafka_topics[0], message).get(timeout=60)

    def check_check_stream(batch_limit):
        assert transformation == "kafka_transform.simple" or transformation == "kafka_transform.with_parameters"
        test_results = common.execute_and_fetch_all(cursor, f"CHECK STREAM test BATCH_LIMIT {batch_limit}")
        assert len(test_results) == batch_limit

        for i in range(batch_limit):
            message_as_str = messages[i].decode("utf-8")
            assert (
                BATCH_SIZE == 1
            )  # If batch size != 1, then the usage of INDEX_OF_FIRST_BATCH must change: the result will have a list of queries (pair<parameters,query>)

            if transformation == "kafka_transform.simple":
                assert (
                    f"payload: '{message_as_str}'"
                    in test_results[i][common.QUERIES][INDEX_OF_FIRST_BATCH][common.QUERY_LITERAL]
                )
                assert test_results[i][common.QUERIES][INDEX_OF_FIRST_BATCH][common.PARAMETERS_LITERAL] is None
            else:
                assert (
                    f"payload: $payload" in test_results[i][common.QUERIES][INDEX_OF_FIRST_BATCH][common.QUERY_LITERAL]
                    and f"topic: $topic" in test_results[i][common.QUERIES][INDEX_OF_FIRST_BATCH][common.QUERY_LITERAL]
                )
                parameters = test_results[i][common.QUERIES][INDEX_OF_FIRST_BATCH][common.PARAMETERS_LITERAL]
                # this is not a very sophisticated test, but checks if
                # timestamp has some kind of value
                assert parameters["timestamp"] > 1000000000000
                assert parameters["topic"] == kafka_topics[0]
                assert parameters["payload"] == message_as_str

    check_check_stream(1)
    check_check_stream(2)
    check_check_stream(3)
    common.start_stream(cursor, "test")

    for message in messages:
        common.kafka_check_vertex_exists_with_topic_and_payload(cursor, kafka_topics[0], message)


def test_show_streams(kafka_producer, kafka_topics, connection):
    assert len(kafka_topics) > 1
    cursor = connection.cursor()
    common.execute_and_fetch_all(
        cursor,
        f"CREATE KAFKA STREAM default_values TOPICS {kafka_topics[0]} TRANSFORM kafka_transform.simple BOOTSTRAP_SERVERS 'localhost:9092'",
    )

    consumer_group = "my_special_consumer_group"
    BATCH_INTERVAL = 42
    BATCH_SIZE = 3
    common.execute_and_fetch_all(
        cursor,
        f"CREATE KAFKA STREAM complex_values TOPICS {','.join(kafka_topics)} TRANSFORM kafka_transform.with_parameters CONSUMER_GROUP {consumer_group} BATCH_INTERVAL {BATCH_INTERVAL} BATCH_SIZE {BATCH_SIZE} ",
    )

    assert len(common.execute_and_fetch_all(cursor, "SHOW STREAMS")) == 2

    common.check_stream_info(
        cursor,
        "default_values",
        ("default_values", "kafka", 100, 1000, "kafka_transform.simple", None, False),
    )

    common.check_stream_info(
        cursor,
        "complex_values",
        (
            "complex_values",
            "kafka",
            BATCH_INTERVAL,
            BATCH_SIZE,
            "kafka_transform.with_parameters",
            None,
            False,
        ),
    )


@pytest.mark.parametrize("operation", ["START", "STOP"])
def test_start_and_stop_during_check(kafka_producer, kafka_topics, connection, operation):
    assert len(kafka_topics) > 1
    BATCH_SIZE = 1

    def stream_creator(stream_name):
        return f"CREATE KAFKA STREAM {stream_name} TOPICS {kafka_topics[0]} TRANSFORM kafka_transform.simple BATCH_SIZE {BATCH_SIZE}"

    def message_sender(msg):
        kafka_producer.send(kafka_topics[0], msg).get(timeout=60)

    common.test_start_and_stop_during_check(
        operation,
        connection,
        stream_creator,
        message_sender,
        "Kafka consumer test_stream is already stopped",
        BATCH_SIZE,
    )


def test_check_already_started_stream(kafka_topics, connection):
    assert len(kafka_topics) > 0
    cursor = connection.cursor()

    common.execute_and_fetch_all(
        cursor,
        f"CREATE KAFKA STREAM started_stream TOPICS {kafka_topics[0]} TRANSFORM kafka_transform.simple",
    )
    common.start_stream(cursor, "started_stream")

    with pytest.raises(mgclient.DatabaseError):
        common.execute_and_fetch_all(cursor, "CHECK STREAM started_stream")


def test_start_checked_stream_after_timeout(kafka_topics, connection):
    def stream_creator(stream_name):
        return f"CREATE KAFKA STREAM {stream_name} TOPICS {kafka_topics[0]} TRANSFORM kafka_transform.simple"

    common.test_start_checked_stream_after_timeout(connection, stream_creator)


def test_restart_after_error(kafka_producer, kafka_topics, connection):
    cursor = connection.cursor()
    common.execute_and_fetch_all(
        cursor,
        f"CREATE KAFKA STREAM test_stream TOPICS {kafka_topics[0]} TRANSFORM kafka_transform.query",
    )

    common.start_stream(cursor, "test_stream")
    time.sleep(1)

    kafka_producer.send(kafka_topics[0], common.SIMPLE_MSG).get(timeout=60)
    assert common.timed_wait(lambda: not common.get_is_running(cursor, "test_stream"))

    common.start_stream(cursor, "test_stream")
    time.sleep(1)
    kafka_producer.send(kafka_topics[0], b"CREATE (n:VERTEX { id : 42 })")
    assert common.check_one_result_row(cursor, "MATCH (n:VERTEX { id : 42 }) RETURN n")


@pytest.mark.parametrize("transformation", TRANSFORMATIONS_TO_CHECK_PY)
def test_bootstrap_server(kafka_producer, kafka_topics, connection, transformation):
    assert len(kafka_topics) > 0
    cursor = connection.cursor()
    LOCAL = "localhost:9092"
    common.execute_and_fetch_all(
        cursor,
        f"CREATE KAFKA STREAM test TOPICS {','.join(kafka_topics)} TRANSFORM {transformation} BOOTSTRAP_SERVERS '{LOCAL}'",
    )
    common.start_stream(cursor, "test")
    time.sleep(5)

    for topic in kafka_topics:
        kafka_producer.send(topic, common.SIMPLE_MSG).get(timeout=60)

    for topic in kafka_topics:
        common.kafka_check_vertex_exists_with_topic_and_payload(cursor, topic, common.SIMPLE_MSG)


@pytest.mark.parametrize("transformation", TRANSFORMATIONS_TO_CHECK_PY)
def test_bootstrap_server_empty(kafka_producer, kafka_topics, connection, transformation):
    assert len(kafka_topics) > 0
    cursor = connection.cursor()
    with pytest.raises(mgclient.DatabaseError):
        common.execute_and_fetch_all(
            cursor,
            f"CREATE KAFKA STREAM test TOPICS {','.join(kafka_topics)} TRANSFORM {transformation} BOOTSTRAP_SERVERS ''",
        )


@pytest.mark.parametrize("transformation", TRANSFORMATIONS_TO_CHECK_PY)
def test_set_offset(kafka_producer, kafka_topics, connection, transformation):
    assert len(kafka_topics) > 0
    cursor = connection.cursor()
    common.execute_and_fetch_all(
        cursor,
        f"CREATE KAFKA STREAM test TOPICS {kafka_topics[0]} TRANSFORM {transformation} BATCH_SIZE 1",
    )

    messages = [f"{i} message" for i in range(1, 21)]
    for message in messages:
        kafka_producer.send(kafka_topics[0], message.encode()).get(timeout=60)

    def consume(expected_msgs):
        common.start_stream(cursor, "test")
        if len(expected_msgs) == 0:
            time.sleep(2)
        else:
            assert common.check_one_result_row(
                cursor,
                (f"MATCH (n: MESSAGE {{payload: '{expected_msgs[-1]}'}})" "RETURN n"),
            )
        common.stop_stream(cursor, "test")
        res = common.execute_and_fetch_all(cursor, "MATCH (n) RETURN n.payload")
        return res

    def execute_set_offset_and_consume(id, expected_msgs):
        common.execute_and_fetch_all(cursor, f"CALL mg.kafka_set_stream_offset('test', {id})")
        return consume(expected_msgs)

    with pytest.raises(mgclient.DatabaseError):
        res = common.execute_and_fetch_all(cursor, "CALL mg.kafka_set_stream_offset('foo', 10)")

    def comparison_check(a, b):
        return a == str(b).strip("'(,)")

    res = execute_set_offset_and_consume(10, messages[10:])
    assert len(res) == 10
    assert all([comparison_check(a, b) for a, b in zip(messages[10:], res)])
    common.execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n")

    res = execute_set_offset_and_consume(-1, messages)
    assert len(res) == len(messages)
    assert all([comparison_check(a, b) for a, b in zip(messages, res)])
    res = common.execute_and_fetch_all(cursor, "MATCH (n) return n.offset")
    assert all([comparison_check(str(i), res[i]) for i in range(1, 20)])
    res = common.execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n")

    res = execute_set_offset_and_consume(-2, [])
    assert len(res) == 0
    last_msg = "Final Message"
    kafka_producer.send(kafka_topics[0], last_msg.encode()).get(timeout=60)
    res = consume([last_msg])
    assert len(res) == 1
    assert comparison_check("Final Message", res[0])
    common.execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n")


def test_info_procedure(kafka_topics, connection):
    cursor = connection.cursor()
    STREAM_NAME = "test_stream"
    CONFIGS = {"sasl.username": "michael.scott"}
    LOCAL = "localhost:9092"
    CREDENTIALS = {"sasl.password": "S3cr3tP4ssw0rd"}
    CONSUMER_GROUP = "ConsumerGr"
    common.execute_and_fetch_all(
        cursor,
        f"CREATE KAFKA STREAM {STREAM_NAME} TOPICS {','.join(kafka_topics)} TRANSFORM kafka_transform.simple CONSUMER_GROUP {CONSUMER_GROUP} BOOTSTRAP_SERVERS '{LOCAL}' CONFIGS {CONFIGS} CREDENTIALS {CREDENTIALS}",
    )

    stream_info = common.execute_and_fetch_all(cursor, f"CALL mg.kafka_stream_info('{STREAM_NAME}') YIELD *")

    reducted_credentials = {key: "<REDUCTED>" for key in CREDENTIALS.keys()}

    expected_stream_info = [(LOCAL, CONFIGS, CONSUMER_GROUP, reducted_credentials, kafka_topics)]
    common.validate_info(stream_info, expected_stream_info)


@pytest.mark.parametrize("transformation", TRANSFORMATIONS_TO_CHECK_C)
def test_load_c_transformations(connection, transformation):
    cursor = connection.cursor()

    query = f"CALL mg.transformations() YIELD * WITH name WHERE name STARTS WITH '{transformation}' RETURN name"
    result = common.execute_and_fetch_all(cursor, query)
    assert len(result) == 1
    assert result[0][0] == transformation


def test_check_stream_same_number_of_queries_than_messages(kafka_producer, kafka_topics, connection):
    assert len(kafka_topics) > 0

    TRANSFORMATION = "common_transform.check_stream_no_filtering"

    def stream_creator(stream_name, batch_size):
        return f"CREATE KAFKA STREAM {stream_name} TOPICS {kafka_topics[0]} TRANSFORM {TRANSFORMATION} BATCH_INTERVAL 3000 BATCH_SIZE {batch_size}"

    def message_sender(msg):
        kafka_producer.send(kafka_topics[0], msg).get(timeout=60)

    common.test_check_stream_same_number_of_queries_than_messages(connection, stream_creator, message_sender)


def test_check_stream_different_number_of_queries_than_messages(kafka_producer, kafka_topics, connection):
    assert len(kafka_topics) > 0

    TRANSFORMATION = "common_transform.check_stream_with_filtering"

    def stream_creator(stream_name, batch_size):
        return f"CREATE KAFKA STREAM {stream_name} TOPICS {kafka_topics[0]} TRANSFORM {TRANSFORMATION} BATCH_INTERVAL 3000  BATCH_SIZE {batch_size}"

    def message_sender(msg):
        kafka_producer.send(kafka_topics[0], msg).get(timeout=60)

    common.test_check_stream_different_number_of_queries_than_messages(connection, stream_creator, message_sender)


def test_start_stream_with_batch_limit(kafka_producer, kafka_topics, connection):
    assert len(kafka_topics) > 0

    def stream_creator(stream_name):
        return (
            f"CREATE KAFKA STREAM {stream_name} TOPICS {kafka_topics[0]} TRANSFORM kafka_transform.simple BATCH_SIZE 1"
        )

    def messages_sender(nof_messages):
        for x in range(nof_messages):
            kafka_producer.send(kafka_topics[0], common.SIMPLE_MSG).get(timeout=60)

    common.test_start_stream_with_batch_limit(connection, stream_creator, messages_sender)


def test_start_stream_with_batch_limit_timeout(kafka_producer, kafka_topics, connection):
    assert len(kafka_topics) > 0

    def stream_creator(stream_name):
        return (
            f"CREATE KAFKA STREAM {stream_name} TOPICS {kafka_topics[0]} TRANSFORM kafka_transform.simple BATCH_SIZE 1"
        )

    common.test_start_stream_with_batch_limit_timeout(connection, stream_creator)


def test_start_stream_with_batch_limit_reaching_timeout(kafka_producer, kafka_topics, connection):
    assert len(kafka_topics) > 0

    def stream_creator(stream_name, batch_size):
        return f"CREATE KAFKA STREAM {stream_name} TOPICS {kafka_topics[0]} TRANSFORM kafka_transform.simple BATCH_SIZE {batch_size}"

    common.test_start_stream_with_batch_limit_reaching_timeout(connection, stream_creator)


def test_start_stream_with_batch_limit_while_check_running(kafka_producer, kafka_topics, connection):
    assert len(kafka_topics) > 0

    def stream_creator(stream_name):
        return (
            f"CREATE KAFKA STREAM {stream_name} TOPICS {kafka_topics[0]} TRANSFORM kafka_transform.simple BATCH_SIZE 1"
        )

    def message_sender(message):
        kafka_producer.send(kafka_topics[0], message).get(timeout=6000)

    def setup_function(start_check_stream, cursor, stream_name, batch_limit, timeout):
        thread_stream_check = Process(target=start_check_stream, daemon=True, args=(stream_name, batch_limit, timeout))
        thread_stream_check.start()

        def is_running():
            return common.get_is_running(cursor, stream_name)

        assert mg_sleep_and_assert(True, is_running)
        message_sender(common.SIMPLE_MSG)
        thread_stream_check.join()

    common.test_start_stream_with_batch_limit_while_check_running(
        connection, stream_creator, message_sender, setup_function
    )


def test_check_while_stream_with_batch_limit_running(kafka_producer, kafka_topics, connection):
    assert len(kafka_topics) > 0

    def stream_creator(stream_name):
        return (
            f"CREATE KAFKA STREAM {stream_name} TOPICS {kafka_topics[0]} TRANSFORM kafka_transform.simple BATCH_SIZE 1"
        )

    def message_sender(message):
        kafka_producer.send(kafka_topics[0], message).get(timeout=6000)

    common.test_check_while_stream_with_batch_limit_running(connection, stream_creator, message_sender)


def test_start_stream_with_batch_limit_with_invalid_batch_limit(kafka_producer, kafka_topics, connection):
    assert len(kafka_topics) > 0

    def stream_creator(stream_name):
        return (
            f"CREATE KAFKA STREAM {stream_name} TOPICS {kafka_topics[0]} TRANSFORM kafka_transform.simple BATCH_SIZE 1"
        )

    common.test_start_stream_with_batch_limit_with_invalid_batch_limit(connection, stream_creator)


def test_check_stream_with_batch_limit_with_invalid_batch_limit(kafka_producer, kafka_topics, connection):
    assert len(kafka_topics) > 0

    def stream_creator(stream_name):
        return (
            f"CREATE KAFKA STREAM {stream_name} TOPICS {kafka_topics[0]} TRANSFORM kafka_transform.simple BATCH_SIZE 1"
        )

    common.test_check_stream_with_batch_limit_with_invalid_batch_limit(connection, stream_creator)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
