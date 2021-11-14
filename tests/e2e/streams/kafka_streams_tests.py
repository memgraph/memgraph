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

TRANSFORMATIONS_TO_CHECK = [
    "kafka_transform.simple",
    "kafka_transform.with_parameters"]


@pytest.mark.parametrize("transformation", TRANSFORMATIONS_TO_CHECK)
def test_simple(kafka_producer, kafka_topics, connection, transformation):
    assert len(kafka_topics) > 0
    cursor = connection.cursor()
    common.execute_and_fetch_all(
        cursor,
        "CREATE KAFKA STREAM test "
        f"TOPICS {','.join(kafka_topics)} "
        f"TRANSFORM {transformation}",
    )
    common.start_stream(cursor, "test")
    time.sleep(5)

    for topic in kafka_topics:
        kafka_producer.send(topic, common.SIMPLE_MSG).get(timeout=60)

    for topic in kafka_topics:
        common.kafka_check_vertex_exists_with_topic_and_payload(
            cursor, topic, common.SIMPLE_MSG)


@pytest.mark.parametrize("transformation", TRANSFORMATIONS_TO_CHECK)
def test_separate_consumers(
        kafka_producer,
        kafka_topics,
        connection,
        transformation):
    assert len(kafka_topics) > 0
    cursor = connection.cursor()

    stream_names = []
    for topic in kafka_topics:
        stream_name = "stream_" + topic
        stream_names.append(stream_name)
        common.execute_and_fetch_all(
            cursor,
            f"CREATE KAFKA STREAM {stream_name} "
            f"TOPICS {topic} "
            f"TRANSFORM {transformation}",
        )

    for stream_name in stream_names:
        common.start_stream(cursor, stream_name)

    time.sleep(5)

    for topic in kafka_topics:
        kafka_producer.send(topic, common.SIMPLE_MSG).get(timeout=60)

    for topic in kafka_topics:
        common.kafka_check_vertex_exists_with_topic_and_payload(
            cursor, topic, common.SIMPLE_MSG)


def test_start_from_last_committed_offset(
        kafka_producer, kafka_topics, connection):
    # This test creates a stream, consumes a message to have a committed
    # offset, then destroys the stream. A new message is sent before the
    # stream is recreated and then restarted. This simulates when Memgraph is
    # stopped (stream is destroyed) and then restarted (stream is recreated).
    # This is of course not as good as restarting memgraph would be, but
    # restarting Memgraph during a single workload cannot be done currently.
    assert len(kafka_topics) > 0
    cursor = connection.cursor()
    common.execute_and_fetch_all(
        cursor, "CREATE KAFKA STREAM test "
        f"TOPICS {kafka_topics[0]} "
        "TRANSFORM kafka_transform.simple", )
    common.start_stream(cursor, "test")
    time.sleep(1)

    kafka_producer.send(kafka_topics[0], common.SIMPLE_MSG).get(timeout=60)

    common.kafka_check_vertex_exists_with_topic_and_payload(
        cursor, kafka_topics[0], common.SIMPLE_MSG)

    common.stop_stream(cursor, "test")
    common.drop_stream(cursor, "test")

    messages = [b"second message", b"third message"]
    for message in messages:
        kafka_producer.send(kafka_topics[0], message).get(timeout=60)

    for message in messages:
        vertices_with_msg = common.execute_and_fetch_all(
            cursor,
            "MATCH (n: MESSAGE {" f"payload: '{message.decode('utf-8')}'" "}) RETURN n",
        )

        assert len(vertices_with_msg) == 0

    common.execute_and_fetch_all(
        cursor, "CREATE KAFKA STREAM test "
        f"TOPICS {kafka_topics[0]} "
        "TRANSFORM kafka_transform.simple", )
    common.start_stream(cursor, "test")

    for message in messages:
        common.kafka_check_vertex_exists_with_topic_and_payload(
            cursor, kafka_topics[0], message)


@pytest.mark.parametrize("transformation", TRANSFORMATIONS_TO_CHECK)
def test_check_stream(
        kafka_producer,
        kafka_topics,
        connection,
        transformation):
    assert len(kafka_topics) > 0
    cursor = connection.cursor()
    common.execute_and_fetch_all(
        cursor,
        "CREATE KAFKA STREAM test "
        f"TOPICS {kafka_topics[0]} "
        f"TRANSFORM {transformation} "
        "BATCH_SIZE 1",
    )
    common.start_stream(cursor, "test")
    time.sleep(1)

    kafka_producer.send(kafka_topics[0], common.SIMPLE_MSG).get(timeout=60)
    common.stop_stream(cursor, "test")

    messages = [b"first message", b"second message", b"third message"]
    for message in messages:
        kafka_producer.send(kafka_topics[0], message).get(timeout=60)

    def check_check_stream(batch_limit):
        assert (
            transformation == "kafka_transform.simple"
            or transformation == "kafka_transform.with_parameters"
        )
        test_results = common.execute_and_fetch_all(
            cursor, f"CHECK STREAM test BATCH_LIMIT {batch_limit}"
        )
        assert len(test_results) == batch_limit

        for i in range(batch_limit):
            message_as_str = messages[i].decode("utf-8")
            if transformation == "kafka_transform.simple":
                assert f"payload: '{message_as_str}'" in test_results[i][common.QUERY]
                assert test_results[i][common.PARAMS] is None
            else:
                assert test_results[i][common.QUERY] == (
                    "CREATE (n:MESSAGE "
                    "{timestamp: $timestamp, "
                    "payload: $payload, "
                    "topic: $topic})"
                )
                parameters = test_results[i][common.PARAMS]
                # this is not a very sofisticated test, but checks if
                # timestamp has some kind of value
                assert parameters["timestamp"] > 1000000000000
                assert parameters["topic"] == kafka_topics[0]
                assert parameters["payload"] == message_as_str

    check_check_stream(1)
    check_check_stream(2)
    check_check_stream(3)
    common.start_stream(cursor, "test")

    for message in messages:
        common.kafka_check_vertex_exists_with_topic_and_payload(
            cursor, kafka_topics[0], message)


def test_show_streams(kafka_producer, kafka_topics, connection):
    assert len(kafka_topics) > 1
    cursor = connection.cursor()
    common.execute_and_fetch_all(
        cursor,
        "CREATE KAFKA STREAM default_values "
        f"TOPICS {kafka_topics[0]} "
        f"TRANSFORM kafka_transform.simple "
        f"BOOTSTRAP_SERVERS 'localhost:9092'",
    )

    consumer_group = "my_special_consumer_group"
    batch_interval = 42
    batch_size = 3
    common.execute_and_fetch_all(
        cursor,
        "CREATE KAFKA STREAM complex_values "
        f"TOPICS {','.join(kafka_topics)} "
        f"TRANSFORM kafka_transform.with_parameters "
        f"CONSUMER_GROUP {consumer_group} "
        f"BATCH_INTERVAL {batch_interval} "
        f"BATCH_SIZE {batch_size} ",
    )

    assert len(common.execute_and_fetch_all(cursor, "SHOW STREAMS")) == 2

    common.check_stream_info(
        cursor,
        "default_values",
        ("default_values", None, None, "kafka_transform.simple", None, False),
    )

    common.check_stream_info(
        cursor,
        "complex_values",
        (
            "complex_values",
            batch_interval,
            batch_size,
            "kafka_transform.with_parameters",
            None,
            False,
        ),
    )


@pytest.mark.parametrize("operation", ["START", "STOP"])
def test_start_and_stop_during_check(
        kafka_producer,
        kafka_topics,
        connection,
        operation):
    assert len(kafka_topics) > 1

    def stream_creator(stream_name):
        return f"CREATE KAFKA STREAM {stream_name} TOPICS {kafka_topics[0]} TRANSFORM kafka_transform.simple"

    def message_sender(msg):
        kafka_producer.send(kafka_topics[0], msg).get(timeout=60)

    common.test_start_and_stop_during_check(
        operation,
        connection,
        stream_creator,
        message_sender,
        "Kafka consumer test_stream is already stopped")


def test_check_already_started_stream(kafka_topics, connection):
    assert len(kafka_topics) > 0
    cursor = connection.cursor()

    common.execute_and_fetch_all(
        cursor,
        "CREATE KAFKA STREAM started_stream "
        f"TOPICS {kafka_topics[0]} "
        f"TRANSFORM kafka_transform.simple",
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
        "CREATE KAFKA STREAM test_stream "
        f"TOPICS {kafka_topics[0]} "
        f"TRANSFORM kafka_transform.query",
    )

    common.start_stream(cursor, "test_stream")
    time.sleep(1)

    kafka_producer.send(kafka_topics[0], common.SIMPLE_MSG).get(timeout=60)
    assert common.timed_wait(
        lambda: not common.get_is_running(
            cursor, "test_stream"))

    common.start_stream(cursor, "test_stream")
    time.sleep(1)
    kafka_producer.send(kafka_topics[0], b"CREATE (n:VERTEX { id : 42 })")
    assert common.check_one_result_row(
        cursor, "MATCH (n:VERTEX { id : 42 }) RETURN n")


@pytest.mark.parametrize("transformation", TRANSFORMATIONS_TO_CHECK)
def test_bootstrap_server(
        kafka_producer,
        kafka_topics,
        connection,
        transformation):
    assert len(kafka_topics) > 0
    cursor = connection.cursor()
    local = "localhost:9092"
    common.execute_and_fetch_all(
        cursor,
        "CREATE KAFKA STREAM test "
        f"TOPICS {','.join(kafka_topics)} "
        f"TRANSFORM {transformation} "
        f"BOOTSTRAP_SERVERS '{local}'",
    )
    common.start_stream(cursor, "test")
    time.sleep(5)

    for topic in kafka_topics:
        kafka_producer.send(topic, common.SIMPLE_MSG).get(timeout=60)

    for topic in kafka_topics:
        common.kafka_check_vertex_exists_with_topic_and_payload(
            cursor, topic, common.SIMPLE_MSG)


@pytest.mark.parametrize("transformation", TRANSFORMATIONS_TO_CHECK)
def test_bootstrap_server_empty(
        kafka_producer,
        kafka_topics,
        connection,
        transformation):
    assert len(kafka_topics) > 0
    cursor = connection.cursor()
    with pytest.raises(mgclient.DatabaseError):
        common.execute_and_fetch_all(
            cursor,
            "CREATE KAFKA STREAM test "
            f"TOPICS {','.join(kafka_topics)} "
            f"TRANSFORM {transformation} "
            "BOOTSTRAP_SERVERS ''",
        )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
