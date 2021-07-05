#!/usr/bin/python3

# To run these test locally a running Kafka sever is necessery. The test tries
# to connect on localhost:9092.

# All tests are implemented in this file, because using the same test fixtures
# in multiple files is not possible in a straightforward way

import sys
import pytest
import mgclient
import time
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

NAME = 0
TOPICS = 1
CONSUMER_GROUP = 2
BATCH_INTERVAL = 3
BATCH_SIZE = 4
TRANSFORM = 5
IS_RUNNING = 6

SIMPLE_MSG = b'message'


def execute_and_fetch_all(cursor, query):
    cursor.execute(query)
    return cursor.fetchall()


@pytest.fixture(autouse=True)
def connection():
    connection = mgclient.connect(host="localhost", port=7687)
    connection.autocommit = True
    yield connection
    cursor = connection.cursor()
    execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n")
    stream_infos = execute_and_fetch_all(cursor, "SHOW STREAMS")
    for stream_info in stream_infos:
        execute_and_fetch_all(cursor, f"DROP STREAM {stream_info[NAME]}")


@pytest.fixture(scope="function")
def topics():
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:9092", client_id='test')

    topics = []
    topics_to_create = []
    for index in range(3):
        topic = f"topic_{index}"
        topics.append(topic)
        topics_to_create.append(NewTopic(name=topic,
                                num_partitions=1, replication_factor=1))

    admin_client.create_topics(new_topics=topics_to_create, timeout_ms=5000)
    yield topics
    admin_client.delete_topics(topics=topics, timeout_ms=5000)


@pytest.fixture(scope="function")
def producer():
    yield KafkaProducer(bootstrap_servers="localhost:9092")


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
    execute_and_fetch_all(cursor, f"DROP_STREAM {stream_name}")

    assert get_stream_info(cursor, stream_name) is None


def test_simple(producer, topics, connection):
    assert len(topics) > 0
    cursor = connection.cursor()
    execute_and_fetch_all(cursor,
                          "CREATE STREAM test "
                          f"TOPICS {','.join(topics)} "
                          "TRANSFORM transform.transformation")
    start_stream(cursor, "test")
    time.sleep(1)

    for topic in topics:
        producer.send(topic, SIMPLE_MSG).get(timeout=60)

    for topic in topics:
        check_vertex_exists_with_topic_and_payload(
            cursor, topic, SIMPLE_MSG)


def test_separate_consumers(producer, topics, connection):
    assert len(topics) > 0
    cursor = connection.cursor()

    stream_names = []
    for topic in topics:
        stream_name = "stream_" + topic
        stream_names.append(stream_name)
        execute_and_fetch_all(cursor,
                              f"CREATE STREAM {stream_name} "
                              f"TOPICS {topic} "
                              "TRANSFORM transform.transformation")

    for stream_name in stream_names:
        start_stream(cursor, stream_name)

    time.sleep(5)

    for topic in topics:
        producer.send(topic, SIMPLE_MSG).get(timeout=60)

    for topic in topics:
        check_vertex_exists_with_topic_and_payload(
            cursor, topic, SIMPLE_MSG)


def test_start_from_last_committed_offset(producer, topics, connection):
    # This test creates a stream, consumes a message to have a committed
    # offset, then destroys the stream. A new message is sent before the
    # stream is recreated and then restarted. This simulates when Memgraph is
    # stopped (stream is destroyed) and then restarted (stream is recreated).
    # This is of course not as good as restarting memgraph would be, but
    # restarting Memgraph during a single workload is cannot be done currently.
    assert len(topics) > 0
    cursor = connection.cursor()
    execute_and_fetch_all(cursor,
                          "CREATE STREAM test "
                          f"TOPICS {topics[0]} "
                          "TRANSFORM transform.transformation")
    start_stream(cursor, "test")
    time.sleep(1)

    producer.send(topics[0], SIMPLE_MSG).get(timeout=60)

    check_vertex_exists_with_topic_and_payload(
        cursor, topics[0], SIMPLE_MSG)

    stop_stream(cursor, "test")
    drop_stream(cursor, "test")

    messages = [b"second message", b"third message"]
    for message in messages:
        producer.send(topics[0], message).get(timeout=60)

    for message in messages:
        vertices_with_msg = execute_and_fetch_all(cursor,
                                                  "MATCH (n: MESSAGE {"
                                                  f"payload: '{message.decode('utf-8')}'"
                                                  "}) RETURN n")

        assert len(vertices_with_msg) == 0

    execute_and_fetch_all(cursor,
                          "CREATE STREAM test "
                          f"TOPICS {topics[0]} "
                          "TRANSFORM transform.transformation")
    start_stream(cursor, "test")

    for message in messages:
        check_vertex_exists_with_topic_and_payload(
            cursor, topics[0], message)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__]))
