#!/usr/bin/python3

# To run these test locally a running Kafka sever is necessery. The test tries
# to connect on localhost:9092.

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
SIMPLE_MSG_STR = SIMPLE_MSG.decode('utf-8')


@pytest.fixture(autouse=True)
def cleanup():
    yield
    connection = mgclient.connect(host="localhost", port=7687)
    connection.autocommit = True
    cursor = connection.cursor()
    cursor.execute("MATCH (n) DETACH DELETE n")
    cursor.fetchall()
    cursor.execute("SHOW STREAMS")
    streams = cursor.fetchall()
    for stream_info in streams:
        cursor.execute(f"DROP STREAM {stream_info[0]}")
        cursor.fetchall()


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


def check_vertex_exists_with_topic_and_payload(cursor, topic, payload):
    assert check_one_result_row(cursor,
                                f"MATCH (n: MESSAGE {{ payload: '{payload}', topic: '{topic}'}}) RETURN n")


def start_stream(cursor, stream_name):
    cursor.execute("START STREAM trial")
    cursor.fetchall()
    time.sleep(1)
    cursor.execute("SHOW STREAMS")
    stream_infos = cursor.fetchall()

    found = False
    for stream_info in stream_infos:
        if (stream_info[NAME] == stream_name):
            found = True
            assert stream_info[IS_RUNNING]

    assert found


def test_simple(producer, topics):
    assert len(topics) > 0

    connection = mgclient.connect(host="localhost", port=7687)
    connection.autocommit = True
    cursor = connection.cursor()
    cursor.execute(
        f"CREATE STREAM trial TOPICS {','.join(topics)} TRANSFORM transform.transformation")
    cursor.fetchall()
    start_stream(cursor, "trial")

    for topic in topics:
        producer.send(topic, SIMPLE_MSG)
    producer.flush()

    for topic in topics:
        check_vertex_exists_with_topic_and_payload(
            cursor, topic, SIMPLE_MSG_STR)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__]))
