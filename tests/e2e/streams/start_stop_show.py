#!/usr/bin/python3

import sys
import pytest
import mgclient
import time
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic


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
    for index in range(10):
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

        assert len(results) == 1
        return 1


def check_vertex_exists_with_topic(cursor, topic):
    assert check_one_result_row(cursor,
                                f"MATCH (n: MESSAGE {{ topic: '{topic}'}}) RETURN n")


def test_trial(producer, topics):
    assert len(topics) > 0
    connection = mgclient.connect(host="localhost", port=7687)
    connection.autocommit = True
    cursor = connection.cursor()
    cursor.execute(
        f"CREATE STREAM trial TOPICS {','.join(topics)} TRANSFORM transform.transformation")
    cursor.fetchall()
    cursor.execute("START STREAM trial")
    cursor.fetchall()

    for topic in topics:
        producer.send(topic, b'message')

    for topic in topics[1:]:
        check_vertex_exists_with_topic(cursor, topic)


# def test_simple():
#     assert True


if __name__ == "__main__":
    sys.exit(pytest.main([__file__]))
