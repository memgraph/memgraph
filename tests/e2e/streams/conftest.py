import pytest
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

from common import execute_and_fetch_all, connect, NAME

# To run these test locally a running Kafka sever is necessery. The test tries
# to connect on localhost:9092.


@pytest.fixture(autouse=True)
def connection():
    connection = connect()
    yield connection
    cursor = connection.cursor()
    execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n")
    stream_infos = execute_and_fetch_all(cursor, "SHOW STREAMS")
    for stream_info in stream_infos:
        execute_and_fetch_all(cursor, f"DROP STREAM {stream_info[NAME]}")
    users = execute_and_fetch_all(cursor, "SHOW USERS")
    for username, in users:
        execute_and_fetch_all(cursor, f"DROP USER {username}")


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
