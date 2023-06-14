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

import pytest
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

import pulsar
import requests

from common import NAME, connect, execute_and_fetch_all, PULSAR_SERVICE_URL

# To run these test locally a running Kafka sever is necessary. The test tries
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
    for (username,) in users:
        execute_and_fetch_all(cursor, f"DROP USER {username}")


def get_topics(num):
    return [f'topic_{i}' for i in range(num)]


@pytest.fixture(scope="function")
def kafka_topics():
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:9092",
        client_id="test")
    # The issue arises if we remove default kafka topics, e.g.
    # "__consumer_offsets"
    previous_topics = [
        topic for topic in admin_client.list_topics() if topic != "__consumer_offsets"]
    if previous_topics:
        admin_client.delete_topics(topics=previous_topics, timeout_ms=5000)

    topics = get_topics(3)
    topics_to_create = []
    for topic in topics:
        topics_to_create.append(
            NewTopic(
                name=topic,
                num_partitions=1,
                replication_factor=1))

    admin_client.create_topics(new_topics=topics_to_create, timeout_ms=5000)
    yield topics
    admin_client.delete_topics(topics=topics, timeout_ms=5000)


@pytest.fixture(scope="function")
def kafka_producer():
    yield KafkaProducer(bootstrap_servers="localhost:9092")


@pytest.fixture(scope="function")
def pulsar_client():
    yield pulsar.Client(PULSAR_SERVICE_URL)


@pytest.fixture(scope="function")
def pulsar_topics():
    topics = get_topics(3)
    for topic in topics:
        requests.delete(
            f'http://127.0.0.1:6652/admin/v2/persistent/public/default/{topic}?force=true')
    yield topics
