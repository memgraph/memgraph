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

# import os
import pulsar
import pytest
from common import NAME, PULSAR_SERVICE_URL, connect, execute_and_fetch_all
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

import requests

# To run these test locally a running Kafka sever is necessery. The test tries
# to connect on localhost:9092.

# KAFKA_HOSTNAME=os.getenv("KAFKA_HOSTNAME", "localhost")
# PULSAR_HOSTNAME=os.getenv("PULSAR_HOSTNAME", "localhost")
# PULSAR_PORT="6652" if PULSAR_HOSTNAME == "localhost" else "8080"

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
    return [f"topic_{i}" for i in range(num)]


@pytest.fixture(scope="function")
def kafka_topics():
    admin_client = KafkaAdminClient(bootstrap_servers="localhost:29092", client_id="test")
    # The issue arises if we remove default kafka topics, e.g.
    # "__consumer_offsets"
    previous_topics = [topic for topic in admin_client.list_topics() if topic != "__consumer_offsets"]
    if previous_topics:
        admin_client.delete_topics(topics=previous_topics, timeout_ms=5000)

    topics = get_topics(3)
    topics_to_create = []
    for topic in topics:
        topics_to_create.append(NewTopic(name=topic, num_partitions=1, replication_factor=1))

    admin_client.create_topics(new_topics=topics_to_create, timeout_ms=5000)
    yield topics
    admin_client.delete_topics(topics=topics, timeout_ms=5000)


@pytest.fixture(scope="function")
def kafka_producer():
    yield KafkaProducer(bootstrap_servers=["localhost:29092"], api_version_auto_timeout_ms=10000)


@pytest.fixture(scope="function")
def pulsar_client():
    yield pulsar.Client(PULSAR_SERVICE_URL)


@pytest.fixture(scope="function")
def pulsar_topics():
    topics = get_topics(3)
    for topic in topics:
        requests.delete(f"http://localhost:6652/admin/v2/persistent/public/default/{topic}?force=true")
    yield topics
