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

import re

# import os
import time

import pulsar
import pytest
from common import NAME, PULSAR_SERVICE_URL, connect, execute_and_fetch_all
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

import requests

# To run these test locally a running Kafka sever is necessery. The test tries
# to connect on localhost:9092.

# KAFKA_HOSTNAME=os.getenv("KAFKA_HOSTNAME", "localhost")
# PULSAR_HOSTNAME=os.getenv("PULSAR_HOSTNAME", "localhost")
# PULSAR_PORT="6652" if PULSAR_HOSTNAME == "localhost" else "8080"


@pytest.fixture()
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
def kafka_topics(request):
    admin = KafkaAdminClient(bootstrap_servers="localhost:29092", client_id="test")

    # generate a safe, unique prefix from the test name
    raw = request.node.name  # e.g. "test_separate_consumers[kafka_transform.with_parameters]"
    safe = re.sub(r"[^\w]", "_", raw)  # now underscores only

    # build 3 new topics
    topics = [f"{safe}_topic_{i}" for i in range(3)]
    new_topics = [NewTopic(name=t, num_partitions=1, replication_factor=1) for t in topics]

    # create with retry in case of lingering deletions
    deadline = time.time() + 30
    while True:
        try:
            admin.create_topics(new_topics=new_topics, timeout_ms=5000)
            break
        except TopicAlreadyExistsError:
            if time.time() > deadline:
                pytest.fail(f"Could not create topics (still marked for deletion): {topics}")
            time.sleep(1)

    yield topics

    # teardown
    admin.delete_topics(topics, timeout_ms=5000)


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
