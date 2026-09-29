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
import time

import pulsar
import pytest
from common import KAFKA_BOOTSTRAP_SERVERS, NAME, PULSAR_ADMIN_URL, PULSAR_SERVICE_URL, connect, execute_and_fetch_all
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError

import requests

# Running these tests needs the Kafka and Pulsar compose stacks under this
# directory; see common.py for how the broker hosts are resolved.


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


def unique_topics(request, num):
    """Topic names derived from the test name so tests (and leftovers from aborted runs) never share topics."""
    safe = re.sub(r"[^\w]", "_", request.node.name)  # e.g. "test_simple[kafka_transform.simple]" -> underscores only
    return [f"{safe}_topic_{i}" for i in range(num)]


@pytest.fixture(scope="function")
def kafka_topics(request):
    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, client_id="test")

    # build 3 new topics, one request each so one leftover topic doesn't fail the whole batch
    topics = unique_topics(request, 3)
    # Everything after this point runs inside the `try` so the client is closed even when setup fails
    try:
        deadline = time.time() + 30
        for topic in topics:
            while True:
                try:
                    admin.create_topics(
                        new_topics=[NewTopic(name=topic, num_partitions=1, replication_factor=1)], timeout_ms=5000
                    )
                    break
                except TopicAlreadyExistsError:
                    # Left behind by an aborted run, or still pending deletion: (re)issue the delete and wait for it
                    if time.time() > deadline:
                        pytest.fail(f"Could not create topic (still marked for deletion): {topic}")
                    try:
                        admin.delete_topics([topic], timeout_ms=5000)
                    except KafkaError:
                        pass
                    time.sleep(1)

        # The broker applies new topics to its metadata asynchronously (noticeably late under CI load), and
        # CREATE KAFKA STREAM rejects topics missing from the metadata, so wait until they are visible.
        deadline = time.time() + 30
        while not set(topics) <= set(admin.list_topics()):
            if time.time() > deadline:
                pytest.fail(f"Topics not visible in broker metadata: {topics}")
            time.sleep(0.2)

        yield topics
    finally:
        # A failed delete (or a topic never created because setup failed) must not turn into a teardown error
        try:
            admin.delete_topics(topics, timeout_ms=5000)
        except KafkaError:
            pass
        admin.close()


@pytest.fixture(scope="function")
def kafka_producer():
    yield KafkaProducer(bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS], bootstrap_timeout_ms=10000)


@pytest.fixture(scope="function")
def pulsar_client():
    yield pulsar.Client(PULSAR_SERVICE_URL)


def delete_pulsar_topic(topic):
    # Pulsar answers 204 even when the topic doesn't exist, so a failure here means the admin endpoint is broken
    requests.delete(
        f"{PULSAR_ADMIN_URL}/admin/v2/persistent/public/default/{topic}?force=true", timeout=10
    ).raise_for_status()


@pytest.fixture(scope="function")
def pulsar_topics(request):
    topics = unique_topics(request, 3)
    for topic in topics:
        delete_pulsar_topic(topic)
    try:
        yield topics
    finally:
        # As with Kafka: a failed delete must not turn the test result into a teardown error
        for topic in topics:
            try:
                delete_pulsar_topic(topic)
            except requests.RequestException:
                pass
