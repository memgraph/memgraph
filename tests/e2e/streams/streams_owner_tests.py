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
import time
import mgclient
import common

def get_cursor_with_user(username):
    connection = common.connect(username=username, password="")
    return connection.cursor()


def create_admin_user(cursor, admin_user):
    common.execute_and_fetch_all(cursor, f"CREATE USER {admin_user}")
    common.execute_and_fetch_all(
        cursor, f"GRANT ALL PRIVILEGES TO {admin_user}")


def create_stream_user(cursor, stream_user):
    common.execute_and_fetch_all(cursor, f"CREATE USER {stream_user}")
    common.execute_and_fetch_all(
        cursor, f"GRANT STREAM TO {stream_user}")


def test_ownerless_stream(kafka_producer, kafka_topics, connection):
    assert len(kafka_topics) > 0
    userless_cursor = connection.cursor()
    common.execute_and_fetch_all(userless_cursor,
                                 "CREATE KAFKA STREAM ownerless "
                                 f"TOPICS {kafka_topics[0]} "
                                 f"TRANSFORM kafka_transform.simple")
    common.start_stream(userless_cursor, "ownerless")
    time.sleep(1)

    admin_user = "admin_user"
    create_admin_user(userless_cursor, admin_user)

    kafka_producer.send(kafka_topics[0], b"first message").get(timeout=60)
    assert common.timed_wait(
        lambda: not common.get_is_running(userless_cursor, "ownerless"))

    assert len(common.execute_and_fetch_all(
        userless_cursor, "MATCH (n) RETURN n")) == 0

    common.execute_and_fetch_all(userless_cursor, f"DROP USER {admin_user}")
    common.start_stream(userless_cursor, "ownerless")
    time.sleep(1)

    second_message = b"second message"
    kafka_producer.send(kafka_topics[0], second_message).get(timeout=60)
    common.kafka_check_vertex_exists_with_topic_and_payload(
        userless_cursor, kafka_topics[0], second_message)

    assert len(common.execute_and_fetch_all(
        userless_cursor, "MATCH (n) RETURN n")) == 1


def test_owner_is_shown(kafka_topics, connection):
    assert len(kafka_topics) > 0
    userless_cursor = connection.cursor()

    stream_user = "stream_user"
    create_stream_user(userless_cursor, stream_user)
    stream_cursor = get_cursor_with_user(stream_user)

    common.execute_and_fetch_all(stream_cursor, "CREATE KAFKA STREAM test "
                                 f"TOPICS {kafka_topics[0]} "
                                 f"TRANSFORM kafka_transform.simple")

    common.check_stream_info(userless_cursor, "test", ("test", None, None,
        "kafka_transform.simple", stream_user, False))


def test_insufficient_privileges(kafka_producer, kafka_topics, connection):
    assert len(kafka_topics) > 0
    userless_cursor = connection.cursor()

    admin_user = "admin_user"
    create_admin_user(userless_cursor, admin_user)
    admin_cursor = get_cursor_with_user(admin_user)

    stream_user = "stream_user"
    create_stream_user(userless_cursor, stream_user)
    stream_cursor = get_cursor_with_user(stream_user)

    common.execute_and_fetch_all(stream_cursor,
                                 "CREATE KAFKA STREAM insufficient_test "
                                 f"TOPICS {kafka_topics[0]} "
                                 f"TRANSFORM kafka_transform.simple")

    # the stream is started by admin, but should check against the owner
    # privileges
    common.start_stream(admin_cursor, "insufficient_test")
    time.sleep(1)

    kafka_producer.send(kafka_topics[0], b"first message").get(timeout=60)
    assert common.timed_wait(
        lambda: not common.get_is_running(userless_cursor, "insufficient_test"))

    assert len(common.execute_and_fetch_all(
        userless_cursor, "MATCH (n) RETURN n")) == 0

    common.execute_and_fetch_all(
        admin_cursor, f"GRANT CREATE TO {stream_user}")
    common.start_stream(userless_cursor, "insufficient_test")
    time.sleep(1)

    second_message = b"second message"
    kafka_producer.send(kafka_topics[0], second_message).get(timeout=60)
    common.kafka_check_vertex_exists_with_topic_and_payload(
        userless_cursor, kafka_topics[0], second_message)

    assert len(common.execute_and_fetch_all(
        userless_cursor, "MATCH (n) RETURN n")) == 1


def test_happy_case(kafka_producer, kafka_topics, connection):
    assert len(kafka_topics) > 0
    userless_cursor = connection.cursor()

    admin_user = "admin_user"
    create_admin_user(userless_cursor, admin_user)
    admin_cursor = get_cursor_with_user(admin_user)

    stream_user = "stream_user"
    create_stream_user(userless_cursor, stream_user)
    stream_cursor = get_cursor_with_user(stream_user)
    common.execute_and_fetch_all(
        admin_cursor, f"GRANT CREATE TO {stream_user}")

    common.execute_and_fetch_all(stream_cursor,
                                 "CREATE KAFKA STREAM insufficient_test "
                                 f"TOPICS {kafka_topics[0]} "
                                 f"TRANSFORM kafka_transform.simple")

    common.start_stream(stream_cursor, "insufficient_test")
    time.sleep(1)

    first_message = b"first message"
    kafka_producer.send(kafka_topics[0], first_message).get(timeout=60)

    common.kafka_check_vertex_exists_with_topic_and_payload(
        userless_cursor, kafka_topics[0], first_message)

    assert len(common.execute_and_fetch_all(
        userless_cursor, "MATCH (n) RETURN n")) == 1


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
