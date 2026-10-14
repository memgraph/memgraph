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

import contextlib
import sys
import time

import common
import mgclient
import pytest


def get_cursor_with_user(username):
    connection = common.connect(username=username, password="")
    return connection.cursor()


def create_admin_user(request, admin_user):
    """Creates the first user (anonymous session) and returns a cursor of that user.

    The first user turns authentication on and gets all privileges, and the anonymous session is denied from then on.
    The finalizer drops every user the test left, the first one last as dropping it ends its own session.
    """
    common.execute_and_fetch_all(common.connect().cursor(), f"CREATE USER {admin_user}")

    def drop_users():
        try:
            cursor = get_cursor_with_user(admin_user)
        except mgclient.Error:  # dropped by the test, authentication is off again
            return
        for (username,) in common.execute_and_fetch_all(cursor, "SHOW USERS"):
            if username != admin_user:
                common.execute_and_fetch_all(cursor, f"DROP USER {username}")
        with contextlib.suppress(mgclient.Error):
            common.execute_and_fetch_all(cursor, f"DROP USER {admin_user}")

    request.addfinalizer(drop_users)
    admin_cursor = get_cursor_with_user(admin_user)
    common.execute_and_fetch_all(admin_cursor, f"GRANT ALL PRIVILEGES TO {admin_user}")
    return admin_cursor


def create_stream_user(cursor, stream_user):
    common.execute_and_fetch_all(cursor, f"CREATE USER {stream_user}")
    common.execute_and_fetch_all(cursor, f"GRANT STREAM TO {stream_user}")


def test_ownerless_stream(request, kafka_producer, kafka_topics, connection):
    assert len(kafka_topics) > 0
    userless_cursor = connection.cursor()
    common.execute_and_fetch_all(
        userless_cursor,
        "CREATE KAFKA STREAM ownerless " f"TOPICS {kafka_topics[0]} " f"TRANSFORM kafka_transform.simple",
    )
    common.start_stream(userless_cursor, "ownerless")
    time.sleep(1)

    admin_user = "admin_user"
    admin_cursor = create_admin_user(request, admin_user)

    kafka_producer.send(kafka_topics[0], b"first message").get(timeout=60)
    assert common.timed_wait(lambda: not common.get_is_running(admin_cursor, "ownerless"))

    assert len(common.execute_and_fetch_all(admin_cursor, "MATCH (n) RETURN n")) == 0

    # Dropping the session's own user ends the session; with no users left authentication is off again.
    with contextlib.suppress(mgclient.Error):
        common.execute_and_fetch_all(admin_cursor, f"DROP USER {admin_user}")
    userless_cursor = common.connect().cursor()
    common.start_stream(userless_cursor, "ownerless")
    time.sleep(1)

    second_message = b"second message"
    kafka_producer.send(kafka_topics[0], second_message).get(timeout=60)
    common.kafka_check_vertex_exists_with_topic_and_payload(userless_cursor, kafka_topics[0], second_message)

    assert len(common.execute_and_fetch_all(userless_cursor, "MATCH (n) RETURN n")) == 1


def test_owner_is_shown(request, kafka_topics, connection):
    assert len(kafka_topics) > 0
    admin_cursor = create_admin_user(request, "admin_user")

    stream_user = "stream_user"
    create_stream_user(admin_cursor, stream_user)
    stream_cursor = get_cursor_with_user(stream_user)

    common.execute_and_fetch_all(
        stream_cursor, "CREATE KAFKA STREAM test " f"TOPICS {kafka_topics[0]} " f"TRANSFORM kafka_transform.simple"
    )

    common.check_stream_info(
        admin_cursor, "test", ("test", "kafka", 100, 1000, "kafka_transform.simple", stream_user, False)
    )


def test_insufficient_privileges(request, kafka_producer, kafka_topics, connection):
    assert len(kafka_topics) > 0
    admin_cursor = create_admin_user(request, "admin_user")

    stream_user = "stream_user"
    create_stream_user(admin_cursor, stream_user)
    stream_cursor = get_cursor_with_user(stream_user)

    common.execute_and_fetch_all(
        stream_cursor,
        "CREATE KAFKA STREAM insufficient_test " f"TOPICS {kafka_topics[0]} " f"TRANSFORM kafka_transform.simple",
    )

    # the stream is started by admin, but should check against the owner
    # privileges
    common.start_stream(admin_cursor, "insufficient_test")
    time.sleep(1)

    kafka_producer.send(kafka_topics[0], b"first message").get(timeout=60)
    assert common.timed_wait(lambda: not common.get_is_running(admin_cursor, "insufficient_test"))

    assert len(common.execute_and_fetch_all(admin_cursor, "MATCH (n) RETURN n")) == 0

    common.execute_and_fetch_all(admin_cursor, f"GRANT CREATE TO {stream_user}")
    common.start_stream(admin_cursor, "insufficient_test")
    time.sleep(1)

    second_message = b"second message"
    kafka_producer.send(kafka_topics[0], second_message).get(timeout=60)
    common.kafka_check_vertex_exists_with_topic_and_payload(admin_cursor, kafka_topics[0], second_message)

    assert len(common.execute_and_fetch_all(admin_cursor, "MATCH (n) RETURN n")) == 1


def test_happy_case(request, kafka_producer, kafka_topics, connection):
    assert len(kafka_topics) > 0
    admin_cursor = create_admin_user(request, "admin_user")

    stream_user = "stream_user"
    create_stream_user(admin_cursor, stream_user)
    stream_cursor = get_cursor_with_user(stream_user)
    common.execute_and_fetch_all(admin_cursor, f"GRANT CREATE TO {stream_user}")

    common.execute_and_fetch_all(
        stream_cursor,
        "CREATE KAFKA STREAM insufficient_test " f"TOPICS {kafka_topics[0]} " f"TRANSFORM kafka_transform.simple",
    )

    common.start_stream(stream_cursor, "insufficient_test")
    time.sleep(1)

    first_message = b"first message"
    kafka_producer.send(kafka_topics[0], first_message).get(timeout=60)

    common.kafka_check_vertex_exists_with_topic_and_payload(admin_cursor, kafka_topics[0], first_message)

    assert len(common.execute_and_fetch_all(admin_cursor, "MATCH (n) RETURN n")) == 1


def usernames(cursor):
    return {username for (username,) in common.execute_and_fetch_all(cursor, "SHOW USERS")}


def test_a_stream_runs_auth_statements_its_owner_may_run(request, kafka_producer, kafka_topics, connection):
    cursor = create_admin_user(request, "admin_user")
    common.execute_and_fetch_all(cursor, "CREATE USER owner")
    common.execute_and_fetch_all(cursor, "GRANT STREAM, AUTH TO owner")
    owner_cursor = get_cursor_with_user("owner")

    common.execute_and_fetch_all(
        owner_cursor, f"CREATE KAFKA STREAM auth_stream TOPICS {kafka_topics[0]} TRANSFORM kafka_transform.create_user"
    )
    common.start_stream(owner_cursor, "auth_stream")

    kafka_producer.send(kafka_topics[0], b"streamed").get(timeout=60)
    assert common.timed_wait(lambda: "streamed" in usernames(cursor))


def test_a_stream_checks_auth_statements_against_the_system_database(request, kafka_producer, kafka_topics, connection):
    # The owner holds AUTH on the stream's database only. An auth statement needs it on the system database, as it
    # does from a session, whichever database the stream runs in.
    cursor = create_admin_user(request, "admin_user")
    for query in [
        "CREATE DATABASE tenant",
        "CREATE ROLE tenant_auth",
        "GRANT STREAM, AUTH TO tenant_auth",
        "GRANT DATABASE tenant TO tenant_auth",
        "CREATE USER owner",
        "GRANT DATABASE tenant TO owner",
        "SET ROLE FOR owner TO tenant_auth ON tenant",
        "SET MAIN DATABASE tenant FOR owner",
    ]:
        common.execute_and_fetch_all(cursor, query)
    owner = common.connect(username="owner", password="")
    try:
        owner_cursor = owner.cursor()
        common.execute_and_fetch_all(
            owner_cursor,
            f"CREATE KAFKA STREAM tenant_stream TOPICS {kafka_topics[0]} TRANSFORM kafka_transform.create_user",
        )
        common.start_stream(owner_cursor, "tenant_stream")

        kafka_producer.send(kafka_topics[0], b"streamed").get(timeout=60)
        assert common.timed_wait(lambda: not common.get_is_running(owner_cursor, "tenant_stream"))
        assert "streamed" not in usernames(cursor)
    finally:
        owner.close()
        for query in [
            "USE DATABASE tenant",
            "DROP STREAM tenant_stream",
            "USE DATABASE memgraph",
            "DROP DATABASE tenant",
            "DROP USER owner",
            "DROP ROLE tenant_auth",
        ]:
            common.execute_and_fetch_all(cursor, query)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
