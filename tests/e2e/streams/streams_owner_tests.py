import sys
import pytest
import time
import mgclient
import common

@pytest.fixture(scope="module", autouse=True)
def license_setup():
    connection = common.connect()
    cursor = connection.cursor()
    common.execute_and_fetch_all(
        cursor, f"SET DATABASE SETTING 'enterprise.license' TO 'mglk-GAAAAAgAAAAAAAAATWVtZ3JhcGj/n3JOGAkAAAAAAAA='")
    common.execute_and_fetch_all(
        cursor, f"SET DATABASE SETTING 'organization.name' TO 'Memgraph'")
    yield

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


def test_ownerless_stream(producer, topics, connection):
    assert len(topics) > 0
    userless_cursor = connection.cursor()
    common.execute_and_fetch_all(userless_cursor,
                                 "CREATE STREAM ownerless "
                                 f"TOPICS {topics[0]} "
                                 f"TRANSFORM transform.simple")
    common.start_stream(userless_cursor, "ownerless")
    time.sleep(1)

    admin_user = "admin_user"
    create_admin_user(userless_cursor, admin_user)

    producer.send(topics[0], b"first message").get(timeout=60)
    assert common.timed_wait(
        lambda: not common.get_is_running(userless_cursor, "ownerless"))

    assert len(common.execute_and_fetch_all(
        userless_cursor, "MATCH (n) RETURN n")) == 0

    common.execute_and_fetch_all(userless_cursor, f"DROP USER {admin_user}")
    common.start_stream(userless_cursor, "ownerless")
    time.sleep(1)

    second_message = b"second message"
    producer.send(topics[0], second_message).get(timeout=60)
    common.check_vertex_exists_with_topic_and_payload(
        userless_cursor, topics[0], second_message)

    assert len(common.execute_and_fetch_all(
        userless_cursor, "MATCH (n) RETURN n")) == 1


def test_owner_is_shown(topics, connection):
    assert len(topics) > 0
    userless_cursor = connection.cursor()

    stream_user = "stream_user"
    create_stream_user(userless_cursor, stream_user)
    stream_cursor = get_cursor_with_user(stream_user)

    common.execute_and_fetch_all(stream_cursor, "CREATE STREAM test "
                                 f"TOPICS {topics[0]} "
                                 f"TRANSFORM transform.simple")

    common.check_stream_info(userless_cursor, "test", ("test", [
        topics[0]], "mg_consumer", None, None,
        "transform.simple", stream_user, False))


def test_insufficient_privileges(producer, topics, connection):
    assert len(topics) > 0
    userless_cursor = connection.cursor()

    admin_user = "admin_user"
    create_admin_user(userless_cursor, admin_user)
    admin_cursor = get_cursor_with_user(admin_user)

    stream_user = "stream_user"
    create_stream_user(userless_cursor, stream_user)
    stream_cursor = get_cursor_with_user(stream_user)

    common.execute_and_fetch_all(stream_cursor,
                                 "CREATE STREAM insufficient_test "
                                 f"TOPICS {topics[0]} "
                                 f"TRANSFORM transform.simple")

    # the stream is started by admin, but should check against the owner
    # privileges
    common.start_stream(admin_cursor, "insufficient_test")
    time.sleep(1)

    producer.send(topics[0], b"first message").get(timeout=60)
    assert common.timed_wait(
        lambda: not common.get_is_running(userless_cursor, "insufficient_test"))

    assert len(common.execute_and_fetch_all(
        userless_cursor, "MATCH (n) RETURN n")) == 0

    common.execute_and_fetch_all(
        admin_cursor, f"GRANT CREATE TO {stream_user}")
    common.start_stream(userless_cursor, "insufficient_test")
    time.sleep(1)

    second_message = b"second message"
    producer.send(topics[0], second_message).get(timeout=60)
    common.check_vertex_exists_with_topic_and_payload(
        userless_cursor, topics[0], second_message)

    assert len(common.execute_and_fetch_all(
        userless_cursor, "MATCH (n) RETURN n")) == 1


def test_happy_case(producer, topics, connection):
    assert len(topics) > 0
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
                                 "CREATE STREAM insufficient_test "
                                 f"TOPICS {topics[0]} "
                                 f"TRANSFORM transform.simple")

    common.start_stream(stream_cursor, "insufficient_test")
    time.sleep(1)

    first_message = b"first message"
    producer.send(topics[0], first_message).get(timeout=60)

    common.check_vertex_exists_with_topic_and_payload(
        userless_cursor, topics[0], first_message)

    assert len(common.execute_and_fetch_all(
        userless_cursor, "MATCH (n) RETURN n")) == 1


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
