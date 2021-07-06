#!/usr/bin/python3

# To run these test locally a running Kafka sever is necessery. The test tries
# to connect on localhost:9092.

# All tests are implemented in this file, because using the same test fixtures
# in multiple files is not possible in a straightforward way

import sys
import pytest
import mgclient
import time
from multiprocessing import Process, Value
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

# These are the indices of the different values in the result of SHOW STREAM
# query
NAME = 0
TOPICS = 1
CONSUMER_GROUP = 2
BATCH_INTERVAL = 3
BATCH_SIZE = 4
TRANSFORM = 5
IS_RUNNING = 6

# These are the indices of the query and parameters in the result of CHECK
# STREAM query
QUERY = 0
PARAMS = 1

TRANSFORMATIONS_TO_CHECK = [
    "transform.simple", "transform.with_parameters"]

SIMPLE_MSG = b'message'


def execute_and_fetch_all(cursor, query):
    cursor.execute(query)
    return cursor.fetchall()


def connect():
    connection = mgclient.connect(host="localhost", port=7687)
    connection.autocommit = True
    return connection


@pytest.fixture(autouse=True)
def connection():
    connection = connect()
    yield connection
    cursor = connection.cursor()
    execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n")
    stream_infos = execute_and_fetch_all(cursor, "SHOW STREAMS")
    for stream_info in stream_infos:
        execute_and_fetch_all(cursor, f"DROP STREAM {stream_info[NAME]}")


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


def timed_wait(fun):
    start_time = time.time()
    seconds = 10

    while True:
        current_time = time.time()
        elapsed_time = current_time - start_time

        if elapsed_time > seconds:
            return False

        if fun():
            return True


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


def check_vertex_exists_with_topic_and_payload(cursor, topic, payload_bytes):
    assert check_one_result_row(cursor,
                                "MATCH (n: MESSAGE {"
                                f"payload: '{payload_bytes.decode('utf-8')}',"
                                f"topic: '{topic}'"
                                "}) RETURN n")


def get_stream_info(cursor, stream_name):
    stream_infos = execute_and_fetch_all(cursor, "SHOW STREAMS")
    for stream_info in stream_infos:
        if (stream_info[NAME] == stream_name):
            return stream_info

    return None


def get_is_running(cursor, stream_name):
    stream_info = get_stream_info(cursor, stream_name)

    assert stream_info
    return stream_info[IS_RUNNING]


def start_stream(cursor, stream_name):
    execute_and_fetch_all(cursor, f"START STREAM {stream_name}")

    assert get_is_running(cursor, stream_name)


def stop_stream(cursor, stream_name):
    execute_and_fetch_all(cursor, f"STOP STREAM {stream_name}")

    assert not get_is_running(cursor, stream_name)


def drop_stream(cursor, stream_name):
    execute_and_fetch_all(cursor, f"DROP STREAM {stream_name}")

    assert get_stream_info(cursor, stream_name) is None


def check_stream_info(cursor, stream_name, expected_stream_info):
    stream_info = get_stream_info(cursor, stream_name)
    assert len(stream_info) == len(expected_stream_info)
    for i in range(len(stream_info)):
        assert stream_info[i] == expected_stream_info[i]

##############################################
# Tests
##############################################


@pytest.mark.parametrize("transformation", TRANSFORMATIONS_TO_CHECK)
def test_simple(producer, topics, connection, transformation):
    assert len(topics) > 0
    cursor = connection.cursor()
    execute_and_fetch_all(cursor,
                          "CREATE STREAM test "
                          f"TOPICS {','.join(topics)} "
                          f"TRANSFORM {transformation}")
    start_stream(cursor, "test")
    time.sleep(5)

    for topic in topics:
        producer.send(topic, SIMPLE_MSG).get(timeout=60)

    for topic in topics:
        check_vertex_exists_with_topic_and_payload(
            cursor, topic, SIMPLE_MSG)


@pytest.mark.parametrize("transformation", TRANSFORMATIONS_TO_CHECK)
def test_separate_consumers(producer, topics, connection, transformation):
    assert len(topics) > 0
    cursor = connection.cursor()

    stream_names = []
    for topic in topics:
        stream_name = "stream_" + topic
        stream_names.append(stream_name)
        execute_and_fetch_all(cursor,
                              f"CREATE STREAM {stream_name} "
                              f"TOPICS {topic} "
                              f"TRANSFORM {transformation}")

    for stream_name in stream_names:
        start_stream(cursor, stream_name)

    time.sleep(5)

    for topic in topics:
        producer.send(topic, SIMPLE_MSG).get(timeout=60)

    for topic in topics:
        check_vertex_exists_with_topic_and_payload(
            cursor, topic, SIMPLE_MSG)


def test_start_from_last_committed_offset(producer, topics, connection):
    # This test creates a stream, consumes a message to have a committed
    # offset, then destroys the stream. A new message is sent before the
    # stream is recreated and then restarted. This simulates when Memgraph is
    # stopped (stream is destroyed) and then restarted (stream is recreated).
    # This is of course not as good as restarting memgraph would be, but
    # restarting Memgraph during a single workload cannot be done currently.
    assert len(topics) > 0
    cursor = connection.cursor()
    execute_and_fetch_all(cursor,
                          "CREATE STREAM test "
                          f"TOPICS {topics[0]} "
                          "TRANSFORM transform.simple")
    start_stream(cursor, "test")
    time.sleep(1)

    producer.send(topics[0], SIMPLE_MSG).get(timeout=60)

    check_vertex_exists_with_topic_and_payload(
        cursor, topics[0], SIMPLE_MSG)

    stop_stream(cursor, "test")
    drop_stream(cursor, "test")

    messages = [b"second message", b"third message"]
    for message in messages:
        producer.send(topics[0], message).get(timeout=60)

    for message in messages:
        vertices_with_msg = execute_and_fetch_all(cursor,
                                                  "MATCH (n: MESSAGE {"
                                                  f"payload: '{message.decode('utf-8')}'"
                                                  "}) RETURN n")

        assert len(vertices_with_msg) == 0

    execute_and_fetch_all(cursor,
                          "CREATE STREAM test "
                          f"TOPICS {topics[0]} "
                          "TRANSFORM transform.simple")
    start_stream(cursor, "test")

    for message in messages:
        check_vertex_exists_with_topic_and_payload(
            cursor, topics[0], message)


@pytest.mark.parametrize("transformation", TRANSFORMATIONS_TO_CHECK)
def test_check_stream(producer, topics, connection, transformation):
    assert len(topics) > 0
    cursor = connection.cursor()
    execute_and_fetch_all(cursor,
                          "CREATE STREAM test "
                          f"TOPICS {topics[0]} "
                          f"TRANSFORM {transformation} "
                          "BATCH_SIZE 1")
    start_stream(cursor, "test")
    time.sleep(1)

    producer.send(topics[0], SIMPLE_MSG).get(timeout=60)
    stop_stream(cursor, "test")

    messages = [b"first message", b"second message", b"third message"]
    for message in messages:
        producer.send(topics[0], message).get(timeout=60)

    def check_check_stream(batch_limit):
        assert transformation == "transform.simple" \
            or transformation == "transform.with_parameters"
        test_results = execute_and_fetch_all(
            cursor, f"CHECK STREAM test BATCH_LIMIT {batch_limit}")
        assert len(test_results) == batch_limit

        for i in range(batch_limit):
            message_as_str = messages[i].decode('utf-8')
            if transformation == "transform.simple":
                assert f"payload: '{message_as_str}'" in \
                    test_results[i][QUERY]
                assert test_results[i][PARAMS] is None
            else:
                assert test_results[i][QUERY] == ("CREATE (n:MESSAGE "
                                                  "{timestamp: $timestamp, "
                                                  "payload: $payload, "
                                                  "topic: $topic})")
                parameters = test_results[i][PARAMS]
                # this is not a very sofisticated test, but checks if
                # timestamp has some kind of value
                assert parameters["timestamp"] > 1000000000000
                assert parameters["topic"] == topics[0]
                assert parameters["payload"] == message_as_str

    check_check_stream(1)
    check_check_stream(2)
    check_check_stream(3)
    start_stream(cursor, "test")

    for message in messages:
        check_vertex_exists_with_topic_and_payload(
            cursor, topics[0], message)


def test_show_streams(producer, topics, connection):
    assert len(topics) > 1
    cursor = connection.cursor()
    execute_and_fetch_all(cursor,
                          "CREATE STREAM default_values "
                          f"TOPICS {topics[0]} "
                          f"TRANSFORM transform.simple")

    consumer_group = "my_special_consumer_group"
    batch_interval = 42
    batch_size = 3
    execute_and_fetch_all(cursor,
                          "CREATE STREAM complex_values "
                          f"TOPICS {','.join(topics)} "
                          f"TRANSFORM transform.with_parameters "
                          f"CONSUMER_GROUP {consumer_group} "
                          f"BATCH_INTERVAL {batch_interval} "
                          f"BATCH_SIZE {batch_size} ")

    assert len(execute_and_fetch_all(cursor, "SHOW STREAMS")) == 2

    check_stream_info(cursor, "default_values", ("default_values", [
                      topics[0]], "mg_consumer", None, None,
        "transform.simple", False))

    check_stream_info(cursor, "complex_values", ("complex_values", topics,
                      consumer_group, batch_interval, batch_size,
                                                 "transform.with_parameters",
                                                 False))


@pytest.mark.parametrize("operation", ["START", "STOP"])
def test_start_and_stop_during_check(producer, topics, connection, operation):
    # This test is quite complex. The goal is to call START/STOP queries
    # while a CHECK query is waiting for its result. Because the Global
    # Interpreter Lock, running queries on multiple threads is not useful,
    # because only one of them can call Cursor::execute at a time. Therefore
    # multiple processes are used to execute the queries, because different
    # processes have different GILs.
    # The counter variables are thread- and process-safe variables to
    # synchronize between the different processes. Each value represents a
    # specific phase of the execution of the processes.
    assert len(topics) > 1
    assert operation == "START" or operation == "STOP"
    cursor = connection.cursor()
    execute_and_fetch_all(cursor,
                          "CREATE STREAM test_stream "
                          f"TOPICS {topics[0]} "
                          f"TRANSFORM transform.simple")

    check_counter = Value('i', 0)
    check_result_len = Value('i', 0)
    operation_counter = Value('i', 0)

    CHECK_BEFORE_EXECUTE = 1
    CHECK_AFTER_FETCHALL = 2
    CHECK_CORRECT_RESULT = 3
    CHECK_INCORRECT_RESULT = 4

    def call_check(counter, result_len):
        # This process will call the CHECK query and increment the counter
        # based on its progress and expected behavior
        connection = connect()
        cursor = connection.cursor()
        counter.value = CHECK_BEFORE_EXECUTE
        result = execute_and_fetch_all(cursor, "CHECK STREAM test_stream")
        result_len.value = len(result)
        counter.value = CHECK_AFTER_FETCHALL
        if len(result) > 0 and "payload: 'message'" in result[0][QUERY]:
            counter.value = CHECK_CORRECT_RESULT
        else:
            counter.value = CHECK_INCORRECT_RESULT

    OP_BEFORE_EXECUTE = 1
    OP_AFTER_FETCHALL = 2
    OP_ALREADY_STOPPED_EXCEPTION = 3
    OP_INCORRECT_ALREADY_STOPPED_EXCEPTION = 4
    OP_UNEXPECTED_EXCEPTION = 5

    def call_operation(counter):
        # This porcess will call the query with the specified operation and
        # increment the counter based on its progress and expected behavior
        connection = connect()
        cursor = connection.cursor()
        counter.value = OP_BEFORE_EXECUTE
        try:
            execute_and_fetch_all(cursor, f"{operation} STREAM test_stream")
            counter.value = OP_AFTER_FETCHALL
        except mgclient.DatabaseError as e:
            if "Kafka consumer test_stream is already stopped" in str(e):
                counter.value = OP_ALREADY_STOPPED_EXCEPTION
            else:
                counter.value = OP_INCORRECT_ALREADY_STOPPED_EXCEPTION
        except Exception:
            counter.value = OP_UNEXPECTED_EXCEPTION

    check_stream_proc = Process(
        target=call_check, daemon=True, args=(check_counter, check_result_len))
    operation_proc = Process(target=call_operation,
                             daemon=True, args=(operation_counter,))

    try:
        check_stream_proc.start()

        time.sleep(0.5)

        assert timed_wait(lambda: check_counter.value == CHECK_BEFORE_EXECUTE)
        operation_proc.start()
        assert timed_wait(lambda: operation_counter.value == OP_BEFORE_EXECUTE)

        producer.send(topics[0], SIMPLE_MSG).get(timeout=60)
        assert timed_wait(lambda: check_counter.value > CHECK_AFTER_FETCHALL)
        assert check_counter.value == CHECK_CORRECT_RESULT
        assert check_result_len.value == 1
        check_stream_proc.join()

        operation_proc.join()
        if operation == "START":
            assert operation_counter.value == OP_AFTER_FETCHALL
            assert get_is_running(cursor, "test_stream")
        else:
            assert operation_counter.value == OP_ALREADY_STOPPED_EXCEPTION
            assert not get_is_running(cursor, "test_stream")

    finally:
        # to make sure CHECK STREAM finishes
        producer.send(topics[0], SIMPLE_MSG).get(timeout=60)
        if check_stream_proc.is_alive():
            check_stream_proc.terminate()
        if operation_proc.is_alive():
            operation_proc.terminate()


def test_check_already_started_stream(producer, topics, connection):
    assert len(topics) > 0
    cursor = connection.cursor()

    execute_and_fetch_all(cursor,
                          "CREATE STREAM started_stream "
                          f"TOPICS {topics[0]} "
                          f"TRANSFORM transform.simple")
    start_stream(cursor, "started_stream")

    with pytest.raises(mgclient.DatabaseError):
        execute_and_fetch_all(cursor, "CHECK STREAM started_stream")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
