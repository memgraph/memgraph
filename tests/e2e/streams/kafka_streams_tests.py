#!/usr/bin/python3

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
import mgclient
import time
from multiprocessing import Process, Value
import common

TRANSFORMATIONS_TO_CHECK_C = ["empty_transformation"]

TRANSFORMATIONS_TO_CHECK_PY = ["kafka_transform.simple", "kafka_transform.with_parameters"]


@pytest.mark.parametrize("transformation", TRANSFORMATIONS_TO_CHECK_PY)
def test_simple(kafka_producer, kafka_topics, connection, transformation):
    assert len(kafka_topics) > 0
    cursor = connection.cursor()
    common.execute_and_fetch_all(
        cursor,
        "CREATE KAFKA STREAM test " f"TOPICS {','.join(kafka_topics)} " f"TRANSFORM {transformation}",
    )
    common.start_stream(cursor, "test")
    time.sleep(5)

    for topic in kafka_topics:
        kafka_producer.send(topic, common.SIMPLE_MSG).get(timeout=60)

    for topic in kafka_topics:
        common.kafka_check_vertex_exists_with_topic_and_payload(cursor, topic, common.SIMPLE_MSG)


@pytest.mark.parametrize("transformation", TRANSFORMATIONS_TO_CHECK_PY)
def test_separate_consumers(kafka_producer, kafka_topics, connection, transformation):
    assert len(kafka_topics) > 0
    cursor = connection.cursor()

    stream_names = []
    for topic in kafka_topics:
        stream_name = "stream_" + topic
        stream_names.append(stream_name)
        common.execute_and_fetch_all(
            cursor,
            f"CREATE KAFKA STREAM {stream_name} " f"TOPICS {topic} " f"TRANSFORM {transformation}",
        )

    for stream_name in stream_names:
        common.start_stream(cursor, stream_name)

    time.sleep(5)

    for topic in kafka_topics:
        kafka_producer.send(topic, common.SIMPLE_MSG).get(timeout=60)

    for topic in kafka_topics:
        common.kafka_check_vertex_exists_with_topic_and_payload(cursor, topic, common.SIMPLE_MSG)


def test_start_from_last_committed_offset(kafka_producer, kafka_topics, connection):
    # This test creates a stream, consumes a message to have a committed
    # offset, then destroys the stream. A new message is sent before the
    # stream is recreated and then restarted. This simulates when Memgraph is
    # stopped (stream is destroyed) and then restarted (stream is recreated).
    # This is of course not as good as restarting memgraph would be, but
    # restarting Memgraph during a single workload cannot be done currently.
    assert len(kafka_topics) > 0
    cursor = connection.cursor()
    common.execute_and_fetch_all(
        cursor,
        "CREATE KAFKA STREAM test " f"TOPICS {kafka_topics[0]} " "TRANSFORM kafka_transform.simple",
    )
    common.start_stream(cursor, "test")
    time.sleep(1)

    kafka_producer.send(kafka_topics[0], common.SIMPLE_MSG).get(timeout=60)

    common.kafka_check_vertex_exists_with_topic_and_payload(cursor, kafka_topics[0], common.SIMPLE_MSG)

    common.stop_stream(cursor, "test")
    common.drop_stream(cursor, "test")

    messages = [b"second message", b"third message"]
    for message in messages:
        kafka_producer.send(kafka_topics[0], message).get(timeout=60)

    for message in messages:
        vertices_with_msg = common.execute_and_fetch_all(
            cursor,
            "MATCH (n: MESSAGE {" f"payload: '{message.decode('utf-8')}'" "}) RETURN n",
        )

        assert len(vertices_with_msg) == 0

    common.execute_and_fetch_all(
        cursor,
        "CREATE KAFKA STREAM test " f"TOPICS {kafka_topics[0]} " "TRANSFORM kafka_transform.simple",
    )
    common.start_stream(cursor, "test")

    for message in messages:
        common.kafka_check_vertex_exists_with_topic_and_payload(cursor, kafka_topics[0], message)


@pytest.mark.parametrize("transformation", TRANSFORMATIONS_TO_CHECK_PY)
def test_check_stream(kafka_producer, kafka_topics, connection, transformation):
    assert len(kafka_topics) > 0
    kBatchSize = 1
    kIndexOfFirstBatch = 0
    cursor = connection.cursor()
    common.execute_and_fetch_all(
        cursor,
        "CREATE KAFKA STREAM test "
        f"TOPICS {kafka_topics[0]} "
        f"TRANSFORM {transformation} "
        f"BATCH_SIZE {kBatchSize}",
    )
    common.start_stream(cursor, "test")
    time.sleep(1)

    kafka_producer.send(kafka_topics[0], common.SIMPLE_MSG).get(timeout=60)
    common.stop_stream(cursor, "test")

    messages = [b"first message", b"second message", b"third message"]
    for message in messages:
        kafka_producer.send(kafka_topics[0], message).get(timeout=60)

    def check_check_stream(batch_limit):
        assert transformation == "kafka_transform.simple" or transformation == "kafka_transform.with_parameters"
        test_results = common.execute_and_fetch_all(cursor, f"CHECK STREAM test BATCH_LIMIT {batch_limit}")
        assert len(test_results) == batch_limit

        for i in range(batch_limit):
            message_as_str = messages[i].decode("utf-8")
            assert (
                kBatchSize == 1
            )  # If batch size != 1, then the usage of kIndexOfFirstBatch must change: the result will have a list of queries (pair<parameters,query>)

            if transformation == "kafka_transform.simple":
                assert (
                    f"payload: '{message_as_str}'"
                    in test_results[i][common.QUERIES][kIndexOfFirstBatch][common.QUERY_LITERAL]
                )
                assert test_results[i][common.QUERIES][kIndexOfFirstBatch][common.PARAMETERS_LITERAL] is None
            else:
                assert (
                    f"payload: $payload" in test_results[i][common.QUERIES][kIndexOfFirstBatch][common.QUERY_LITERAL]
                    and f"topic: $topic" in test_results[i][common.QUERIES][kIndexOfFirstBatch][common.QUERY_LITERAL]
                )
                parameters = test_results[i][common.QUERIES][kIndexOfFirstBatch][common.PARAMETERS_LITERAL]
                # this is not a very sofisticated test, but checks if
                # timestamp has some kind of value
                assert parameters["timestamp"] > 1000000000000
                assert parameters["topic"] == kafka_topics[0]
                assert parameters["payload"] == message_as_str

    check_check_stream(1)
    check_check_stream(2)
    check_check_stream(3)
    common.start_stream(cursor, "test")

    for message in messages:
        common.kafka_check_vertex_exists_with_topic_and_payload(cursor, kafka_topics[0], message)


def test_show_streams(kafka_producer, kafka_topics, connection):
    assert len(kafka_topics) > 1
    cursor = connection.cursor()
    common.execute_and_fetch_all(
        cursor,
        "CREATE KAFKA STREAM default_values "
        f"TOPICS {kafka_topics[0]} "
        f"TRANSFORM kafka_transform.simple "
        f"BOOTSTRAP_SERVERS 'localhost:9092'",
    )

    consumer_group = "my_special_consumer_group"
    batch_interval = 42
    batch_size = 3
    common.execute_and_fetch_all(
        cursor,
        "CREATE KAFKA STREAM complex_values "
        f"TOPICS {','.join(kafka_topics)} "
        f"TRANSFORM kafka_transform.with_parameters "
        f"CONSUMER_GROUP {consumer_group} "
        f"BATCH_INTERVAL {batch_interval} "
        f"BATCH_SIZE {batch_size} ",
    )

    assert len(common.execute_and_fetch_all(cursor, "SHOW STREAMS")) == 2

    common.check_stream_info(
        cursor,
        "default_values",
        ("default_values", "kafka", 100, 1000, "kafka_transform.simple", None, False),
    )

    common.check_stream_info(
        cursor,
        "complex_values",
        (
            "complex_values",
            "kafka",
            batch_interval,
            batch_size,
            "kafka_transform.with_parameters",
            None,
            False,
        ),
    )


@pytest.mark.parametrize("operation", ["START", "STOP"])
def test_start_and_stop_during_check(kafka_producer, kafka_topics, connection, operation):
    assert len(kafka_topics) > 1
    kBatchSize = 1

    def stream_creator(stream_name):
        return f"CREATE KAFKA STREAM {stream_name} TOPICS {kafka_topics[0]} TRANSFORM kafka_transform.simple BATCH_SIZE {kBatchSize}"

    def message_sender(msg):
        kafka_producer.send(kafka_topics[0], msg).get(timeout=60)

    common.test_start_and_stop_during_check(
        operation,
        connection,
        stream_creator,
        message_sender,
        "Kafka consumer test_stream is already stopped",
        kBatchSize,
    )


def test_check_already_started_stream(kafka_topics, connection):
    assert len(kafka_topics) > 0
    cursor = connection.cursor()

    common.execute_and_fetch_all(
        cursor,
        "CREATE KAFKA STREAM started_stream " f"TOPICS {kafka_topics[0]} " f"TRANSFORM kafka_transform.simple",
    )
    common.start_stream(cursor, "started_stream")

    with pytest.raises(mgclient.DatabaseError):
        common.execute_and_fetch_all(cursor, "CHECK STREAM started_stream")


def test_start_checked_stream_after_timeout(kafka_topics, connection):
    def stream_creator(stream_name):
        return f"CREATE KAFKA STREAM {stream_name} TOPICS {kafka_topics[0]} TRANSFORM kafka_transform.simple"

    common.test_start_checked_stream_after_timeout(connection, stream_creator)


def test_restart_after_error(kafka_producer, kafka_topics, connection):
    cursor = connection.cursor()
    common.execute_and_fetch_all(
        cursor,
        "CREATE KAFKA STREAM test_stream " f"TOPICS {kafka_topics[0]} " f"TRANSFORM kafka_transform.query",
    )

    common.start_stream(cursor, "test_stream")
    time.sleep(1)

    kafka_producer.send(kafka_topics[0], common.SIMPLE_MSG).get(timeout=60)
    assert common.timed_wait(lambda: not common.get_is_running(cursor, "test_stream"))

    common.start_stream(cursor, "test_stream")
    time.sleep(1)
    kafka_producer.send(kafka_topics[0], b"CREATE (n:VERTEX { id : 42 })")
    assert common.check_one_result_row(cursor, "MATCH (n:VERTEX { id : 42 }) RETURN n")


@pytest.mark.parametrize("transformation", TRANSFORMATIONS_TO_CHECK_PY)
def test_bootstrap_server(kafka_producer, kafka_topics, connection, transformation):
    assert len(kafka_topics) > 0
    cursor = connection.cursor()
    local = "localhost:9092"
    common.execute_and_fetch_all(
        cursor,
        "CREATE KAFKA STREAM test "
        f"TOPICS {','.join(kafka_topics)} "
        f"TRANSFORM {transformation} "
        f"BOOTSTRAP_SERVERS '{local}'",
    )
    common.start_stream(cursor, "test")
    time.sleep(5)

    for topic in kafka_topics:
        kafka_producer.send(topic, common.SIMPLE_MSG).get(timeout=60)

    for topic in kafka_topics:
        common.kafka_check_vertex_exists_with_topic_and_payload(cursor, topic, common.SIMPLE_MSG)


@pytest.mark.parametrize("transformation", TRANSFORMATIONS_TO_CHECK_PY)
def test_bootstrap_server_empty(kafka_producer, kafka_topics, connection, transformation):
    assert len(kafka_topics) > 0
    cursor = connection.cursor()
    with pytest.raises(mgclient.DatabaseError):
        common.execute_and_fetch_all(
            cursor,
            "CREATE KAFKA STREAM test "
            f"TOPICS {','.join(kafka_topics)} "
            f"TRANSFORM {transformation} "
            "BOOTSTRAP_SERVERS ''",
        )


@pytest.mark.parametrize("transformation", TRANSFORMATIONS_TO_CHECK_PY)
def test_set_offset(kafka_producer, kafka_topics, connection, transformation):
    assert len(kafka_topics) > 0
    cursor = connection.cursor()
    common.execute_and_fetch_all(
        cursor,
        "CREATE KAFKA STREAM test " f"TOPICS {kafka_topics[0]} " f"TRANSFORM {transformation} " "BATCH_SIZE 1",
    )

    messages = [f"{i} message" for i in range(1, 21)]
    for message in messages:
        kafka_producer.send(kafka_topics[0], message.encode()).get(timeout=60)

    def consume(expected_msgs):
        common.start_stream(cursor, "test")
        if len(expected_msgs) == 0:
            time.sleep(2)
        else:
            assert common.check_one_result_row(
                cursor,
                (f"MATCH (n: MESSAGE {{payload: '{expected_msgs[-1]}'}})" "RETURN n"),
            )
        common.stop_stream(cursor, "test")
        res = common.execute_and_fetch_all(cursor, "MATCH (n) RETURN n.payload")
        return res

    def execute_set_offset_and_consume(id, expected_msgs):
        common.execute_and_fetch_all(cursor, f"CALL mg.kafka_set_stream_offset('test', {id})")
        return consume(expected_msgs)

    with pytest.raises(mgclient.DatabaseError):
        res = common.execute_and_fetch_all(cursor, "CALL mg.kafka_set_stream_offset('foo', 10)")

    def comparison_check(a, b):
        return a == str(b).strip("'(,)")

    res = execute_set_offset_and_consume(10, messages[10:])
    assert len(res) == 10
    assert all([comparison_check(a, b) for a, b in zip(messages[10:], res)])
    common.execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n")

    res = execute_set_offset_and_consume(-1, messages)
    assert len(res) == len(messages)
    assert all([comparison_check(a, b) for a, b in zip(messages, res)])
    res = common.execute_and_fetch_all(cursor, "MATCH (n) return n.offset")
    assert all([comparison_check(str(i), res[i]) for i in range(1, 20)])
    res = common.execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n")

    res = execute_set_offset_and_consume(-2, [])
    assert len(res) == 0
    last_msg = "Final Message"
    kafka_producer.send(kafka_topics[0], last_msg.encode()).get(timeout=60)
    res = consume([last_msg])
    assert len(res) == 1
    assert comparison_check("Final Message", res[0])
    common.execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n")


def test_info_procedure(kafka_topics, connection):
    cursor = connection.cursor()
    stream_name = "test_stream"
    configs = {"sasl.username": "michael.scott"}
    local = "localhost:9092"
    credentials = {"sasl.password": "S3cr3tP4ssw0rd"}
    consumer_group = "ConsumerGr"
    common.execute_and_fetch_all(
        cursor,
        f"CREATE KAFKA STREAM {stream_name} "
        f"TOPICS {','.join(kafka_topics)} "
        f"TRANSFORM pulsar_transform.simple "
        f"CONSUMER_GROUP {consumer_group} "
        f"BOOTSTRAP_SERVERS '{local}' "
        f"CONFIGS {configs} "
        f"CREDENTIALS {credentials}",
    )

    stream_info = common.execute_and_fetch_all(cursor, f"CALL mg.kafka_stream_info('{stream_name}') YIELD *")

    reducted_credentials = {key: "<REDUCTED>" for key in credentials.keys()}

    expected_stream_info = [(local, configs, consumer_group, reducted_credentials, kafka_topics)]
    common.validate_info(stream_info, expected_stream_info)


@pytest.mark.parametrize("transformation", TRANSFORMATIONS_TO_CHECK_C)
def test_load_c_transformations(connection, transformation):
    cursor = connection.cursor()
    query = (
        "CALL mg.transformations() YIELD * WITH name WHERE name STARTS WITH 'c_transformations."
        + transformation
        + "' RETURN name"
    )
    result = common.execute_and_fetch_all(cursor, query)
    assert len(result) == 1
    assert result[0][0] == "c_transformations." + transformation


def test_check_stream__same_nOf_queries_than_messages(kafka_producer, kafka_topics, connection):
    assert len(kafka_topics) > 0

    kTransformation = "kafka_transform.check_stream_no_filtering"
    kBatchSize = 2
    kBatchLimit = 3

    cursor = connection.cursor()
    common.execute_and_fetch_all(
        cursor,
        "CREATE KAFKA STREAM test "
        f"TOPICS {kafka_topics[0]} "
        f"TRANSFORM  {kTransformation} "
        f"BATCH_SIZE {kBatchSize}",
    )
    time.sleep(1)

    common.start_stream(cursor, "test")
    time.sleep(1)

    kafka_producer.send(kafka_topics[0], common.SIMPLE_MSG).get(timeout=60)
    common.stop_stream(cursor, "test")

    messages = [b"01", b"02", b"03", b"04", b"05", b"06"]
    for message in messages:
        kafka_producer.send(kafka_topics[0], message).get(timeout=60)

    time.sleep(1)

    test_results = common.execute_and_fetch_all(cursor, f"CHECK STREAM test BATCH_LIMIT {kBatchLimit}")

    # Transformation does not do any filtering and simply create queries as "Messages: {contentOfMessage}". Queries should be like:
    # -Batch 1: [{parameters: {"value": "Parameter: 01"}, query: "Message: 01"},
    #            {parameters: {"value": "Parameter: 02"}, query: "Message: 02"}]
    # -Batch 2: [{parameters: {"value": "Parameter: 03"}, query: "Message: 03"},
    #            {parameters: {"value": "Parameter: 04"}, query: "Message: 04"}]
    # -Batch 3: [{parameters: {"value": "Parameter: 05"}, query: "Message: 05"},
    #            {parameters: {"value": "Parameter: 06"}, query: "Message: 06"}]

    assert len(test_results) == kBatchLimit

    expected_raw_message_1 = ["01", "02"]
    expected_raw_message_2 = ["03", "04"]
    expected_raw_message_3 = ["05", "06"]

    assert expected_raw_message_1 == test_results[0][common.RAWMESSAGES]
    assert expected_raw_message_2 == test_results[1][common.RAWMESSAGES]
    assert expected_raw_message_3 == test_results[2][common.RAWMESSAGES]

    expected_Messages_Batch_1 = ["Message: 01", "Message: 02"]
    expected_Messages_Batch_2 = ["Message: 03", "Message: 04"]
    expected_Messages_Batch_3 = ["Message: 05", "Message: 06"]

    assert len(expected_Messages_Batch_1) == len(test_results[0][common.QUERIES])
    assert len(expected_Messages_Batch_2) == len(test_results[1][common.QUERIES])
    assert len(expected_Messages_Batch_3) == len(test_results[2][common.QUERIES])

    for index in range(kBatchSize):
        assert expected_Messages_Batch_1[index] in test_results[0][common.QUERIES][index][common.QUERY_LITERAL]
        assert expected_Messages_Batch_2[index] in test_results[1][common.QUERIES][index][common.QUERY_LITERAL]
        assert expected_Messages_Batch_3[index] in test_results[2][common.QUERIES][index][common.QUERY_LITERAL]

    expected_Parameters_Batch_1 = ["Parameter: 01", "Parameter: 02"]
    expected_Parameters_Batch_2 = ["Parameter: 03", "Parameter: 04"]
    expected_Parameters_Batch_3 = ["Parameter: 05", "Parameter: 06"]

    value_literal = "value"
    for index in range(kBatchSize):
        assert (
            expected_Parameters_Batch_1[index]
            in test_results[0][common.QUERIES][index][common.PARAMETERS_LITERAL][value_literal]
        )
        assert (
            expected_Parameters_Batch_2[index]
            in test_results[1][common.QUERIES][index][common.PARAMETERS_LITERAL][value_literal]
        )
        assert (
            expected_Parameters_Batch_3[index]
            in test_results[2][common.QUERIES][index][common.PARAMETERS_LITERAL][value_literal]
        )


def test_check_stream__different_nOf_queries_than_messages(kafka_producer, kafka_topics, connection):
    assert len(kafka_topics) > 0

    kTransformation = "kafka_transform.check_stream_with_filtering"
    kBatchSize = 2
    kBatchLimit = 3

    cursor = connection.cursor()
    common.execute_and_fetch_all(
        cursor,
        "CREATE KAFKA STREAM test "
        f"TOPICS {kafka_topics[0]} "
        f"TRANSFORM  {kTransformation} "
        f"BATCH_SIZE {kBatchSize}",
    )
    time.sleep(1)

    common.start_stream(cursor, "test")
    time.sleep(1)

    kafka_producer.send(kafka_topics[0], common.SIMPLE_MSG).get(timeout=60)
    common.stop_stream(cursor, "test")

    messages = [b"a_01", b"a_02", b"03", b"04", b"b_05", b"06"]
    for message in messages:
        kafka_producer.send(kafka_topics[0], message).get(timeout=60)

    time.sleep(1)

    test_results = common.execute_and_fetch_all(cursor, f"CHECK STREAM test BATCH_LIMIT {kBatchLimit}")

    # Transformation does some filtering: if message contains "a", it is ignored.
    # Transformation also has special rule to create query if message is "b": it create more queries.
    #
    # Queries should be like:
    # -Batch 1: []
    # -Batch 2: [{parameters: {"value": "Parameter: 03"}, query: "Message: 03"},
    #            {parameters: {"value": "Parameter: 04"}, query: "Message: 04"}]
    # -Batch 3: [{parameters: {"value": "Parameter: 05"}, query: "Message: 05"},
    #            {parameters: {"value": "Parameter: extra_05"}, query: "Message: extra_05"}
    #            {parameters: {"value": "Parameter: 06"}, query: "Message: 06"}]

    assert len(test_results) == kBatchLimit

    expected_raw_message_1 = ["a_01", "a_02"]
    expected_raw_message_2 = ["03", "04"]
    expected_raw_message_3 = ["b_05", "06"]

    assert expected_raw_message_1 == test_results[0][common.RAWMESSAGES]
    assert expected_raw_message_2 == test_results[1][common.RAWMESSAGES]
    assert expected_raw_message_3 == test_results[2][common.RAWMESSAGES]

    expected_Messages_Batch_1 = []
    expected_Messages_Batch_2 = ["Message: 03", "Message: 04"]
    expected_Messages_Batch_3 = ["Message: b_05", "Message: extra_b_05", "Message: 06"]

    assert len(expected_Messages_Batch_1) == len(test_results[0][common.QUERIES])
    assert len(expected_Messages_Batch_2) == len(test_results[1][common.QUERIES])
    assert len(expected_Messages_Batch_3) == len(test_results[2][common.QUERIES])

    for index in range(kBatchSize):
        assert expected_Messages_Batch_2[index] in test_results[1][common.QUERIES][index][common.QUERY_LITERAL]

    for index in range(kBatchSize + 1):
        assert expected_Messages_Batch_3[index] in test_results[2][common.QUERIES][index][common.QUERY_LITERAL]

    expected_Parameters_Batch_2 = ["Parameter: 03", "Parameter: 04"]
    expected_Parameters_Batch_3 = ["Parameter: b_05", "Parameter: extra_b_05", "Parameter: 06"]

    value_literal = "value"
    for index in range(kBatchSize):
        assert (
            expected_Parameters_Batch_2[index]
            in test_results[1][common.QUERIES][index][common.PARAMETERS_LITERAL][value_literal]
        )

    for index in range(kBatchSize + 1):
        assert (
            expected_Parameters_Batch_3[index]
            in test_results[2][common.QUERIES][index][common.PARAMETERS_LITERAL][value_literal]
        )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
