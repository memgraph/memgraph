#!/usr/bin/python3

# Copyright 2026 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

# Live-consumer coverage for hot/cold databases over a real
# Kafka broker. The hot/cold v2 design stops a database's stream consumers on SUSPEND (Streams::Shutdown,
# without touching the durable is_running flag) and rebuilds them on RESUME (Streams::RestoreStreams,
# which re-Start()s any stream that was running). This test proves that end to end:
#   1. a running stream on a non-default database consumes a live Kafka message,
#   2. SUSPEND turns the database COLD and makes it (and its stream) inaccessible,
#   3. RESUME brings the stream back RUNNING and it consumes a fresh message produced after resume.

import sys

import common
import mgclient
import pytest
from mg_utils import mg_sleep_and_assert

KAFKA_PRODUCER_TIMEOUT = 60
AFTER_RESUME_MSG = b"after_resume"


def _show_databases(cursor):
    return {row[0]: row[1] for row in common.execute_and_fetch_all(cursor, "SHOW DATABASES")}


def test_stream_stops_on_suspend_and_restores_running_on_resume(kafka_producer, kafka_topics, connection):
    assert len(kafka_topics) > 0
    topic = kafka_topics[0]

    # Control session on the default database — issues SUSPEND/RESUME (which require sole access to the
    # target, so it must NOT be the session pinned to the hot/cold database).
    control = connection.cursor()
    common.execute_and_fetch_all(control, "CREATE DATABASE hcdb;")

    # Separate session pinned to hcdb to create + start the stream there.
    stream_conn = common.connect()
    scursor = stream_conn.cursor()
    common.execute_and_fetch_all(scursor, "USE DATABASE hcdb;")
    common.create_stream(scursor, "hc_stream", topic, "kafka_transform.simple")
    common.start_stream(scursor, "hc_stream")
    assert common.get_is_running(scursor, "hc_stream")

    # Live consumer works while HOT: produce a message, see the node land in hcdb.
    kafka_producer.send(topic, common.SIMPLE_MSG).get(timeout=KAFKA_PRODUCER_TIMEOUT)
    common.kafka_check_vertex_exists_with_topic_and_payload(scursor, topic, common.SIMPLE_MSG)

    # Release hcdb: the stream session must not pin it HOT, or SUSPEND can never reach sole access. The
    # running consumer's own per-batch accessors are released by the suspend arm (Streams::Shutdown).
    common.execute_and_fetch_all(scursor, "USE DATABASE memgraph;")

    # SUSPEND hcdb -> COLD. Its stream consumer is stopped and the database becomes inaccessible.
    common.execute_and_fetch_all(control, "SUSPEND DATABASE hcdb;")
    mg_sleep_and_assert("COLD", lambda: _show_databases(control).get("hcdb"))
    with pytest.raises(mgclient.DatabaseError) as exc:
        common.execute_and_fetch_all(scursor, "USE DATABASE hcdb;")
    assert "suspended (cold)" in str(exc.value), f"a cold database's stream must be inaccessible: {exc.value}"
    # leave the session on the default db (the failed USE did not switch it)
    common.execute_and_fetch_all(scursor, "USE DATABASE memgraph;")

    # RESUME hcdb -> HOT. RestoreStreams re-creates the consumer and, because it was running at suspend,
    # restarts it (durable is_running was never cleared).
    common.execute_and_fetch_all(control, "RESUME DATABASE hcdb;")
    common.execute_and_fetch_all(scursor, "USE DATABASE hcdb;")
    assert common.timed_wait(
        lambda: common.get_is_running(scursor, "hc_stream")
    ), "the stream must be RUNNING again after RESUME"

    # The restored consumer is live: a message produced after resume is consumed into hcdb.
    kafka_producer.send(topic, AFTER_RESUME_MSG).get(timeout=KAFKA_PRODUCER_TIMEOUT)
    common.kafka_check_vertex_exists_with_topic_and_payload(scursor, topic, AFTER_RESUME_MSG)

    # Cleanup: stop + drop the stream and the database while HOT.
    common.execute_and_fetch_all(scursor, "STOP STREAM hc_stream;")
    common.execute_and_fetch_all(scursor, "DROP STREAM hc_stream;")
    common.execute_and_fetch_all(scursor, "USE DATABASE memgraph;")
    common.execute_and_fetch_all(control, "DROP DATABASE hcdb;")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
