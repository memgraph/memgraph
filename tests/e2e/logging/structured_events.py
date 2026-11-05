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

import glob
import json
import os
import sys
import time
from contextlib import closing

import mgclient
import pytest
from neo4j import Auth, GraphDatabase
from neo4j.exceptions import Neo4jError

_BUILD_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
LOG_GLOB = os.path.join(_BUILD_DIR, "e2e", "logs", "structured_events*.log")


def _log_path():
    candidates = glob.glob(LOG_GLOB)
    assert candidates, f"No structured logging file matching {LOG_GLOB}"
    return max(candidates, key=os.path.getmtime)


def _connect():
    conn = mgclient.connect(host="localhost", port=7687)
    conn.autocommit = True
    return conn


def _run(conn, query):
    cursor = conn.cursor()
    cursor.execute(query)
    return cursor.fetchall() if cursor.description is not None else []


def _closed(events, session_id):
    return any(
        record["event"]["name"] == "connection.closed" and record["attributes"]["session.id"] == session_id
        for record in events
    )


def _wait_events(path, offset, predicate):
    deadline = time.monotonic() + 5
    while True:
        with open(path) as log:
            log.seek(offset)
            lines = log.read().splitlines(keepends=True)
        records = [json.loads(line) for line in lines if line.endswith("\n")]
        events = [record for record in records if "event" in record]
        if predicate(events):
            return events
        if time.monotonic() >= deadline:
            pytest.fail(f"Expected structured events were not observed: {events}")
        time.sleep(0.05)


def test_query_events_correlate_with_connection_and_keep_precommit_transaction():
    path = _log_path()
    offset = os.path.getsize(path)
    with closing(_connect()) as conn:
        session_id = _run(conn, "SET SESSION TRACE OFF")[0][0]
        _run(conn, 'SET SESSION SETTING "log.failed_queries" TO "true"')
        _run(conn, 'SET SESSION SETTING "log.query_plan" TO "false"')
        _run(conn, 'SET SESSION SETTING "log.min_duration_ms" TO "0"')
        _run(conn, "RETURN 'structured_slow_marker'")
        with pytest.raises(mgclient.DatabaseError):
            _run(conn, "RETURN 1 / 0 /* structured_failure_marker */")

    events = _wait_events(
        path,
        offset,
        lambda records: _closed(records, session_id),
    )
    slow = next(
        record
        for record in events
        if record["event"]["name"] == "query.slow" and "structured_slow_marker" in record["attributes"]["db.query.text"]
    )
    failed = next(
        record
        for record in events
        if record["event"]["name"] == "query.failed"
        and "structured_failure_marker" in record["attributes"]["db.query.text"]
    )
    for record in (slow, failed):
        attributes = record["attributes"]
        assert isinstance(attributes["transaction.id"], int)
        assert attributes["transaction.id"] > 0
        assert attributes["db.namespace"] == "memgraph"
        assert any(
            connection["event"]["name"] == "connection.accepted"
            and connection["attributes"]["session.id"] == attributes["session.id"]
            for connection in events
        )
    assert isinstance(slow["attributes"]["duration_ms"], int)
    assert slow["attributes"]["outcome"] == "success"
    assert "query.plan" not in slow["attributes"]
    assert failed["attributes"]["phase"] == "pull"
    assert failed["attributes"]["error.type"]
    assert failed["attributes"]["outcome"] == "failure"


def test_failed_query_setting_still_gates_events():
    path = _log_path()
    offset = os.path.getsize(path)
    with closing(_connect()) as conn:
        session_id = _run(conn, "SET SESSION TRACE OFF")[0][0]
        with pytest.raises(mgclient.DatabaseError):
            _run(conn, "RETURN 1 / 0 /* structured_disabled_marker */")
    events = _wait_events(
        path,
        offset,
        lambda records: _closed(records, session_id),
    )
    assert not any(
        record["event"]["name"] == "query.failed"
        and "structured_disabled_marker" in record["attributes"]["db.query.text"]
        for record in events
    )


def test_parse_failure_has_no_invented_transaction_or_query_id():
    path = _log_path()
    offset = os.path.getsize(path)
    with closing(_connect()) as conn:
        session_id = _run(conn, "SET SESSION TRACE OFF")[0][0]
        _run(conn, 'SET SESSION SETTING "log.failed_queries" TO "true"')
        with pytest.raises(mgclient.DatabaseError):
            _run(conn, "THIS IS NOT CYPHER /* structured_parse_marker */")
    events = _wait_events(
        path,
        offset,
        lambda records: _closed(records, session_id),
    )
    failed = next(record for record in events if record["event"]["name"] == "query.failed")
    assert failed["attributes"]["phase"] == "parse"
    assert "transaction.id" not in failed["attributes"]
    assert "query.id" not in failed["attributes"]


def test_authentication_rejection_is_correlated_without_credentials():
    path = _log_path()
    offset = os.path.getsize(path)
    secret = "structured_logging_test_credential"
    auth = Auth(scheme="structured_logging_unsupported", principal="", credentials=secret)
    with GraphDatabase.driver("bolt://localhost:7687", auth=auth) as driver:
        with pytest.raises(Neo4jError) as error:
            driver.verify_connectivity()
    assert error.value.code.endswith("Security.Unauthenticated")
    events = _wait_events(
        path,
        offset,
        lambda records: any(
            record["event"]["name"] == "authentication.failed" and _closed(records, record["attributes"]["session.id"])
            for record in records
        ),
    )
    failed = next(record for record in events if record["event"]["name"] == "authentication.failed")
    assert failed["attributes"]["reason"] == "unauthenticated"
    assert failed["attributes"]["outcome"] == "failure"
    assert secret not in json.dumps(events)
    accepted = next(
        record
        for record in events
        if record["event"]["name"] == "connection.accepted"
        and record["attributes"]["session.id"] == failed["attributes"]["session.id"]
    )
    assert accepted["attributes"]["client.address"]
    assert isinstance(accepted["attributes"]["client.port"], int)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA", *sys.argv[1:]]))
