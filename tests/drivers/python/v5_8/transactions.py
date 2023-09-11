#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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

import time

from neo4j import GraphDatabase, basic_auth
from neo4j.exceptions import ClientError, TransientError


def tx_error(tx, name, name2):
    a = tx.run("CREATE (a:Person {name: $name}) RETURN a", name=name).value()
    print(a[0])
    tx.run("CREATE (").consume()
    a = tx.run("CREATE (a:Person {name: $name}) RETURN a", name=name2).value()
    print(a[0])


def tx_good(tx, name, name2):
    a = tx.run("CREATE (a:Person {name: $name}) RETURN a", name=name).value()
    print(a[0])
    a = tx.run("CREATE (a:Person {name: $name}) RETURN a", name=name2).value()
    print(a[0])


def tx_too_long(tx):
    tx.run("MATCH (a), (b), (c), (d), (e), (f) RETURN COUNT(*) AS cnt")


def assert_timeout(set_timeout, measure_timeout):
    print(measure_timeout)
    print(set_timeout)
    assert (
        measure_timeout >= set_timeout and measure_timeout < set_timeout * 1.2
    ), "Wrong timeout; expected {}s and measured {}s".format(set_timeout, measure_timeout)


def get_timeout(tx):
    res = tx.run("SHOW DATABASE SETTINGS").values()
    for setting in res:
        if setting[0] == "query.timeout":
            return float(setting[1])
    assert False, "No setting named query.timeout"


def set_timeout(tx, timeout):
    tx.run("SET DATABASE SETTING 'query.timeout' TO '{}'".format(timeout)).consume()


def test_timeout(driver, set_timeout):
    # Query that will run for a very long time, transient error expected.
    timed_out = False
    try:
        with driver.session() as session:
            start_time = time.time()
            session.run("MATCH (a), (b), (c), (d), (e), (f) RETURN COUNT(*) AS cnt").consume()
    except TransientError:
        end_time = time.time()
        assert_timeout(set_timeout, end_time - start_time)
        timed_out = True

    if timed_out:
        print("The query timed out as was expected.")
    else:
        raise Exception("The query should have timed out, but it didn't!")


with GraphDatabase.driver("bolt://localhost:7687", auth=None, encrypted=False) as driver:

    def add_person(f, name, name2):
        with driver.session() as session:
            session.write_transaction(f, name, name2)

    # Wrong query.
    try:
        add_person(tx_error, "mirko", "slavko")
    except ClientError:
        pass
    # Correct query.
    add_person(tx_good, "mirka", "slavka")

    # Setup for next query.
    with driver.session() as session:
        session.run("UNWIND range(1, 100000) AS x CREATE ()").consume()

    # Test changing the timeout at run-time
    with driver.session() as session:
        default_timeout = get_timeout(session)
    test_timeout(driver, default_timeout)

    with driver.session() as session:
        set_timeout(session, 1)
    test_timeout(driver, 1)

    with driver.session() as session:
        set_timeout(session, default_timeout)

    print("All ok!")
