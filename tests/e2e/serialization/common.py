# Copyright 2025 Memgraph Ltd.
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
import typing
from functools import partial
from itertools import chain
from multiprocessing import Barrier, Lock, Manager, Process
from threading import Event

import mgclient
import pytest


def execute_and_fetch_all(cursor: mgclient.Cursor, query: str, params: dict = {}) -> typing.List[tuple]:
    cursor.execute(query, params)
    return cursor.fetchall()


def make_connection(**kwargs):
    connection = mgclient.connect(host="localhost", port=7687, **kwargs)
    connection.autocommit = False
    return connection


def execute_phase_1(query, args, barrier, shared_state, state_lock, should_abort=False):
    """Phase 1 transactions execute the query, hit barrier, sleep to ensure Phase 2 hits dependency, then commit"""
    try:
        connection = make_connection()
        cursor = connection.cursor()
        cursor.execute(query, args)
        barrier.wait()

        # Half-a-second wait here is to "guarantee" that the phase 2 threads
        # begin executing before we commit. In the unlikely event that it isn't
        # enough time, tests will still pass, but we just won't be testing
        # write/write conflicts this iteration because transaction will be
        # running on a serial schedule. Think of it as happy flakiness.
        time.sleep(0.5)

        if should_abort:
            connection.rollback()
        else:
            connection.commit()

        with state_lock:
            if should_abort:
                shared_state["fails"] += 1
            else:
                shared_state["passes"] += 1
    except Exception as e:
        with state_lock:
            shared_state["fails"] += 1


def execute_phase_2(query, args, barrier, shared_state, state_lock):
    """Phase 2 transactions hit barrier, then execute (will block on dependency)"""
    try:
        barrier.wait()
        connection = make_connection()
        cursor = connection.cursor()
        cursor.execute(query, args)
        connection.commit()
        with state_lock:
            shared_state["passes"] += 1
    except Exception as e:
        with state_lock:
            shared_state["fails"] += 1


class SerializationFixture:
    def __init__(self):
        self.manager = Manager()
        self.shared_state = self.manager.dict()
        self.shared_state["passes"] = 0
        self.shared_state["fails"] = 0
        self.state_lock = Lock()
        self.primary_connection = mgclient.connect(host="localhost", port=7687)
        self.primary_connection.autocommit = True

    def __del__(self):
        cursor = self.primary_connection.cursor()
        execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n")
        execute_and_fetch_all(cursor, "STORAGE MODE IN_MEMORY_TRANSACTIONAL;")

    def setup(self, query):
        cursor = self.primary_connection.cursor()
        execute_and_fetch_all(cursor, query)
        self.primary_connection.commit()

    def run(self, phase1_txs, phase2_txs):
        processes = []
        barrier = Barrier(len(phase1_txs) + len(phase2_txs))

        for tx in phase1_txs:
            should_abort = tx.get("abort", False)
            process = Process(
                target=execute_phase_1,
                args=(tx["query"], tx.get("args", {}), barrier, self.shared_state, self.state_lock, should_abort),
            )
            processes.append(process)

        for tx in phase2_txs:
            process = Process(
                target=execute_phase_2,
                args=(tx["query"], tx.get("args", {}), barrier, self.shared_state, self.state_lock),
            )
            processes.append(process)

        for process in processes:
            process.start()

        for process in processes:
            process.join()

        return (self.shared_state["passes"], self.shared_state["fails"])


@pytest.fixture
def serialization() -> SerializationFixture:
    return SerializationFixture()
