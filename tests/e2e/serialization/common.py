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
from multiprocessing import Barrier, Process, Value

import mgclient
import pytest


def execute_and_fetch_all(cursor: mgclient.Cursor, query: str, params: dict = {}) -> typing.List[tuple]:
    cursor.execute(query, params)
    return cursor.fetchall()


class SerializationFixture:
    def __init__(self):
        self.passes = Value("i", 0)
        self.fails = Value("i", 0)
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
            process = Process(
                target=self._execute_phase_1, args=(tx["query"], tx.get("args", {}), tx.get("delay"), barrier)
            )
            processes.append(process)

        for tx in phase2_txs:
            process = Process(target=self._execute_phase_2, args=(tx["query"], tx.get("args", {}), barrier))
            processes.append(process)

        for process in processes:
            process.start()

        for process in processes:
            process.join()

        return (self.passes.value, self.fails.value)

    def _make_connection(self, **kwargs):
        connection = mgclient.connect(host="localhost", port=7687, **kwargs)
        connection.autocommit = False
        return connection

    def _execute_phase_1(self, query, args, sleep_duration, barrier):
        """Phase 1 transactions execute the query, hit a barrier, and then
        sleep for `wait` seconds
        """
        try:
            connection = self._make_connection()
            cursor = connection.cursor()
            cursor.execute(query, args)
            barrier.wait()
            if sleep_duration:
                time.sleep(sleep_duration)
            connection.commit()
            with self.passes.get_lock():
                self.passes.value += 1
        except Exception as e:
            print(str(e))
            with self.fails.get_lock():
                self.fails.value += 1

    def _execute_phase_2(self, query, args, barrier):
        """Phase 2 transactions hit the barrier, and the execute the query"""
        try:
            barrier.wait()
            connection = self._make_connection()
            cursor = connection.cursor()
            cursor.execute(query, args)
            connection.commit()
            with self.passes.get_lock():
                self.passes.value += 1
        except Exception as e:
            print(str(e))
            with self.fails.get_lock():
                self.fails.value += 1


@pytest.fixture
def serialization() -> SerializationFixture:
    return SerializationFixture()
