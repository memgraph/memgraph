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
from threading import Barrier, Thread

import mgclient
import pytest


def execute_and_fetch_all(cursor: mgclient.Cursor, query: str, params: dict = {}) -> typing.List[tuple]:
    cursor.execute(query, params)
    return cursor.fetchall()


class SerializationFixture:
    def __init__(self):
        self.passes = 0
        self.fails = 0
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
        threads = []

        for tx in phase1_txs:
            thread = Thread(target=partial(self._execute_phase_1, tx["query"], tx.get("args", {}), tx.get("delay")))
            threads.append(thread)

        for tx in phase2_txs:
            thread = Thread(target=partial(self._execute_phase_2, tx["query"], tx.get("args", {})))
            threads.append(thread)

        self.barrier = Barrier(len(threads))

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        return (self.passes, self.fails)

    def _make_connection(self, **kwargs):
        connection = mgclient.connect(host="localhost", port=7687, **kwargs)
        connection.autocommit = False
        return connection

    def _execute_phase_1(self, query, args, sleep_duration):
        """Phase 1 transactions execute the query, hit a barrier, and then
        sleep for `wait` seconds
        """
        try:
            connection = self._make_connection()
            cursor = connection.cursor()
            cursor.execute(query, args)
            self.barrier.wait()
            if sleep_duration:
                time.sleep(sleep_duration)
            connection.commit()
            self.passes += 1
        except Exception as e:
            print(str(e))
            self.fails += 1

    def _execute_phase_2(self, query, args):
        """Phase 2 transactions hit the barrier, and the execute the query"""
        try:
            self.barrier.wait()
            connection = self._make_connection()
            cursor = connection.cursor()
            cursor.execute(query, args)
            connection.commit()
            self.passes += 1
        except Exception as e:
            print(str(e))
            self.fails += 1


@pytest.fixture
def serialization() -> SerializationFixture:
    return SerializationFixture()
