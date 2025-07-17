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
        self.error = None
        self.barrier = Barrier(2)
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

    def run(self, transactions):
        threads_with_wait = []
        threads_without_wait = []

        for tx in transactions:
            if "wait" in tx:
                thread = Thread(target=partial(self._execute_and_wait, tx["query"], tx.get("args", {}), tx["wait"]))
                threads_with_wait.append(thread)
            else:
                thread = Thread(target=partial(self._execute, tx["query"], tx.get("args", {})))
                threads_without_wait.append(thread)

        for thread in threads_with_wait:
            thread.start()
        self.barrier.wait()
        for thread in threads_without_wait:
            thread.start()

        for thread in chain(threads_with_wait, threads_without_wait):
            thread.join()

        if self.error:
            print(self.error)
        assert not self.error

    def _make_connection(self, **kwargs):
        connection = mgclient.connect(host="localhost", port=7687, **kwargs)
        connection.autocommit = True
        return connection

    def _execute_and_wait(self, query, args, wait):
        try:
            connection = self._make_connection()
            cursor = connection.cursor()
            cursor.execute(query, args)
            self.barrier.wait()
            if wait:
                time.sleep(wait)
            connection.commit()
        except Exception as e:
            self.error = e

    def _execute(self, query, args):
        try:
            connection = self._make_connection()
            cursor = connection.cursor()
            cursor.execute(query, args)
            connection.commit()
        except Exception as e:
            self.error = e


@pytest.fixture
def serialization() -> SerializationFixture:
    return SerializationFixture()
