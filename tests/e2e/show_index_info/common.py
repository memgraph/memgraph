# Copyright 2023 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import typing
from enum import IntEnum

import mgclient
import pytest


class Row(IntEnum):
    INDEX_TYPE = 0
    LABEL = 1
    PROPERTY = 2


@pytest.fixture(scope="module")
def cursor(**kwargs) -> mgclient.Connection:
    connection = mgclient.connect(host="localhost", port=7687, **kwargs)
    connection.autocommit = True
    cursor = connection.cursor()

    cursor.execute("CREATE INDEX ON :Gene;")
    cursor.execute("CREATE INDEX ON :Gene(id);")
    cursor.execute("CREATE INDEX ON :Gene(i5);")
    cursor.execute("CREATE INDEX ON :Compound;")
    cursor.execute("CREATE INDEX ON :Compound(id);")
    cursor.execute("CREATE INDEX ON :Compound(mgid);")
    cursor.execute("CREATE INDEX ON :Compound(inchikey);")
    cursor.execute("CREATE INDEX ON :Anatomy;")
    cursor.execute("CREATE INDEX ON :Disease;")

    yield cursor

    cursor.execute("DROP INDEX ON :Gene;")
    cursor.execute("DROP INDEX ON :Gene(id);")
    cursor.execute("DROP INDEX ON :Gene(i5);")
    cursor.execute("DROP INDEX ON :Compound;")
    cursor.execute("DROP INDEX ON :Compound(id);")
    cursor.execute("DROP INDEX ON :Compound(mgid);")
    cursor.execute("DROP INDEX ON :Compound(inchikey);")
    cursor.execute("DROP INDEX ON :Anatomy;")
    cursor.execute("DROP INDEX ON :Disease;")


def execute_and_fetch_all(cursor: mgclient.Cursor, query: str, params: dict = dict()) -> typing.List[tuple]:
    cursor.execute(query, params)
    return cursor.fetchall()
