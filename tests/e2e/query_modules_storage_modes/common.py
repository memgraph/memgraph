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

import mgclient
import pytest


@pytest.fixture(scope="function")
def cursor(**kwargs) -> mgclient.Connection:
    connection = mgclient.connect(host="localhost", port=7687, **kwargs)
    connection.autocommit = True
    cursor = connection.cursor()

    cursor.execute("MATCH (n) DETACH DELETE n;")
    cursor.execute("CREATE (m:Component {id: 'A7422'}), (n:Component {id: '7X8X0'});")
    cursor.execute("MATCH (m:Component {id: 'A7422'}) MATCH (n:Component {id: '7X8X0'}) CREATE (m)-[:PART_OF]->(n);")
    cursor.execute("MATCH (m:Component {id: 'A7422'}) MATCH (n:Component {id: '7X8X0'}) CREATE (n)-[:DEPENDS_ON]->(m);")

    yield cursor

    cursor.execute("MATCH (n) DETACH DELETE n;")


def connect(**kwargs):
    connection = mgclient.connect(host="localhost", port=7687, **kwargs)
    connection.autocommit = True
    return connection.cursor()
