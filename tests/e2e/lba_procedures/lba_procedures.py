# Copyright 2022 Memgraph Ltd.
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
from common import execute_and_fetch_all


def test_lba_procedures_vertices_iterator_count_only_permitted_vertices(connection_from_username_and_password):
    cursor = next(connection_from_username_and_password(username="Josip", password="")).cursor()
    result = execute_and_fetch_all(cursor, "CALL read.number_of_visible_nodes() YIELD nr_of_nodes RETURN nr_of_nodes ;")

    assert result[0][0] == 10

    cursor = next(connection_from_username_and_password(username="Boris", password="")).cursor()
    result = execute_and_fetch_all(cursor, "CALL read.number_of_visible_nodes() YIELD nr_of_nodes RETURN nr_of_nodes ;")

    assert result[0][0] == 6


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
