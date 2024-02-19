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

import sys

import pytest
from common import get_bytes, memgraph


def test_property_size_on_null_prop(memgraph):
    memgraph.execute(
        """
        CREATE (n:Node)
        SET n.null_prop = null;
        """
    )

    null_bytes = get_bytes(memgraph, "null_prop")

    # No property stored, no bytes allocated
    assert null_bytes == 0


def test_property_size_on_bool_prop(memgraph):
    memgraph.execute(
        """
        CREATE (n:Node)
        SET n.bool_prop = True;
        """
    )

    bool_bytes = get_bytes(memgraph, "bool_prop")

    # 1 byte metadata, 1 byte prop id, but value is encoded in the metadata
    assert bool_bytes == 2


def test_property_size_on_one_byte_int_prop(memgraph):
    memgraph.execute(
        """
        CREATE (n:Node)
        SET n.S_int_prop = 4;
        """
    )

    s_int_bytes = get_bytes(memgraph, "S_int_prop")

    # 1 byte metadata, 1 byte prop id + payload size 1 byte to store the int
    assert s_int_bytes == 3


def test_property_size_on_two_byte_int_prop(memgraph):
    memgraph.execute(
        """
        CREATE (n:Node)
        SET n.M_int_prop = 500;
        """
    )

    m_int_bytes = get_bytes(memgraph, "M_int_prop")

    # 1 byte metadata, 1 byte prop id + payload size 2 bytes to store the int
    assert m_int_bytes == 4


def test_property_size_on_four_byte_int_prop(memgraph):
    memgraph.execute(
        """
        CREATE (n:Node)
        SET n.L_int_prop = 1000000000;
        """
    )

    l_int_bytes = get_bytes(memgraph, "L_int_prop")

    # 1 byte metadata, 1 byte prop id + payload size 4 bytes to store the int
    assert l_int_bytes == 6


def test_property_size_on_eight_byte_int_prop(memgraph):
    memgraph.execute(
        """
        CREATE (n:Node)
        SET n.XL_int_prop = 1000000000000;
        """
    )

    xl_int_bytes = get_bytes(memgraph, "XL_int_prop")

    # 1 byte metadata, 1 byte prop id + payload size 8 bytes to store the int
    assert xl_int_bytes == 10


def test_property_size_on_float_prop(memgraph):
    memgraph.execute(
        """
        CREATE (n:Node)
        SET n.float_prop = 4.0;
        """
    )

    float_bytes = get_bytes(memgraph, "float_prop")

    # 1 byte metadata, 1 byte prop id + payload size 8 bytes to store the float
    assert float_bytes == 10


def test_property_size_on_string_prop(memgraph):
    memgraph.execute(
        """
        CREATE (n:Node)
        SET n.str_prop = 'str_value';
        """
    )

    str_bytes = get_bytes(memgraph, "str_prop")

    # 1 byte metadata
    # 1 byte prop id
    #   - the payload size contains the amount of bytes stored for the size in the next sequence
    # X bytes for the length of the string (1, 2, 4 or 8 bytes) -> "str_value" has 1 byte for the length of 9
    # Y bytes for the string content -> 9 bytes for "str_value"
    assert str_bytes == 12


def test_property_size_on_list_prop(memgraph):
    memgraph.execute(
        """
        CREATE (n:Node)
        SET n.list_prop = [1, 2, 3];
        """
    )

    list_bytes = get_bytes(memgraph, "list_prop")

    # 1 byte metadata
    # 1 byte prop id
    #   - the payload size contains the amount of bytes stored for the size of the list
    # X bytes for the size of the list (1, 2, 4 or 8 bytes)
    # for each list element:
    #   - 1 byte for the metadata
    #   - the amount of bytes for the payload of the type (a small int is 1 additional byte)
    # in this case 1 + 1 + 3 * (1 + 1)
    assert list_bytes == 9


def test_property_size_on_map_prop(memgraph):
    memgraph.execute(
        """
        CREATE (n:Node)
        SET  n.map_prop = {key1: 'value', key2: 4};
        """
    )

    map_bytes = get_bytes(memgraph, "map_prop")

    # 1 byte metadata
    # 1 byte prop id
    #   - the payload size contains the amount of bytes stored for the size of the map
    # X bytes for the size of the map (1, 2, 4 or 8 bytes - in this case 1)
    # for every map element:
    #   - 1 byte for metadata
    #   - 1, 2, 4 or 8 bytes for the key length (read from the metadata payload) -> this case 1
    #   - Y bytes for the key content -> this case 4
    #   - Z amount of bytes for the type
    #       - for 'value' -> 1 byte for size and 5 for length
    #       - for 4 -> 1 byte for content read from payload
    # total: 1 + 1 + (1 + 1 + 4 + (1 + 5)) + (1 + 1 + 4 + (1))
    assert map_bytes == 22


def test_property_size_on_date_prop(memgraph):
    memgraph.execute(
        """
        CREATE (n:Node)
        SET n.date_prop = date('2023-01-01');
        """
    )

    date_bytes = get_bytes(memgraph, "date_prop")

    # 1 byte metadata (to see that it's temporal data)
    # 1 byte prop id
    # 1 byte metadata
    #   - type is again the same
    #   - id field contains the length of the specific temporal type (1, 2, 4 or 8 bytes) -> probably always 1
    #   - payload field contains the length of the microseconds (1, 2, 4, or 8 bytes) -> probably always 8
    assert date_bytes == 12


def test_property_size_on_local_time_prop(memgraph):
    memgraph.execute(
        """
        CREATE (n:Node)
        SET n.localtime_prop = localtime('23:00:00');
        """
    )

    local_time_bytes = get_bytes(memgraph, "localtime_prop")

    # 1 byte metadata (to see that it's temporal data)
    # 1 byte prop id
    # 1 byte metadata
    #   - type is again the same
    #   - id field contains the length of the specific temporal type (1, 2, 4 or 8 bytes) -> probably always 1
    #   - payload field contains the length of the microseconds (1, 2, 4, or 8 bytes) -> probably always 8
    assert local_time_bytes == 12


def test_property_size_on_local_date_time_prop(memgraph):
    memgraph.execute(
        """
        CREATE (n:Node)
        SET n.localdatetime_prop = localdatetime('2022-01-01T23:00:00');
        """
    )

    local_date_time_bytes = get_bytes(memgraph, "localdatetime_prop")

    # 1 byte metadata (to see that it's temporal data)
    # 1 byte prop id
    # 1 byte metadata
    #   - type is again the same
    #   - id field contains the length of the specific temporal type (1, 2, 4 or 8 bytes) -> probably always 1
    #   - payload field contains the length of the microseconds (1, 2, 4, or 8 bytes) -> probably always 8
    assert local_date_time_bytes == 12


def test_property_size_on_duration_prop(memgraph):
    memgraph.execute(
        """
        CREATE (n:Node)
        SET n.duration_prop = duration('P5DT2M2.33S');
        """
    )

    duration_bytes = get_bytes(memgraph, "duration_prop")

    # 1 byte metadata (to see that it's temporal data)
    # 1 byte prop id
    # 1 byte metadata
    #   - type is again the same
    #   - id field contains the length of the specific temporal type (1, 2, 4 or 8 bytes) -> probably always 1
    #   - payload field contains the length of the microseconds (1, 2, 4, or 8 bytes) -> probably always 8
    assert duration_bytes == 12


def test_property_size_on_nonexistent_prop(memgraph):
    memgraph.execute(
        """
        CREATE (n:Node);
        """
    )

    nonexistent_bytes = get_bytes(memgraph, "nonexistent_prop")

    assert nonexistent_bytes is None


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
