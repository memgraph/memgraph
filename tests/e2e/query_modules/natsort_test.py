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

import sys

import mgclient
import pytest
from common import connect, execute_and_fetch_all


def key(value):
    cursor = connect().cursor()
    return execute_and_fetch_all(cursor, "RETURN natsort.key($value) AS key;", {"value": value})[0][0]


def test_key_compares_digit_runs_as_numbers():
    assert key("T2") < key("T10")
    assert key("T9 Stage 10") < key("T10 Stage 2")
    assert key("x99999999999999999999") < key("x100000000000000000000")
    assert key("x1234567890123456789012345") < key("x12345678901234567890123456")
    # Leading zeros do not change the value.
    assert key("T01") == key("T1")
    assert key("x0") == key("x000")
    assert key("x0") < key("x1")


def test_key_ignores_case_and_folds_latin_letters():
    assert key("Temp") == key("temp")
    assert key("Café") == key("cafe")
    assert key("LÍNEA") == key("linea")
    assert key("Straße") == key("strasse")
    assert key("Ærø") == key("aero")
    assert key("Ĳssel") == key("ijssel")
    assert key("Ǆemal") == key("dzemal")
    assert key("Ǣsir") == key("aesir")
    assert key("Ǿre") == key("ore")


def test_key_ranks_marks_before_digits_and_letters():
    assert key("T 1") < key("T_1") < key("T-1") < key("T.1") < key("T1") < key("TA")
    assert key("a\tb") < key("a b")
    assert key("a-b") < key("a–b")
    # A mark that ICU expands compares as its parts.
    assert key("a…b") == key("a...b")
    assert key("a…a") < key("a.b")
    assert key("Stop‼") == key("Stop!!")


def test_key_skips_ignorable_characters():
    assert key("a\x01b") == key("ab")
    assert key("Café") == key("Café")
    assert key("co­op") == key("coop")
    # ICU ignores a middle dot right after "l".
    assert key("col·lega") == key("collega")
    assert key("a·b") != key("ab")


def test_key_of_null_and_empty_string():
    assert key(None) is None
    assert key("") == ""


def test_key_rejects_a_non_string():
    cursor = connect().cursor()
    with pytest.raises(mgclient.DatabaseError):
        execute_and_fetch_all(cursor, "RETURN natsort.key(1) AS key;")


def test_order_by_key_matches_icu_numeric_base_collation():
    # The order Intl.Collator(undefined, {numeric: true, sensitivity: "base"})
    # gives in Node 20 (ICU 77.1, en-US), exact ties broken by the raw string,
    # which is how a client that sorts with it orders these names.
    expected = [
        "",
        "\u00bfQue?",
        "\u201cquoted\u201d",
        "(spare)",
        "[old]",
        "50 C",
        "50\u00b0C",
        "a\tb",
        "a b",
        "a\u00a0b",
        "a-b",
        "a\u2013b",
        "a\u2026a",
        "a...b",
        "a\u2026b",
        "a.b",
        "A&B",
        "A#B",
        "a\x01b",
        "ab",
        "aero 1",
        "\u00c6r\u00f8 1",
        "Aesir",
        "\u01e2sir",
        "Angstrom",
        "\u00c5ngstr\u00f6m",
        "back\\slash 1",
        "Cafe\u0301",
        "Caf\u00e9",
        "cafe",
        "colla",
        "collega",
        "col\u00b7lega",
        "Dura",
        "\u00d0ura",
        "Dzemal",
        "\u01c4emal",
        "IJssel 2",
        "\u0132ssel 10",
        "Linea 1",
        "L\u00ednea 2",
        "L\u00cdNEA 3",
        "Lodz",
        "\u0141\u00f3d\u017a",
        "Lote",
        "Nandu",
        "\u00d1and\u00fa",
        "O'Brien 2",
        "Ore",
        "\u01fere",
        "Pump 9 Stage 10",
        "Pump 10 Stage 2",
        "Pump 10 Stage 10",
        "Stop!!",
        "Stop\u203c",
        "Strasse",
        "Stra\u00dfe",
        "T",
        "T 1",
        "T_1",
        "T-1",
        "T.1",
        "T01",
        "T1",
        "t1",
        "TA",
        "WTG9",
        "WTG10",
        "WTG12.Gen_345-TT7",
        "WTG12.Gen_345-TT10",
        "x0",
        "x00",
        "x000001",
        "x1",
        "x12345678901234567890",
        "x99999999999999999999",
        "x0100000000000000000000",
        "x100000000000000000000",
    ]
    cursor = connect().cursor()
    query = "UNWIND $names AS name RETURN name ORDER BY natsort.key(name), name;"
    rows = execute_and_fetch_all(cursor, query, {"names": expected[::-1]})
    assert [row[0] for row in rows] == expected


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
