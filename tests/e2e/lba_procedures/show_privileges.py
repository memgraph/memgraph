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
from common import connect, execute_and_fetch_all

BASIC_PRIVILEGES = [
    "CREATE",
    "DELETE",
    "MATCH",
    "MERGE",
    "SET",
    "REMOVE",
    "INDEX",
    "STATS",
    "AUTH",
    "REPLICATION",
    "READ_FILE",
    "DURABILITY",
    "FREE_MEMORY",
    "TRIGGER",
    "STREAM",
    "CONFIG",
    "CONSTRAINT",
    "DUMP",
    "MODULE_READ",
    "WEBSOCKET",
    "MODULE_WRITE",
    "TRANSACTION_MANAGEMENT",
    "STORAGE_MODE",
    "MULTI_DATABASE_EDIT",
    "MULTI_DATABASE_USE",
    "COORDINATOR",
]


def test_lba_procedures_show_privileges_first_user():
    expected_assertions_josip = [
        ("ALL LABELS", "CREATE_DELETE", "GLOBAL LABEL PERMISSION GRANTED TO USER"),
        (
            "ALL EDGE_TYPES",
            "CREATE_DELETE",
            "GLOBAL EDGE_TYPE PERMISSION GRANTED TO USER",
        ),
        ("LABEL :Label1", "READ", "LABEL PERMISSION GRANTED TO USER"),
        ("LABEL :Label2", "NOTHING", "LABEL PERMISSION DENIED TO USER"),
        ("LABEL :Label3", "UPDATE", "LABEL PERMISSION GRANTED TO USER"),
        ("LABEL :Label4", "READ", "LABEL PERMISSION GRANTED TO USER"),
        ("LABEL :Label5", "CREATE_DELETE", "LABEL PERMISSION GRANTED TO USER"),
        ("LABEL :Label6", "UPDATE", "LABEL PERMISSION GRANTED TO USER"),
        ("LABEL :Label7", "NOTHING", "LABEL PERMISSION DENIED TO USER"),
    ]

    cursor = connect(username="Josip", password="").cursor()
    result = execute_and_fetch_all(cursor, "SHOW PRIVILEGES FOR Josip;")

    assert len(result) == 35

    fine_privilege_results = [res for res in result if res[0] not in BASIC_PRIVILEGES]

    assert len(fine_privilege_results) == len(expected_assertions_josip)
    assert set(expected_assertions_josip) == set(fine_privilege_results)


def test_lba_procedures_show_privileges_second_user():
    expected_assertions_boris = [
        ("AUTH", "GRANT", "GRANTED TO USER"),
        ("LABEL :Label1", "READ", "LABEL PERMISSION GRANTED TO USER"),
        ("LABEL :Label2", "NOTHING", "LABEL PERMISSION DENIED TO USER"),
        ("LABEL :Label3", "UPDATE", "LABEL PERMISSION GRANTED TO USER"),
        ("LABEL :Label4", "READ", "LABEL PERMISSION GRANTED TO USER"),
        ("LABEL :Label5", "CREATE_DELETE", "LABEL PERMISSION GRANTED TO USER"),
        ("LABEL :Label6", "UPDATE", "LABEL PERMISSION GRANTED TO USER"),
        ("LABEL :Label7", "NOTHING", "LABEL PERMISSION DENIED TO USER"),
    ]

    cursor = connect(username="Boris", password="").cursor()
    result = execute_and_fetch_all(cursor, "SHOW PRIVILEGES FOR Boris;")

    assert len(result) == len(expected_assertions_boris)
    assert set(result) == set(expected_assertions_boris)


def test_lba_procedures_show_privileges_third_user():
    expected_assertions_niko = [
        ("AUTH", "GRANT", "GRANTED TO USER"),
        ("ALL LABELS", "READ", "GLOBAL LABEL PERMISSION GRANTED TO USER"),
    ]

    cursor = connect(username="Niko", password="").cursor()
    result = execute_and_fetch_all(cursor, "SHOW PRIVILEGES FOR Niko;")

    assert len(result) == len(expected_assertions_niko)
    assert set(result) == set(expected_assertions_niko)


def test_lba_procedures_show_privileges_fourth_user():
    expected_assertions_bruno = [
        ("AUTH", "GRANT", "GRANTED TO USER"),
        ("ALL LABELS", "UPDATE", "GLOBAL LABEL PERMISSION GRANTED TO USER"),
    ]

    # TODO: Revisit behaviour of this test

    cursor = connect(username="Bruno", password="").cursor()
    result = execute_and_fetch_all(cursor, "SHOW PRIVILEGES FOR Bruno;")

    assert len(result) == len(expected_assertions_bruno)
    assert set(result) == set(expected_assertions_bruno)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
