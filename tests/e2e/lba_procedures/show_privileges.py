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
    "IMPERSONATE_USER",
    "PROFILE_RESTRICTION",
]


def test_lba_procedures_show_privileges_first_user():
    expected_assertions_josip = [
        ("CREATE, READ, UPDATE, DELETE ON ALL LABELS", "GRANT", "GLOBAL LABEL PERMISSION GRANTED TO USER"),
        ("CREATE, READ, UPDATE, DELETE ON ALL EDGE_TYPES", "GRANT", "GLOBAL EDGE_TYPE PERMISSION GRANTED TO USER"),
        ("READ ON NODES CONTAINING LABELS :Label1 MATCHING ANY", "GRANT", "LABEL PERMISSION GRANTED TO USER"),
        ("NOTHING ON NODES CONTAINING LABELS :Label2 MATCHING ANY", "DENY", "LABEL PERMISSION DENIED TO USER"),
        ("UPDATE ON NODES CONTAINING LABELS :Label3 MATCHING ANY", "GRANT", "LABEL PERMISSION GRANTED TO USER"),
        ("READ ON NODES CONTAINING LABELS :Label4 MATCHING ANY", "GRANT", "LABEL PERMISSION GRANTED TO USER"),
        ("CREATE, DELETE ON NODES CONTAINING LABELS :Label5 MATCHING ANY", "GRANT", "LABEL PERMISSION GRANTED TO USER"),
        ("UPDATE ON NODES CONTAINING LABELS :Label6 MATCHING ANY", "GRANT", "LABEL PERMISSION GRANTED TO USER"),
        ("NOTHING ON NODES CONTAINING LABELS :Label7 MATCHING ANY", "DENY", "LABEL PERMISSION DENIED TO USER"),
    ]

    cursor = connect(username="Josip", password="").cursor()
    result = execute_and_fetch_all(cursor, "SHOW PRIVILEGES FOR Josip ON MAIN;")

    fine_privilege_results = [res for res in result if res[0] not in BASIC_PRIVILEGES]

    assert len(fine_privilege_results) == len(expected_assertions_josip)
    assert set(expected_assertions_josip) == set(fine_privilege_results)


def test_lba_procedures_show_privileges_second_user():
    expected_assertions_boris = [
        ("AUTH", "GRANT", "GRANTED TO USER"),
        ("READ ON NODES CONTAINING LABELS :Label1 MATCHING ANY", "GRANT", "LABEL PERMISSION GRANTED TO USER"),
        ("NOTHING ON NODES CONTAINING LABELS :Label2 MATCHING ANY", "DENY", "LABEL PERMISSION DENIED TO USER"),
        ("UPDATE ON NODES CONTAINING LABELS :Label3 MATCHING ANY", "GRANT", "LABEL PERMISSION GRANTED TO USER"),
        ("READ ON NODES CONTAINING LABELS :Label4 MATCHING ANY", "GRANT", "LABEL PERMISSION GRANTED TO USER"),
        ("CREATE, DELETE ON NODES CONTAINING LABELS :Label5 MATCHING ANY", "GRANT", "LABEL PERMISSION GRANTED TO USER"),
        ("UPDATE ON NODES CONTAINING LABELS :Label6 MATCHING ANY", "GRANT", "LABEL PERMISSION GRANTED TO USER"),
        ("NOTHING ON NODES CONTAINING LABELS :Label7 MATCHING ANY", "DENY", "LABEL PERMISSION DENIED TO USER"),
    ]

    cursor = connect(username="Boris", password="").cursor()
    result = execute_and_fetch_all(cursor, "SHOW PRIVILEGES FOR Boris ON MAIN;")

    assert len(result) == len(expected_assertions_boris)
    assert set(result) == set(expected_assertions_boris)


def test_lba_procedures_show_privileges_third_user():
    expected_assertions_niko = [
        ("AUTH", "GRANT", "GRANTED TO USER"),
        ("CREATE, READ, DELETE ON ALL LABELS", "GRANT", "GLOBAL LABEL PERMISSION GRANTED TO USER"),
    ]

    cursor = connect(username="Niko", password="").cursor()
    result = execute_and_fetch_all(cursor, "SHOW PRIVILEGES FOR Niko ON MAIN;")

    assert len(result) == len(expected_assertions_niko)
    assert set(result) == set(expected_assertions_niko)


def test_lba_procedures_show_privileges_fourth_user():
    expected_assertions_bruno = [
        ("AUTH", "GRANT", "GRANTED TO USER"),
        ("UPDATE ON ALL LABELS", "GRANT", "GLOBAL LABEL PERMISSION GRANTED TO USER"),
    ]

    # TODO: Revisit behaviour of this test

    cursor = connect(username="Bruno", password="").cursor()
    result = execute_and_fetch_all(cursor, "SHOW PRIVILEGES FOR Bruno ON MAIN;")

    assert len(result) == len(expected_assertions_bruno)
    assert set(result) == set(expected_assertions_bruno)


def test_show_privileges_multiple_labels_matching_any():
    auth_cursor = connect(username="Boris", password="").cursor()

    execute_and_fetch_all(auth_cursor, "CREATE USER test_user;")
    execute_and_fetch_all(
        auth_cursor, "GRANT READ ON NODES CONTAINING LABELS :Person, :Employee MATCHING ANY TO test_user;"
    )
    execute_and_fetch_all(auth_cursor, "GRANT UPDATE ON EDGES CONTAINING TYPES :KNOWS, :MANAGES TO test_user;")

    results = execute_and_fetch_all(auth_cursor, "SHOW PRIVILEGES FOR test_user;")
    privilege_strings = [r[0] for r in results]

    assert "READ ON NODES CONTAINING LABELS :Employee, :Person MATCHING ANY" in privilege_strings
    assert "UPDATE ON EDGES CONTAINING TYPES :KNOWS, :MANAGES" in privilege_strings

    execute_and_fetch_all(auth_cursor, "DROP USER test_user;")


def test_show_privileges_multiple_labels_matching_exactly():
    auth_cursor = connect(username="Niko", password="").cursor()

    execute_and_fetch_all(auth_cursor, "CREATE USER test_user2;")
    execute_and_fetch_all(
        auth_cursor, "GRANT CREATE ON NODES CONTAINING LABELS :Admin, :SuperUser MATCHING EXACTLY TO test_user2;"
    )

    results = execute_and_fetch_all(auth_cursor, "SHOW PRIVILEGES FOR test_user2;")
    privilege_strings = [r[0] for r in results]

    assert "CREATE ON NODES CONTAINING LABELS :Admin, :SuperUser MATCHING EXACTLY" in privilege_strings

    execute_and_fetch_all(auth_cursor, "DROP USER test_user2;")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
