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

import sys

import mgclient
import pytest
from common import connect, execute_and_fetch_all


def test_show_triggers_empty(connect):
    """Test SHOW TRIGGERS when no triggers exist."""
    cursor = connect.cursor()
    result = execute_and_fetch_all(cursor, "SHOW TRIGGERS")
    assert len(result) == 0


def test_show_triggers_output_format(connect):
    """Test that SHOW TRIGGERS returns correct output format with all columns."""
    cursor = connect.cursor()

    execute_and_fetch_all(
        cursor,
        """CREATE TRIGGER trigger_invoker
        SECURITY INVOKER
        ON CREATE BEFORE COMMIT EXECUTE
        UNWIND createdVertices AS node SET node.triggered = true""",
    )

    execute_and_fetch_all(
        cursor,
        """CREATE TRIGGER trigger_definer
        SECURITY DEFINER
        ON UPDATE AFTER COMMIT EXECUTE
        UNWIND updatedVertices AS node SET node.updated = true""",
    )

    execute_and_fetch_all(
        cursor,
        """CREATE TRIGGER trigger_default
        ON DELETE BEFORE COMMIT EXECUTE
        UNWIND deletedVertices AS node SET node.deleted = true""",
    )

    result = execute_and_fetch_all(cursor, "SHOW TRIGGERS")
    assert len(result) == 3

    # Expected columns: "trigger name", "statement", "privilege context", "event type", "phase", "owner"
    expected_columns = ["trigger name", "statement", "privilege context", "event type", "phase", "owner"]

    # Convert results to dictionary for easier access
    triggers = {}
    for row in result:
        # Each row should be a tuple with 6 elements
        assert len(row) == 6
        trigger_name = row[0]
        triggers[trigger_name] = {
            "trigger name": row[0],
            "statement": row[1],
            "privilege context": row[2],
            "event type": row[3],
            "phase": row[4],
            "owner": row[5],
        }

    assert "trigger_invoker" in triggers
    invoker = triggers["trigger_invoker"]
    assert invoker["trigger name"] == "trigger_invoker"
    assert "createdVertices" in invoker["statement"]
    assert invoker["privilege context"] == "INVOKER"
    assert invoker["event type"] == "CREATE"
    assert invoker["phase"] == "BEFORE COMMIT"
    assert invoker["owner"] is None

    assert "trigger_definer" in triggers
    definer = triggers["trigger_definer"]
    assert definer["trigger name"] == "trigger_definer"
    assert "updatedVertices" in definer["statement"]
    assert definer["privilege context"] == "DEFINER"
    assert definer["event type"] == "UPDATE"
    assert definer["phase"] == "AFTER COMMIT"
    assert definer["owner"] is None

    assert "trigger_default" in triggers
    default = triggers["trigger_default"]
    assert default["trigger name"] == "trigger_default"
    assert "deletedVertices" in default["statement"]
    assert default["privilege context"] == "DEFINER"  # default is DEFINER
    assert default["event type"] == "DELETE"
    assert default["phase"] == "BEFORE COMMIT"
    assert default["owner"] is None


def test_show_triggers_all_event_types(connect):
    """Test SHOW TRIGGERS with different event types."""
    cursor = connect.cursor()

    execute_and_fetch_all(
        cursor,
        """CREATE TRIGGER trigger_vertex_create
        ON () CREATE BEFORE COMMIT EXECUTE
        CREATE (:Test)""",
    )

    execute_and_fetch_all(
        cursor,
        """CREATE TRIGGER trigger_edge_create
        ON --> CREATE AFTER COMMIT EXECUTE
        CREATE (:Test)""",
    )

    execute_and_fetch_all(
        cursor,
        """CREATE TRIGGER trigger_any
        BEFORE COMMIT EXECUTE
        CREATE (:Test)""",
    )

    result = execute_and_fetch_all(cursor, "SHOW TRIGGERS")
    assert len(result) == 3

    triggers = {row[0]: row for row in result}

    assert triggers["trigger_vertex_create"][3] == "() CREATE"
    assert triggers["trigger_edge_create"][3] == "--> CREATE"
    assert triggers["trigger_any"][3] == "ANY"


def test_show_triggers_after_drop(connect):
    """Test SHOW TRIGGERS after dropping triggers."""
    cursor = connect.cursor()

    execute_and_fetch_all(
        cursor,
        """CREATE TRIGGER test_trigger
        ON CREATE BEFORE COMMIT EXECUTE
        CREATE (:Test)""",
    )

    result = execute_and_fetch_all(cursor, "SHOW TRIGGERS")
    assert len(result) == 1
    assert result[0][0] == "test_trigger"

    execute_and_fetch_all(cursor, "DROP TRIGGER test_trigger")

    result = execute_and_fetch_all(cursor, "SHOW TRIGGERS")
    assert len(result) == 0


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
