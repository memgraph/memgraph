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

import pytest
from common import connect, execute_and_fetch_all


def test_ssl_reload_without_ssl_configured():
    """
    When Memgraph is started without SSL, RELOAD SSL FOR BOLT_SERVER
    should return a meaningful error, not crash.
    """
    conn = connect()
    cursor = conn.cursor()

    with pytest.raises(Exception):
        execute_and_fetch_all(cursor, "RELOAD SSL FOR BOLT_SERVER;")

    # Server should still be functional after the failed reload
    result = execute_and_fetch_all(conn.cursor(), "RETURN 1 AS n")
    assert result == [(1,)], "Server should still work after reload on non-SSL instance"
    conn.close()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
