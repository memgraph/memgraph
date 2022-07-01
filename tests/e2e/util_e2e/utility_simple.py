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

import typing
import mgclient
import sys
import pytest


def test_does_throw_if_null_is_checked_against_in_generic_case():

    connection = mgclient.connect(host="localhost", port=7687)
    connection.autocommit = True

    cursor = connection.cursor()

    query = """WITH 2 AS name
                RETURN CASE name
                WHEN 3 THEN 'works'
                WHEN null THEN "doesn't work"
                ELSE 'something went wrong'
                END"""

    with pytest.raises(mgclient.DatabaseError) as err:
        cursor.execute(query)

    error_msg = err.value.args[0]
    assert error_msg == "Use the generic form when checking against NULL."


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
