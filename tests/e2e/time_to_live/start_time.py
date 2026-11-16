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
from common import connect


def test_ttl_start_time_fraction(connect):
    # TTL start time goes through the local-time parser, so 1-9 digit fractions must keep their microsecond value.
    memgraph = connect.cursor()
    for start_time in ("03:04:05.5", "03:04:05.123456"):
        try:
            memgraph.execute(f'ENABLE TTL EVERY "1s" AT "{start_time}"')
        except mgclient.DatabaseError as e:
            assert False, f'ENABLE TTL AT "{start_time}" was rejected: {e}'

    memgraph.execute("DISABLE TTL")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
