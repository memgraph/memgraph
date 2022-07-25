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
import mgclient
import pytest

import default_config


def test_does_default_config_match():
    connection = mgclient.connect(host="localhost", port=7687)
    connection.autocommit = True

    cursor = connection.cursor()
    cursor.execute("SHOW CONFIG")
    config = cursor.fetchall()

    assert len(config) == len(default_config.startup_config)

    for idx, flag in enumerate(config):
        assert len(flag) == len(default_config.startup_config[idx])

        current_flag = ()

        for flag in config:
            if flag[0] == default_config.startup_config[idx][0]:
                current_flag = flag
                break

        assert current_flag[1] == default_config.startup_config[idx][1]


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
