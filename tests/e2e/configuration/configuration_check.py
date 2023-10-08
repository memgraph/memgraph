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

import default_config
import mgclient
import pytest


def test_does_default_config_match():
    connection = mgclient.connect(host="localhost", port=7687)
    connection.autocommit = True

    cursor = connection.cursor()
    cursor.execute("SHOW CONFIG")
    config = cursor.fetchall()

    define_msg = """
        If this test fails after adding a new DEFINE_* flag,
        you should decide whether your new flag needs to be
        returned in the SHOW CONFIG command. If not, please
        use the DEFINE_HIDDEN_* macro instead of DEFINE_* to
        prevent SHOW CONFIG from returning it.
    """

    assert len(config) == len(default_config.startup_config_dict), define_msg

    for flag in config:
        flag_name = flag[0]

        # The default value of these is dependent on the given machine.
        machine_dependent_configurations = [
            "bolt_num_workers",
            "data_directory",
            "log_file",
            "storage_recovery_thread_count",
        ]
        if flag_name in machine_dependent_configurations:
            continue

        # default_value
        assert default_config.startup_config_dict[flag_name][0] == flag[1]
        # current_value
        assert default_config.startup_config_dict[flag_name][1] == flag[2]
        # description
        assert default_config.startup_config_dict[flag_name][2] == flag[3]


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
