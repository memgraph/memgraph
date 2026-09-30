# Copyright 2024 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

# Light-edge variant of periodic_snapshot.py. Reuses the heavy workload's test
# bodies but launches Memgraph with --storage-light-edge (which requires
# --storage-properties-on-edges=true). This proves periodic snapshotting works
# end-to-end for a light-edge storage instance.

import os
import sys
import tempfile

import interactive_mg_runner
import periodic_snapshot as base
import pytest

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

# Flags that turn on light edges. Light edges require properties-on-edges.
LIGHT_EDGE_FLAGS = [
    "--storage-properties-on-edges=true",
    "--storage-light-edge",
]


def memgraph_instances(dir, mode="IN_MEMORY_TRANSACTIONAL"):
    # Start from the heavy workload's instance definitions and inject the
    # light-edge flags into every instance's arg list.
    instances = base.memgraph_instances(dir, mode)
    for name, cfg in instances.items():
        cfg["args"] = cfg["args"] + LIGHT_EDGE_FLAGS
        cfg["log_file"] = "light_edge_" + cfg["log_file"]
    return instances


def test_sec_flag_light_edge():
    data_directory = tempfile.TemporaryDirectory()
    interactive_mg_runner.start(memgraph_instances(data_directory.name), "sec_flag")
    base.main_test(data_directory.name + "/snapshots")
    interactive_mg_runner.kill_all()


def test_interval_flag_light_edge():
    data_directory = tempfile.TemporaryDirectory()
    interactive_mg_runner.start(memgraph_instances(data_directory.name), "interval_flag")
    base.main_test(data_directory.name + "/snapshots")
    interactive_mg_runner.kill_all()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
