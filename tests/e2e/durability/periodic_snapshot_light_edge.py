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

import interactive_mg_runner
import periodic_snapshot as base
import pytest
from common import get_data_path, get_logs_path

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

FILE = "periodic_snapshot_light_edge"


@pytest.fixture(autouse=True)
def cleanup_after_test():
    yield
    interactive_mg_runner.kill_all(keep_directories=False)


@pytest.fixture
def test_name(request):
    return request.node.name


# Flags that turn on light edges. Light edges require properties-on-edges.
LIGHT_EDGE_FLAGS = [
    "--storage-properties-on-edges=true",
    "--storage-light-edge",
]


def memgraph_instances(test_name, mode="IN_MEMORY_TRANSACTIONAL"):
    # Start from the heavy workload's instance definitions, keyed under this
    # file's own data and log paths, and inject the light-edge flags into
    # every instance's arg list.
    instances = base.memgraph_instances(test_name, mode, file=FILE)
    for cfg in instances.values():
        cfg["args"] = cfg["args"] + LIGHT_EDGE_FLAGS
    return instances


def test_sec_flag_light_edge(test_name):
    interactive_mg_runner.start(memgraph_instances(test_name), "sec_flag")
    base.main_test(base.snapshots_path(FILE, test_name))
    interactive_mg_runner.kill_all(keep_directories=False)


def test_interval_flag_light_edge(test_name):
    interactive_mg_runner.start(memgraph_instances(test_name), "interval_flag")
    base.main_test(base.snapshots_path(FILE, test_name))
    interactive_mg_runner.kill_all(keep_directories=False)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
