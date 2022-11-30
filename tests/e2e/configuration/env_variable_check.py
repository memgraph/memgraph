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

import os
import sys

import pytest

from memgraph import MemgraphInstanceRunner

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", ".."))
BUILD_DIR = os.path.join(PROJECT_DIR, "build")
MEMGRAPH_BINARY = os.path.join(BUILD_DIR, "memgraph")


def test_create_node_all_labels_granted():
    mg_instance = MemgraphInstanceRunner(MEMGRAPH_BINARY, False)

    binary_args = ["--bolt-port=7688"]

    mg_instance.start(args=binary_args)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
