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

"""Light-edge variant of periodic_snapshot.py.

All tests are identical but Memgraph is started with --storage-light-edge
and --storage-properties-on-edges so the snapshot scheduler is exercised
with pool-allocated (light) edges.
"""

import sys

import periodic_snapshot as _ps

# Patch memgraph_instances before any test runs.
_orig_memgraph_instances = _ps.memgraph_instances

_LE_ARGS = ["--storage-light-edge", "--storage-properties-on-edges"]


def _light_edge_memgraph_instances(dir, mode="IN_MEMORY_TRANSACTIONAL"):
    instances = _orig_memgraph_instances(dir, mode)
    for cfg in instances.values():
        if _LE_ARGS[0] not in cfg["args"]:
            cfg["args"] = cfg["args"] + _LE_ARGS
    return instances


_ps.memgraph_instances = _light_edge_memgraph_instances

# Re-export so pytest collects fixtures and test functions.
from periodic_snapshot import *  # noqa: F401, F403, E402

if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main([__file__, "-rA"]))
