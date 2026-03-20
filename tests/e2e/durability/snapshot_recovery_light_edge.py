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

"""Light-edge variant of snapshot_recovery.py.

All tests are identical but Memgraph is started with --storage-light-edge
and --storage-properties-on-edges so that snapshots and WALs are written
and recovered using pool-allocated (light) edges.
"""

import os
import shutil
import sys

import snapshot_recovery as _sr

# Patch memgraph_instances BEFORE generate_tmp_snapshot() runs so that both
# the writer and the recovery instance use light edges.
_orig_memgraph_instances = _sr.memgraph_instances

_LE_ARGS = ["--storage-light-edge", "--storage-properties-on-edges"]


def _light_edge_memgraph_instances(dir):
    instances = _orig_memgraph_instances(dir)
    for cfg in instances.values():
        if _LE_ARGS[0] not in cfg["args"]:
            cfg["args"] = cfg["args"] + _LE_ARGS
    return instances


_sr.memgraph_instances = _light_edge_memgraph_instances

# Re-export everything so pytest collects fixtures and test functions from here.
from snapshot_recovery import *  # noqa: F401, F403, E402

if __name__ == "__main__":
    import pytest

    try:
        shutil.rmtree(_sr.TMP_DIR)
        os.mkdir(_sr.TMP_DIR)
    except Exception:
        pass

    # generate_tmp_snapshot uses the now-patched memgraph_instances
    _sr.generate_tmp_snapshot()

    res = pytest.main([__file__, "-rA"])

    _sr.interactive_mg_runner.kill_all()
    try:
        shutil.rmtree(_sr.TMP_DIR)
    except Exception:
        pass

    sys.exit(res)
