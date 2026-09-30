# Copyright 2025 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file licenses/APL.txt.

"""
Query module that provides mgp.function(s) returning vector index config maps.
Use with: CREATE VECTOR INDEX ... WITH CONFIG vector_index_config.default_config();
"""

import mgp


@mgp.function
def default_config() -> mgp.Map:
    """Returns a default vector index config map (dimension=128, capacity=1000)."""
    return {
        "dimension": 128,
        "capacity": 1000,
    }


@mgp.function
def config(dimension: int, capacity: int, metric: str = "l2sq", scalar_kind: str = "f32") -> mgp.Map:
    """Returns a vector index config map with the given parameters."""
    return {
        "dimension": dimension,
        "capacity": capacity,
        "metric": metric,
        "scalar_kind": scalar_kind,
    }
