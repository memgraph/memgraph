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

import mgp


@mgp.read_proc
def point2d_to_string(ctx: mgp.ProcCtx, val: mgp.Point2d) -> mgp.Record(result=str):
    return mgp.Record(result=repr(val))


@mgp.read_proc
def point3d_to_string(ctx: mgp.ProcCtx, val: mgp.Point3d) -> mgp.Record(result=str):
    return mgp.Record(result=repr(val))


@mgp.read_proc
def enum_to_string(ctx: mgp.ProcCtx, val: mgp.Enum) -> mgp.Record(result=str):
    return mgp.Record(result=repr(val))
