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
def echo_point2d(ctx: mgp.ProcCtx, val: mgp.Point2d) -> mgp.Record(result=mgp.Point2d):
    return mgp.Record(result=val)


@mgp.read_proc
def echo_point3d(ctx: mgp.ProcCtx, val: mgp.Point3d) -> mgp.Record(result=mgp.Point3d):
    return mgp.Record(result=val)


@mgp.read_proc
def echo_zoned_date_time(ctx: mgp.ProcCtx, val: mgp.ZonedDateTime) -> mgp.Record(result=mgp.ZonedDateTime):
    return mgp.Record(result=val)
