# Copyright 2021 Memgraph Ltd.
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

# import typing


@mgp.read_proc
def underlying_graph_is_mutable(ctx: mgp.ProcCtx, object: mgp.Any) -> mgp.Record(mutable=bool):
    return mgp.Record(mutable=object.underlying_graph_is_mutable())


@mgp.read_proc
def graph_is_mutable(ctx: mgp.ProcCtx) -> mgp.Record(mutable=bool):
    return mgp.Record(mutable=ctx.graph.is_mutable())


@mgp.read_proc
def number_of_visible_nodes(ctx: mgp.ProcCtx, object: mgp.Any) -> mgp.Record(mutable=bool):
    return mgp.Record(mutable=True)
