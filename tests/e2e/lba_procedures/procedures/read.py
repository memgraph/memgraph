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


@mgp.read_proc
def number_of_visible_nodes(ctx: mgp.ProcCtx) -> mgp.Record(nr_of_nodes=int):
    return mgp.Record(nr_of_nodes=len(mgp.Vertices(ctx.graph._graph)))


@mgp.read_proc
def number_of_visible_edges(ctx: mgp.ProcCtx) -> mgp.Record(nr_of_edges=int):
    count = 0
    for vertex in ctx.graph.vertices:
        for _ in vertex.out_edges:
            count += 1

    return mgp.Record(nr_of_edges=count)
