# Copyright 2026 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file

import mgp


@mgp.read_proc
def count(ctx: mgp.ProcCtx, label: str) -> mgp.Record(count=int):
    return mgp.Record(count=sum(1 for v in ctx.graph.vertices if label in {l.name for l in v.labels}))


@mgp.write_proc
def create(ctx: mgp.ProcCtx, label: str) -> mgp.Record(created=bool):
    v = ctx.graph.create_vertex()
    v.add_label(label)
    return mgp.Record(created=True)
