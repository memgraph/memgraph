# Copyright 2023 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

from time import sleep

import mgp


@mgp.read_proc
def pass_node_with_id(ctx: mgp.ProcCtx, node: mgp.Vertex) -> mgp.Record(node=mgp.Vertex, id=int):
    sleep(1)
    return mgp.Record(node=node, id=node.id)


@mgp.function
def pass_node(ctx: mgp.FuncCtx, node: mgp.Vertex):
    sleep(1)
    return node


@mgp.function
def pass_relationship(ctx: mgp.FuncCtx, relationship: mgp.Edge):
    sleep(1)
    return relationship


@mgp.function
def pass_path(ctx: mgp.FuncCtx, path: mgp.Path):
    sleep(1)
    return path


@mgp.function
def pass_list(ctx: mgp.FuncCtx, list_: mgp.List[mgp.Any]):
    sleep(1)
    return list_


@mgp.function
def pass_map(ctx: mgp.FuncCtx, map_: mgp.Map):
    sleep(1)
    return map_
