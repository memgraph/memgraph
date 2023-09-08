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

from typing import Union

import mgp


@mgp.write_proc
def create_vertex(ctx: mgp.ProcCtx, id: mgp.Any) -> mgp.Record(v=mgp.Any):
    v = None
    try:
        v = ctx.graph.create_vertex()
        v.properties.set("id", id)
        v.properties.set("tbd", 0)
    except RuntimeError as e:
        return mgp.Record(v=str(e))
    return mgp.Record(v=v)


@mgp.write_proc
def delete_vertex(ctx: mgp.ProcCtx, v: mgp.Any) -> mgp.Record():
    ctx.graph.delete_vertex(v)
    return mgp.Record()


@mgp.write_proc
def detach_delete_vertex(ctx: mgp.ProcCtx, v: mgp.Any) -> mgp.Record():
    ctx.graph.detach_delete_vertex(v)
    return mgp.Record()


@mgp.write_proc
def create_edge(
    ctx: mgp.ProcCtx, from_vertex: mgp.Vertex, to_vertex: mgp.Vertex, edge_type: str
) -> mgp.Record(e=mgp.Any):
    e = None
    try:
        e = ctx.graph.create_edge(from_vertex, to_vertex, mgp.EdgeType(edge_type))
        e.properties.set("id", 1)
        e.properties.set("tbd", 0)
    except RuntimeError as ex:
        return mgp.Record(e=str(ex))
    return mgp.Record(e=e)


@mgp.write_proc
def delete_edge(ctx: mgp.ProcCtx, edge: mgp.Edge) -> mgp.Record():
    ctx.graph.delete_edge(edge)
    return mgp.Record()


@mgp.write_proc
def set_property(ctx: mgp.ProcCtx, object: mgp.Any) -> mgp.Record():
    object.properties.set("id", 2)
    return mgp.Record()


@mgp.write_proc
def remove_property(ctx: mgp.ProcCtx, object: mgp.Any) -> mgp.Record():
    object.properties.set("tbd", None)
    return mgp.Record()


@mgp.write_proc
def add_label(ctx: mgp.ProcCtx, object: mgp.Any, name: str) -> mgp.Record(o=mgp.Any):
    object.add_label(name)
    return mgp.Record(o=object)


@mgp.write_proc
def remove_label(ctx: mgp.ProcCtx, object: mgp.Any, name: str) -> mgp.Record(o=mgp.Any):
    object.remove_label(name)
    return mgp.Record(o=object)


@mgp.write_proc
def access_deleted(ctx: mgp.ProcCtx, event: mgp.Any) -> mgp.Record(status=bool):
    def process_obj(obj: Union[mgp.Vertex, mgp.Edge]) -> None:
        for label in obj.labels:
            assert label
        for prop in obj.properties:
            assert prop

    try:
        if "event_type" not in event:
            return False
        if event["event_type"] == "deleted_vertex":
            process_obj(event["vertex"])
        if event["event_type"] == "deleted_edge":
            e = event["edge"]
            process_obj(e)
            process_obj(e.from_vertex)
            process_obj(e.to_vertex)
    except Exception as e:
        raise e
    return mgp.Record(status=True)
