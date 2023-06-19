import random

import mgp


@mgp.read_proc
def dummy_read(
    context: mgp.ProcCtx,
) -> mgp.Record(total_vertices=int, total_edges=int):
    vertices = context.graph.vertices
    total_edges = 0
    for vertex in vertices:
        for edge in vertex.out_edges:
            total_edges += 1
    return mgp.Record(total_vertices=len(vertices), total_edges=total_edges)


@mgp.write_proc
def dummy_write(
    context: mgp.ProcCtx,
) -> mgp.Record(vertex=mgp.Vertex):
    for i in range(0, 10):
        node = context.graph.create_vertex()
        node.add_label("Procedure_node")
        node.properties.set("id", random.randint(0, 100000))

    return mgp.Record(vertex=node)
