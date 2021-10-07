# Build Memgraph with Python 3.5+ support and run Memgraph so that it loads
# this file as a Query Module. The procedure implemented in this module is just
# a rewrite of the `example.c` procedure.
import mgp

import copy


@mgp.read_proc
def procedure(context: mgp.ProcCtx,
              required_arg: mgp.Nullable[mgp.Any],
              optional_arg: mgp.Nullable[mgp.Any] = None
              ) -> mgp.Record(args=list,
                              vertex_count=int,
                              avg_degree=mgp.Number,
                              props=mgp.Nullable[mgp.Map]):
    """
    This example procedure returns 4 fields.

      * `args` is a copy of arguments passed to the procedure.
      * `vertex_count` is the number of vertices in the database.
      * `avg_degree` is the average degree of vertices.
      * `props` is the properties map of the passed in `required_arg`, if it is
        an Edge or a Vertex. In case of a Path instance, properties of the
        starting vertex are returned.

    Any errors can be reported by raising an Exception.

    The procedure can be invoked in openCypher using the following calls:
      CALL example.procedure(1, 2) YIELD args, vertex_count;
      MATCH (n) CALL example.procedure(n, 1) YIELD * RETURN *;

    Naturally, you may pass in different arguments or yield different fields.
    """
    # Create a properties map if we received an Edge, Vertex, or Path instance.
    props = None
    if isinstance(required_arg, (mgp.Edge, mgp.Vertex)):
        props = dict(required_arg.properties.items())
    elif isinstance(required_arg, mgp.Path):
        start_vertex, = required_arg.vertices
        props = dict(start_vertex.properties.items())
    # Count the vertices and edges in the database; this may take a while.
    vertex_count = 0
    edge_count = 0
    for v in context.graph.vertices:
        vertex_count += 1
        edge_count += sum(1 for e in v.in_edges)
        edge_count += sum(1 for e in v.out_edges)
    # Calculate the average degree, as if edges are not directed.
    avg_degree = 0 if vertex_count == 0 else edge_count / vertex_count
    # Copy the received arguments to make it equivalent to the C example.
    args_copy = [copy.deepcopy(required_arg), copy.deepcopy(optional_arg)]
    # Multiple rows can be produced by returning an iterable of mgp.Record.
    return mgp.Record(args=args_copy, vertex_count=vertex_count,
                      avg_degree=avg_degree, props=props)


@mgp.write_proc
def write_procedure(context: mgp.ProcCtx,
                    property_name: str,
                    property_value: mgp.Nullable[mgp.Any]
                    ) -> mgp.Record(created_vertex=mgp.Vertex):
    """
    This example procedure creates a new vertex with the specified property
    and connects it to all existing vertex which has the same property with
    the same name. It returns one field called `created_vertex` which
    contains the newly created vertex.

    Any errors can be reported by raising an Exception.

    The procedure can be invoked in openCypher using the following calls:
      - CALL example.write_procedure("property_name", "property_value")
        YIELD created_vertex;
      - MATCH (n) WHERE n.my_property IS NOT NULL
        WITH n.my_property as property_value
        CALL example.write_procedure("my_property", property_value)
        YIELD created_vertex;

    Naturally, you may pass in different arguments.
    """
    # Collect all the vertices that has the required property with the same
    # value
    vertices_to_connect = []
    for v in context.graph.vertices:
        if v.properties[property_name] == property_value:
            vertices_to_connect.append(v)
    # Create the new vertex and set its property
    vertex = context.graph.create_vertex()
    vertex.properties.set(property_name, property_value)
    # Connect the new vertex to the other vertices
    for v in vertices_to_connect:
        context.graph.create_edge(vertex, v, mgp.EdgeType("HAS_SAME_VALUE"))

    return mgp.Record(created_vertex=vertex)
