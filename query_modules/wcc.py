import sys
import mgp
try:
    import networkx as nx
except ImportError as import_error:
    sys.stderr.write(
        '\n'
        'NOTE: Please install networkx to be able to use wcc module.\n'
        'Using Python:\n'
        + sys.version +
        '\n')
    raise import_error


@mgp.read_proc
def get_components(vertices: mgp.List[mgp.Vertex],
                   edges: mgp.List[mgp.Edge]
                   ) -> mgp.Record(n_components=int,
                                   components=mgp.List[mgp.List[mgp.Vertex]]):
    '''
    This procedure finds weakly connected components of a given subgraph of a
    directed graph.

    The subgraph is defined by a list of vertices and a list edges which are
    passed as arguments of the procedure. More precisely, a set of vertices of
    a subgraph contains all vertices provided in a list of vertices along with
    all vertices that are endpoints of provided edges. Similarly, a set of
    edges of a subgraph contains all edges from the list of provided edges.

    The procedure returns 2 fields:
        * `n_components` is the number of weakly connected components of the
        subgraph.
        * `components` is a list of weakly connected components. Each component
        is given as a list of `mgp.Vertex` objects from that component.

    For example, weakly connected components in a subgraph formed from all
    vertices labeled `Person` and edges between such vertices can be obtained
    using the following openCypher query:

    MATCH (n:Person)-[e]->(m:Person)
    WITH collect(n) AS nodes, collect(e) AS edges
    CALL wcc.get_components(nodes, edges) YIELD *
    RETURN n_components, components;
    '''
    g = nx.DiGraph()
    g.add_nodes_from(vertices)
    g.add_edges_from([(edge.from_vertex, edge.to_vertex) for edge in edges])

    components = [list(wcc) for wcc in nx.weakly_connected_components(g)]

    return mgp.Record(n_components=len(components), components=components)
