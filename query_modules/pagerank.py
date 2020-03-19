import mgp
import networkx as nx
from itertools import chain


class VertexDictionary:
    def __init__(self, graph, prop):
        self.graph = graph
        self.prop = prop
        self.len = None

    def get(self, vertex, default=None):
        return vertex.properties.get(self.prop, default=default)

    def items(self):
        for v in self.graph.vertices:
            if self.prop in v.properties:
                yield v, v.properties[self.prop]

    def keys(self):
        for k, v in self.items():
            yield k

    def values(self):
        for k, v in self.items():
            yield v

    def __len__(self):
        if not self.len:
            self.len = sum(1 for _ in self.items())
        return self.len

    def __iter__(self):
        for k, v in self.items():
            yield k

    def __getitem__(self, vertex):
        try:
            return vertex.properties[self.prop]
        except KeyError:
            raise KeyError(("Vertex {} doesn\t have the required " +
                            "property '{}'").format(vertex.id, self.prop))

    def __contains__(self, vertex):
        return vertex in self.graph.vertices and self.prop in vertex.properties


@mgp.read_proc
def pagerank(ctx: mgp.ProcCtx,
             alpha: mgp.Number = 0.85,
             personalization: mgp.Nullable[str] = None,
             max_iter: int = 100,
             tol: mgp.Number = 1e-06,
             nstart: mgp.Nullable[str] = None,
             weight: mgp.Nullable[str] = 'weight',
             dangling: mgp.Nullable[str] = None
             ) -> mgp.Record(node=mgp.Vertex, rank=float):
    '''Run the PageRank algorithm on the whole graph.

    The available parameters are:

    - `alpha` -- Damping parameter.

    - `personalization` -- The "personalization vector". A string specifying
      the property that will be looked up for every node to give the
      personalization value. If a node doesn't have the specified property, its
      personalization value will be zero. By default, a uniform distribution is
      used.

    - `max_iter` -- Maximum number of iterations in the power method eigenvalue
      solver.

    - `tol` -- Error tolerance used to check for convergence in the power
      eigenvalue method solver.

    - `nstart` -- A string specifying the property that will be looked up for
      every node to give the starting value for the iteration.

    - `weight` -- A string specifying the property that will be looked up for
      every edge to give the weight. If None or if the property doesn't exist,
      weights are set to 1.

    - `dangling` -- The outedges to be assigned to any "dangling" nodes, i.e.,
      nodes without any outedges. A string specifying the property that will be
      looked up for every node to give the weight of the outedge that points to
      that node. By default, dangling nodes are given outedges according to the
      personalization vector. This must be selected to result in an irreducible
      transition matrix. It may be common to have the dangling dictionary be
      the same as the personalization dictionary.

    Return a single record for every node. Each record has two fields, `node`
    and `rank`, which together give the calculated rank for the node.

    As an example, the following openCypher query calculates the ranks of all
    the nodes in the graph using the PageRank algorithm. The personalization
    value for every node is taken from its property named 'personalization',
    while the `alpha` and `max_iter` parameters are set to 0.85 and 150
    respectively:

    CALL pagerank.pagerank(0.85, 'personalization', 150) YIELD *;

    '''
    def to_vertex_dictionary(prop):
        return None if prop is None else VertexDictionary(ctx.graph, prop)

    def make_and_check_vertex(v):
        for prop in (personalization, nstart, dangling):
            if prop is None:
                continue
            if not isinstance(v.properties.get(prop, default=1), (int, float)):
                raise TypeError(("Property '{}' of vertex '{}' needs to " +
                                "be a number").format(prop, v.id))
        return v

    def make_and_check_edge(e):
        if (weight is not None and
            not isinstance(e.properties.get(weight, default=1), (int, float))):
            raise TypeError("Property '{}' of edge '{}' needs to be a number"
                            .format(weight, e.id))
        return e.from_vertex, e.to_vertex, e.properties

    g = nx.DiGraph()
    g.add_nodes_from(make_and_check_vertex(v) for v in ctx.graph.vertices)
    g.add_edges_from(make_and_check_edge(e)
                     for v in ctx.graph.vertices
                     for e in chain(v.in_edges, v.out_edges))

    pg = nx.pagerank(g, alpha=alpha,
                     personalization=to_vertex_dictionary(personalization),
                     max_iter=max_iter, tol=tol,
                     nstart=to_vertex_dictionary(nstart), weight=weight,
                     dangling=to_vertex_dictionary(dangling))

    return [mgp.Record(node=k, rank=v) for k, v in pg.items()]
