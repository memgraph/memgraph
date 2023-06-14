import sys
import mgp
try:
    import networkx as nx
    import numpy  # noqa E401
    import scipy  # noqa E401
except ImportError as import_error:
    sys.stderr.write((
        '\n'
        'NOTE: Please install networkx, numpy, scipy to be able to '
        'use proxied NetworkX algorithms. E.g., CALL nxalg.pagerank(...).\n'
        'Using Python:\n'
        + sys.version +
        '\n'))
    raise import_error
# Imported last because it also depends on networkx.
from mgp_networkx import (MemgraphMultiDiGraph, MemgraphDiGraph,  # noqa: E402
                          MemgraphMultiGraph, MemgraphGraph,
                          PropertiesDictionary)


# networkx.algorithms.approximation.connectivity.node_connectivity
@mgp.read_proc
def node_connectivity(ctx: mgp.ProcCtx,
                      source: mgp.Nullable[mgp.Vertex] = None,
                      target: mgp.Nullable[mgp.Vertex] = None
                      ) -> mgp.Record(connectivity=int):
    return mgp.Record(connectivity=nx.node_connectivity(
        MemgraphMultiDiGraph(ctx=ctx), source, target))


# networkx.algorithms.assortativity.degree_assortativity_coefficient
@mgp.read_proc
def degree_assortativity_coefficient(
        ctx: mgp.ProcCtx,
        x: str = 'out',
        y: str = 'in',
        weight: mgp.Nullable[str] = None,
        nodes: mgp.Nullable[mgp.List[mgp.Vertex]] = None
) -> mgp.Record(assortativity=float):
    return mgp.Record(assortativity=nx.degree_assortativity_coefficient(
        MemgraphMultiDiGraph(ctx=ctx), x, y, weight, nodes))


# networkx.algorithms.asteroidal.is_at_free
@mgp.read_proc
def is_at_free(ctx: mgp.ProcCtx) -> mgp.Record(is_at_free=bool):
    return mgp.Record(is_at_free=nx.is_at_free(MemgraphGraph(ctx=ctx)))


# networkx.algorithms.bipartite.basic.is_bipartite
@mgp.read_proc
def is_bipartite(ctx: mgp.ProcCtx) -> mgp.Record(is_bipartite=bool):
    return mgp.Record(is_bipartite=nx.is_bipartite(
        MemgraphMultiDiGraph(ctx=ctx)))


# networkx.algorithms.boundary.node_boundary
@mgp.read_proc
def node_boundary(ctx: mgp.ProcCtx,
                  nbunch1: mgp.List[mgp.Vertex],
                  nbunch2: mgp.Nullable[mgp.List[mgp.Vertex]] = None
                  ) -> mgp.Record(boundary=mgp.List[mgp.Vertex]):
    return mgp.Record(boundary=list(nx.node_boundary(
        MemgraphMultiDiGraph(ctx=ctx), nbunch1, nbunch2)))


# networkx.algorithms.bridges.bridges
@mgp.read_proc
def bridges(ctx: mgp.ProcCtx,
            root: mgp.Nullable[mgp.Vertex] = None
            ) -> mgp.Record(bridges=mgp.List[mgp.Edge]):
    g = MemgraphMultiGraph(ctx=ctx)
    return mgp.Record(
        bridges=[next(iter(g[u][v]))
                 for u, v in nx.bridges(MemgraphGraph(ctx=ctx),
                                        root=root)])


# networkx.algorithms.centrality.betweenness_centrality
@mgp.read_proc
def betweenness_centrality(ctx: mgp.ProcCtx,
                           k: mgp.Nullable[int] = None,
                           normalized: bool = True,
                           weight: mgp.Nullable[str] = None,
                           endpoints: bool = False,
                           seed: mgp.Nullable[int] = None
                           ) -> mgp.Record(node=mgp.Vertex,
                                           betweenness=mgp.Number):
    return [mgp.Record(node=n, betweenness=b)
            for n, b in nx.betweenness_centrality(
                    MemgraphDiGraph(ctx=ctx), k=k, normalized=normalized,
                    weight=weight, endpoints=endpoints, seed=seed).items()]


# networkx.algorithms.chains.chain_decomposition
@mgp.read_proc
def chain_decomposition(ctx: mgp.ProcCtx,
                        root: mgp.Nullable[mgp.Vertex] = None
                        ) -> mgp.Record(chains=mgp.List[mgp.List[mgp.Edge]]):
    g = MemgraphMultiGraph(ctx=ctx)
    return mgp.Record(
        chains=[[next(iter(g[u][v])) for u, v in d]
                for d in nx.chain_decomposition(MemgraphGraph(ctx=ctx),
                                                root=root)])


# networkx.algorithms.chordal.is_chordal
@mgp.read_proc
def is_chordal(ctx: mgp.ProcCtx) -> mgp.Record(is_chordal=bool):
    return mgp.Record(is_chordal=nx.is_chordal(MemgraphGraph(ctx=ctx)))


# networkx.algorithms.clique.find_cliques
@mgp.read_proc
def find_cliques(ctx: mgp.ProcCtx
                 ) -> mgp.Record(cliques=mgp.List[mgp.List[mgp.Vertex]]):
    return mgp.Record(cliques=list(nx.find_cliques(
        MemgraphMultiGraph(ctx=ctx))))


# networkx.algorithms.cluster.clustering
@mgp.read_proc
def clustering(ctx: mgp.ProcCtx,
               nodes: mgp.Nullable[mgp.List[mgp.Vertex]] = None,
               weight: mgp.Nullable[str] = None
               ) -> mgp.Record(node=mgp.Vertex, clustering=mgp.Number):
    return [mgp.Record(node=n, clustering=c)
            for n, c in nx.clustering(
                    MemgraphDiGraph(ctx=ctx), nodes=nodes,
                    weight=weight).items()]


# networkx.algorithms.coloring.greedy_color
@mgp.read_proc
def greedy_color(ctx: mgp.ProcCtx,
                 strategy: str = 'largest_first',
                 interchange: bool = False
                 ) -> mgp.Record(node=mgp.Vertex, color=int):
    return [mgp.Record(node=n, color=c) for n, c in nx.greedy_color(
        MemgraphMultiDiGraph(ctx=ctx), strategy, interchange).items()]


# networkx.algorithms.communicability_alg.communicability
@mgp.read_proc
def communicability(ctx: mgp.ProcCtx
                    ) -> mgp.Record(node1=mgp.Vertex, node2=mgp.Vertex,
                                    communicability=mgp.Number):
    return [mgp.Record(node1=n1, node2=n2, communicability=v)
            for n1, d in nx.communicability(MemgraphGraph(ctx=ctx)).items()
            for n2, v in d.items()]


# networkx.algorithms.community.kclique.k_clique_communities
@mgp.read_proc
def k_clique_communities(
        ctx: mgp.ProcCtx,
        k: int,
        cliques: mgp.Nullable[mgp.List[mgp.List[mgp.Vertex]]] = None
) -> mgp.Record(communities=mgp.List[mgp.List[mgp.Vertex]]):
    return mgp.Record(communities=[
        list(s) for s in nx.community.k_clique_communities(
            MemgraphMultiGraph(ctx=ctx), k, cliques)])


# networkx.algorithms.approximation.kcomponents.k_components
@mgp.read_proc
def k_components(ctx: mgp.ProcCtx,
                 density: mgp.Number = 0.95
                 ) -> mgp.Record(k=int,
                                 components=mgp.List[mgp.List[mgp.Vertex]]):
    kcomps = nx.k_components(MemgraphMultiGraph(ctx=ctx), density)

    return [mgp.Record(k=k, components=[list(s) for s in comps])
            for k, comps in kcomps.items()]


# networkx.algorithms.components.biconnected_components
@mgp.read_proc
def biconnected_components(
        ctx: mgp.ProcCtx
) -> mgp.Record(components=mgp.List[mgp.List[mgp.Vertex]]):
    comps = nx.biconnected_components(MemgraphMultiGraph(ctx=ctx))
    return mgp.Record(components=[list(s) for s in comps])


# networkx.algorithms.components.strongly_connected_components
@mgp.read_proc
def strongly_connected_components(
        ctx: mgp.ProcCtx
) -> mgp.Record(components=mgp.List[mgp.List[mgp.Vertex]]):
    comps = nx.strongly_connected_components(MemgraphMultiDiGraph(ctx=ctx))
    return mgp.Record(components=[list(s) for s in comps])


# networkx.algorithms.connectivity.edge_kcomponents.k_edge_components
#
# NOTE: NetworkX 2.4, algorithms/connectivity/edge_kcompnents.py:367. We create
# a *copy* of the graph because the algorithm copies the graph using
# __class__() and tries to modify it.
@mgp.read_proc
def k_edge_components(
        ctx: mgp.ProcCtx,
        k: int
) -> mgp.Record(components=mgp.List[mgp.List[mgp.Vertex]]):
    return mgp.Record(components=[list(s) for s in nx.k_edge_components(
        nx.DiGraph(MemgraphDiGraph(ctx=ctx)), k)])


# networkx.algorithms.core.core_number
@mgp.read_proc
def core_number(ctx: mgp.ProcCtx
                ) -> mgp.Record(node=mgp.Vertex, core=mgp.Number):
    return [mgp.Record(node=n, core=c)
            for n, c in nx.core_number(MemgraphDiGraph(ctx=ctx)).items()]


# networkx.algorithms.covering.is_edge_cover
@mgp.read_proc
def is_edge_cover(ctx: mgp.ProcCtx, cover: mgp.List[mgp.Edge]
                  ) -> mgp.Record(is_edge_cover=bool):
    cover = set([(e.from_vertex, e.to_vertex) for e in cover])
    return mgp.Record(is_edge_cover=nx.is_edge_cover(
        MemgraphMultiGraph(ctx=ctx), cover))


# networkx.algorithms.cycles.find_cycle
@mgp.read_proc
def find_cycle(ctx: mgp.ProcCtx,
               source: mgp.Nullable[mgp.List[mgp.Vertex]] = None,
               orientation: mgp.Nullable[str] = None
               ) -> mgp.Record(cycle=mgp.Nullable[mgp.List[mgp.Edge]]):
    try:
        return mgp.Record(cycle=[e for _, _, e in nx.find_cycle(
            MemgraphMultiDiGraph(ctx=ctx), source, orientation)])
    except nx.NetworkXNoCycle:
        return mgp.Record(cycle=None)


# networkx.algorithms.cycles.simple_cycles
#
# NOTE: NetworkX 2.4, algorithms/cycles.py:183. We create a *copy* of the graph
# because the algorithm copies the graph using type() and tries to pass initial
# data.
@mgp.read_proc
def simple_cycles(ctx: mgp.ProcCtx
                  ) -> mgp.Record(cycles=mgp.List[mgp.List[mgp.Vertex]]):
    return mgp.Record(cycles=list(nx.simple_cycles(
        nx.MultiDiGraph(MemgraphMultiDiGraph(ctx=ctx)).copy())))


# networkx.algorithms.cuts.node_expansion
@mgp.read_proc
def node_expansion(ctx: mgp.ProcCtx, s: mgp.List[mgp.Vertex]
                   ) -> mgp.Record(node_expansion=mgp.Number):
    return mgp.Record(node_expansion=nx.node_expansion(
        MemgraphMultiDiGraph(ctx=ctx), set(s)))


# networkx.algorithms.dag.topological_sort
@mgp.read_proc
def topological_sort(ctx: mgp.ProcCtx
                     ) -> mgp.Record(nodes=mgp.Nullable[mgp.List[mgp.Vertex]]):
    return mgp.Record(nodes=list(nx.topological_sort(
        MemgraphMultiDiGraph(ctx=ctx))))


# networkx.algorithms.dag.ancestors
@mgp.read_proc
def ancestors(ctx: mgp.ProcCtx,
              source: mgp.Vertex
              ) -> mgp.Record(ancestors=mgp.List[mgp.Vertex]):
    return mgp.Record(ancestors=list(nx.ancestors(
        MemgraphMultiDiGraph(ctx=ctx), source)))


# networkx.algorithms.dag.descendants
@mgp.read_proc
def descendants(ctx: mgp.ProcCtx,
                source: mgp.Vertex
                ) -> mgp.Record(descendants=mgp.List[mgp.Vertex]):
    return mgp.Record(descendants=list(nx.descendants(
        MemgraphMultiDiGraph(ctx=ctx), source)))


# networkx.algorithms.distance_measures.center
#
# NOTE: Takes more parameters.
@mgp.read_proc
def center(ctx: mgp.ProcCtx) -> mgp.Record(center=mgp.List[mgp.Vertex]):
    return mgp.Record(center=list(nx.center(MemgraphMultiDiGraph(ctx=ctx))))


# networkx.algorithms.distance_measures.diameter
#
# NOTE: Takes more parameters.
@mgp.read_proc
def diameter(ctx: mgp.ProcCtx) -> mgp.Record(diameter=int):
    return mgp.Record(diameter=nx.diameter(MemgraphMultiDiGraph(ctx=ctx)))


# networkx.algorithms.distance_regular.is_distance_regular
@mgp.read_proc
def is_distance_regular(ctx: mgp.ProcCtx
                        ) -> mgp.Record(is_distance_regular=bool):
    return mgp.Record(is_distance_regular=nx.is_distance_regular(
        MemgraphMultiGraph(ctx=ctx)))


# networkx.algorithms.strongly_regular.is_strongly_regular
@mgp.read_proc
def is_strongly_regular(ctx: mgp.ProcCtx
                        ) -> mgp.Record(is_strongly_regular=bool):
    return mgp.Record(is_strongly_regular=nx.is_strongly_regular(
        MemgraphMultiGraph(ctx=ctx)))


# networkx.algorithms.dominance.dominance_frontiers
@mgp.read_proc
def dominance_frontiers(ctx: mgp.ProcCtx, start: mgp.Vertex,
                        ) -> mgp.Record(node=mgp.Vertex,
                                        frontier=mgp.List[mgp.Vertex]):
    return [mgp.Record(node=n, frontier=list(f))
            for n, f in nx.dominance_frontiers(
                    MemgraphMultiDiGraph(ctx=ctx), start).items()]


# networkx.algorithms.dominance.immediate_dominators
@mgp.read_proc
def immediate_dominators(ctx: mgp.ProcCtx, start: mgp.Vertex,
                         ) -> mgp.Record(node=mgp.Vertex,
                                         dominator=mgp.Vertex):
    return [mgp.Record(node=n, dominator=d)
            for n, d in nx.immediate_dominators(
                    MemgraphMultiDiGraph(ctx=ctx), start).items()]


# networkx.algorithms.dominating.dominating_set
@mgp.read_proc
def dominating_set(ctx: mgp.ProcCtx, start: mgp.Vertex,
                   ) -> mgp.Record(dominating_set=mgp.List[mgp.Vertex]):
    return mgp.Record(dominating_set=list(nx.dominating_set(
        MemgraphMultiDiGraph(ctx=ctx), start)))


# networkx.algorithms.efficiency_measures.local_efficiency
@mgp.read_proc
def local_efficiency(ctx: mgp.ProcCtx) -> mgp.Record(local_efficiency=float):
    return mgp.Record(local_efficiency=nx.local_efficiency(
        MemgraphMultiGraph(ctx=ctx)))


# networkx.algorithms.efficiency_measures.global_efficiency
@mgp.read_proc
def global_efficiency(ctx: mgp.ProcCtx) -> mgp.Record(global_efficiency=float):
    return mgp.Record(global_efficiency=nx.global_efficiency(
        MemgraphMultiGraph(ctx=ctx)))


# networkx.algorithms.euler.is_eulerian
@mgp.read_proc
def is_eulerian(ctx: mgp.ProcCtx) -> mgp.Record(is_eulerian=bool):
    return mgp.Record(is_eulerian=nx.is_eulerian(
        MemgraphMultiDiGraph(ctx=ctx)))


# networkx.algorithms.euler.is_semieulerian
@mgp.read_proc
def is_semieulerian(ctx: mgp.ProcCtx) -> mgp.Record(is_semieulerian=bool):
    return mgp.Record(is_semieulerian=nx.is_semieulerian(
        MemgraphMultiDiGraph(ctx=ctx)))


# networkx.algorithms.euler.has_eulerian_path
@mgp.read_proc
def has_eulerian_path(ctx: mgp.ProcCtx) -> mgp.Record(has_eulerian_path=bool):
    return mgp.Record(has_eulerian_path=nx.has_eulerian_path(
        MemgraphMultiDiGraph(ctx=ctx)))


# networkx.algorithms.hierarchy.flow_hierarchy
@mgp.read_proc
def flow_hierarchy(ctx: mgp.ProcCtx,
                   weight: mgp.Nullable[str] = None
                   ) -> mgp.Record(flow_hierarchy=float):
    return mgp.Record(flow_hierarchy=nx.flow_hierarchy(
        MemgraphMultiDiGraph(ctx=ctx), weight=weight))


# networkx.algorithms.isolate.isolates
@mgp.read_proc
def isolates(ctx: mgp.ProcCtx) -> mgp.Record(isolates=mgp.List[mgp.Vertex]):
    return mgp.Record(isolates=list(nx.isolates(
        MemgraphMultiDiGraph(ctx=ctx))))


# networkx.algorithms.isolate.is_isolate
@mgp.read_proc
def is_isolate(ctx: mgp.ProcCtx, n: mgp.Vertex
               ) -> mgp.Record(is_isolate=bool):
    return mgp.Record(is_isolate=nx.is_isolate(
        MemgraphMultiDiGraph(ctx=ctx), n))


# networkx.algorithms.isomorphism.is_isomorphic
@mgp.read_proc
def is_isomorphic(ctx: mgp.ProcCtx,
                  nodes1: mgp.List[mgp.Vertex],
                  edges1: mgp.List[mgp.Edge],
                  nodes2: mgp.List[mgp.Vertex],
                  edges2: mgp.List[mgp.Edge]
                  ) -> mgp.Record(is_isomorphic=bool):
    nodes1, edges1, nodes2, edges2 = map(set, [nodes1, edges1, nodes2, edges2])
    g = MemgraphMultiDiGraph(ctx=ctx)
    g1 = nx.subgraph_view(
        g, lambda n: n in nodes1, lambda n1, n2, e: e in edges1)
    g2 = nx.subgraph_view(
        g, lambda n: n in nodes2, lambda n1, n2, e: e in edges2)
    return mgp.Record(is_isomorphic=nx.is_isomorphic(g1, g2))


# networkx.algorithms.link_analysis.pagerank_alg.pagerank
@mgp.read_proc
def pagerank(ctx: mgp.ProcCtx,
             alpha: mgp.Number = 0.85,
             personalization: mgp.Nullable[str] = None,
             max_iter: int = 100,
             tol: mgp.Number = 1e-06,
             nstart: mgp.Nullable[str] = None,
             weight: mgp.Nullable[str] = 'weight',
             dangling: mgp.Nullable[str] = None,
             ) -> mgp.Record(node=mgp.Vertex, rank=float):
    def to_properties_dictionary(prop):
        return None if prop is None else PropertiesDictionary(ctx, prop)

    pg = nx.pagerank(MemgraphDiGraph(ctx=ctx), alpha=alpha,
                     personalization=to_properties_dictionary(personalization),
                     max_iter=max_iter, tol=tol,
                     nstart=to_properties_dictionary(nstart), weight=weight,
                     dangling=to_properties_dictionary(dangling))

    return [mgp.Record(node=k, rank=v) for k, v in pg.items()]


# networkx.algorithms.link_prediction.jaccard_coefficient
@mgp.read_proc
def jaccard_coefficient(
        ctx: mgp.ProcCtx,
        ebunch: mgp.Nullable[mgp.List[mgp.List[mgp.Vertex]]] = None
) -> mgp.Record(u=mgp.Vertex, v=mgp.Vertex,
                coef=float):
    return [mgp.Record(u=u, v=v, coef=c) for u, v, c
            in nx.jaccard_coefficient(MemgraphGraph(ctx=ctx), ebunch)]


# networkx.algorithms.lowest_common_ancestors.lowest_common_ancestor
@mgp.read_proc
def lowest_common_ancestor(ctx: mgp.ProcCtx, node1: mgp.Vertex,
                           node2: mgp.Vertex
                           ) -> mgp.Record(ancestor=mgp.Nullable[mgp.Vertex]):
    return mgp.Record(ancestor=nx.lowest_common_ancestor(
        MemgraphDiGraph(ctx=ctx), node1, node2))


# networkx.algorithms.matching.maximal_matching
@mgp.read_proc
def maximal_matching(ctx: mgp.ProcCtx) -> mgp.Record(edges=mgp.List[mgp.Edge]):
    g = MemgraphMultiDiGraph(ctx=ctx)
    return mgp.Record(edges=list(
        next(iter(g[u][v])) for u, v in nx.maximal_matching(g)))


# networkx.algorithms.planarity.check_planarity
#
# NOTE: Returns a graph.
@mgp.read_proc
def check_planarity(ctx: mgp.ProcCtx) -> mgp.Record(is_planar=bool):
    return mgp.Record(is_planar=nx.check_planarity(
        MemgraphMultiDiGraph(ctx=ctx))[0])


# networkx.algorithms.non_randomness.non_randomness
@mgp.read_proc
def non_randomness(ctx: mgp.ProcCtx,
                   k: mgp.Nullable[int] = None
                   ) -> mgp.Record(non_randomness=float,
                                   relative_non_randomness=float):
    nn, rnn = nx.non_randomness(
        MemgraphGraph(ctx=ctx), k=k)
    return mgp.Record(non_randomness=nn, relative_non_randomness=rnn)


# networkx.algorithms.reciprocity.reciprocity
@mgp.read_proc
def reciprocity(ctx: mgp.ProcCtx,
                nodes: mgp.Nullable[mgp.List[mgp.Vertex]] = None
                ) -> mgp.Record(node=mgp.Nullable[mgp.Vertex],
                                reciprocity=mgp.Nullable[float]):
    rp = nx.reciprocity(MemgraphMultiDiGraph(ctx=ctx), nodes=nodes)
    if nodes is None:
        return mgp.Record(node=None, reciprocity=rp)
    else:
        return [mgp.Record(node=n, reciprocity=r) for n, r in rp.items()]


# networkx.algorithms.shortest_paths.generic.shortest_path
@mgp.read_proc
def shortest_path(ctx: mgp.ProcCtx,
                  source: mgp.Nullable[mgp.Vertex] = None,
                  target: mgp.Nullable[mgp.Vertex] = None,
                  weight: mgp.Nullable[str] = None,
                  method: str = 'dijkstra'
                  ) -> mgp.Record(source=mgp.Vertex, target=mgp.Vertex,
                                  path=mgp.List[mgp.Vertex]):
    sp = nx.shortest_path(MemgraphMultiDiGraph(ctx=ctx), source=source,
                          target=target, weight=weight, method=method)

    if source and target:
        sp = {source: {target: sp}}
    elif source and not target:
        sp = {source: sp}
    elif not source and target:
        sp = {source: {target: p} for source, p in sp.items()}

    return [mgp.Record(source=s, target=t, path=p)
            for s, d in sp.items()
            for t, p in d.items()]


# networkx.algorithms.shortest_paths.generic.shortest_path_length
@mgp.read_proc
def shortest_path_length(ctx: mgp.ProcCtx,
                         source: mgp.Nullable[mgp.Vertex] = None,
                         target: mgp.Nullable[mgp.Vertex] = None,
                         weight: mgp.Nullable[str] = None,
                         method: str = 'dijkstra'
                         ) -> mgp.Record(source=mgp.Vertex, target=mgp.Vertex,
                                         length=mgp.Number):
    sp = nx.shortest_path_length(MemgraphMultiDiGraph(ctx=ctx), source=source,
                                 target=target, weight=weight, method=method)

    if source and target:
        sp = {source: {target: sp}}
    elif source and not target:
        sp = {source: sp}
    elif not source and target:
        sp = {source: {target: l} for source, l in sp.items()}
    else:
        sp = dict(sp)

    return [mgp.Record(source=s, target=t, length=l)
            for s, d in sp.items()
            for t, l in d.items()]


# networkx.algorithms.shortest_paths.generic.all_shortest_paths
@mgp.read_proc
def all_shortest_paths(ctx: mgp.ProcCtx,
                       source: mgp.Vertex,
                       target: mgp.Vertex,
                       weight: mgp.Nullable[str] = None,
                       method: str = 'dijkstra'
                       ) -> mgp.Record(paths=mgp.List[mgp.List[mgp.Vertex]]):
    return mgp.Record(paths=list(nx.all_shortest_paths(
        MemgraphMultiDiGraph(ctx=ctx), source=source, target=target,
        weight=weight, method=method)))


# networkx.algorithms.shortest_paths.generic.has_path
@mgp.read_proc
def has_path(ctx: mgp.ProcCtx,
             source: mgp.Vertex,
             target: mgp.Vertex) -> mgp.Record(has_path=bool):
    return mgp.Record(has_path=nx.has_path(MemgraphMultiDiGraph(ctx=ctx),
                                           source, target))


# networkx.algorithms.shortest_paths.weighted.multi_source_dijkstra_path
@mgp.read_proc
def multi_source_dijkstra_path(ctx: mgp.ProcCtx,
                               sources: mgp.List[mgp.Vertex],
                               cutoff: mgp.Nullable[int] = None,
                               weight: str = 'weight'
                               ) -> mgp.Record(target=mgp.Vertex,
                                               path=mgp.List[mgp.Vertex]):
    return [mgp.Record(target=t, path=p)
            for t, p in nx.multi_source_dijkstra_path(
                    MemgraphMultiDiGraph(ctx=ctx), sources, cutoff=cutoff,
                    weight=weight).items()]


# networkx.algorithms.shortest_paths.weighted.multi_source_dijkstra_path_length
@mgp.read_proc
def multi_source_dijkstra_path_length(ctx: mgp.ProcCtx,
                                      sources: mgp.List[mgp.Vertex],
                                      cutoff: mgp.Nullable[int] = None,
                                      weight: str = 'weight'
                                      ) -> mgp.Record(target=mgp.Vertex,
                                                      length=mgp.Number):
    return [mgp.Record(target=t, length=l)
            for t, l in nx.multi_source_dijkstra_path_length(
                    MemgraphMultiDiGraph(ctx=ctx), sources, cutoff=cutoff,
                    weight=weight).items()]


# networkx.algorithms.simple_paths.is_simple_path
@mgp.read_proc
def is_simple_path(ctx: mgp.ProcCtx,
                   nodes: mgp.List[mgp.Vertex]
                   ) -> mgp.Record(is_simple_path=bool):
    return mgp.Record(is_simple_path=nx.is_simple_path(
        MemgraphMultiDiGraph(ctx=ctx), nodes))


# networkx.algorithms.simple_paths.all_simple_paths
@mgp.read_proc
def all_simple_paths(ctx: mgp.ProcCtx,
                     source: mgp.Vertex,
                     target: mgp.Vertex,
                     cutoff: mgp.Nullable[int] = None
                     ) -> mgp.Record(paths=mgp.List[mgp.List[mgp.Vertex]]):
    return mgp.Record(paths=list(nx.all_simple_paths(
        MemgraphMultiDiGraph(ctx=ctx), source, target, cutoff=cutoff)))


# networkx.algorithms.tournament.is_tournament
@mgp.read_proc
def is_tournament(ctx: mgp.ProcCtx) -> mgp.Record(is_tournament=bool):
    return mgp.Record(is_tournament=nx.tournament.is_tournament(
        MemgraphDiGraph(ctx=ctx)))


# networkx.algorithms.traversal.breadth_first_search.bfs_edges
@mgp.read_proc
def bfs_edges(ctx: mgp.ProcCtx,
              source: mgp.Vertex,
              reverse: bool = False,
              depth_limit: mgp.Nullable[int] = None
              ) -> mgp.Record(edges=mgp.List[mgp.Edge]):
    return mgp.Record(edges=list(nx.bfs_edges(
        MemgraphMultiDiGraph(ctx=ctx), source, reverse=reverse,
        depth_limit=depth_limit)))


# networkx.algorithms.traversal.breadth_first_search.bfs_tree
@mgp.read_proc
def bfs_tree(ctx: mgp.ProcCtx,
             source: mgp.Vertex,
             reverse: bool = False,
             depth_limit: mgp.Nullable[int] = None
             ) -> mgp.Record(tree=mgp.List[mgp.Vertex]):
    return mgp.Record(tree=list(nx.bfs_tree(
        MemgraphMultiDiGraph(ctx=ctx), source, reverse=reverse,
        depth_limit=depth_limit)))


# networkx.algorithms.traversal.breadth_first_search.bfs_predecessors
@mgp.read_proc
def bfs_predecessors(ctx: mgp.ProcCtx,
                     source: mgp.Vertex,
                     depth_limit: mgp.Nullable[int] = None
                     ) -> mgp.Record(node=mgp.Vertex,
                                     predecessor=mgp.Vertex):
    return [mgp.Record(node=n, predecessor=p)
            for n, p in nx.bfs_predecessors(
                    MemgraphMultiDiGraph(ctx=ctx), source,
                    depth_limit=depth_limit)]


# networkx.algorithms.traversal.breadth_first_search.bfs_successors
@mgp.read_proc
def bfs_successors(ctx: mgp.ProcCtx,
                   source: mgp.Vertex,
                   depth_limit: mgp.Nullable[int] = None
                   ) -> mgp.Record(node=mgp.Vertex,
                                   successors=mgp.List[mgp.Vertex]):
    return [mgp.Record(node=n, successors=s)
            for n, s in nx.bfs_successors(
                    MemgraphMultiDiGraph(ctx=ctx), source,
                    depth_limit=depth_limit)]


# networkx.algorithms.traversal.depth_first_search.dfs_tree
@mgp.read_proc
def dfs_tree(ctx: mgp.ProcCtx,
             source: mgp.Vertex,
             depth_limit: mgp.Nullable[int] = None
             ) -> mgp.Record(tree=mgp.List[mgp.Vertex]):
    return mgp.Record(tree=list(nx.dfs_tree(
        MemgraphMultiDiGraph(ctx=ctx), source, depth_limit=depth_limit)))


# networkx.algorithms.traversal.depth_first_search.dfs_predecessors
@mgp.read_proc
def dfs_predecessors(ctx: mgp.ProcCtx,
                     source: mgp.Vertex,
                     depth_limit: mgp.Nullable[int] = None
                     ) -> mgp.Record(node=mgp.Vertex,
                                     predecessor=mgp.Vertex):
    return [mgp.Record(node=n, predecessor=p)
            for n, p in nx.dfs_predecessors(
                    MemgraphMultiDiGraph(ctx=ctx), source,
                    depth_limit=depth_limit).items()]


# networkx.algorithms.traversal.depth_first_search.dfs_successors
@mgp.read_proc
def dfs_successors(ctx: mgp.ProcCtx,
                   source: mgp.Vertex,
                   depth_limit: mgp.Nullable[int] = None
                   ) -> mgp.Record(node=mgp.Vertex,
                                   successors=mgp.List[mgp.Vertex]):
    return [mgp.Record(node=n, successors=s)
            for n, s in nx.dfs_successors(
                    MemgraphMultiDiGraph(ctx=ctx), source,
                    depth_limit=depth_limit).items()]


# networkx.algorithms.traversal.depth_first_search.dfs_preorder_nodes
@mgp.read_proc
def dfs_preorder_nodes(ctx: mgp.ProcCtx,
                       source: mgp.Vertex,
                       depth_limit: mgp.Nullable[int] = None
                       ) -> mgp.Record(nodes=mgp.List[mgp.Vertex]):
    return mgp.Record(nodes=list(nx.dfs_preorder_nodes(
        MemgraphMultiDiGraph(ctx=ctx), source, depth_limit=depth_limit)))


# networkx.algorithms.traversal.depth_first_search.dfs_postorder_nodes
@mgp.read_proc
def dfs_postorder_nodes(ctx: mgp.ProcCtx,
                        source: mgp.Vertex,
                        depth_limit: mgp.Nullable[int] = None
                        ) -> mgp.Record(nodes=mgp.List[mgp.Vertex]):
    return mgp.Record(nodes=list(nx.dfs_postorder_nodes(
        MemgraphMultiDiGraph(ctx=ctx), source, depth_limit=depth_limit)))


# networkx.algorithms.traversal.edgebfs.edge_bfs
@mgp.read_proc
def edge_bfs(ctx: mgp.ProcCtx,
             source: mgp.Nullable[mgp.Vertex] = None,
             orientation: mgp.Nullable[str] = None
             ) -> mgp.Record(edges=mgp.List[mgp.Edge]):
    return mgp.Record(edges=list(e for _, _, e in nx.edge_bfs(
        MemgraphMultiDiGraph(ctx=ctx), source=source,
        orientation=orientation)))


# networkx.algorithms.traversal.edgedfs.edge_dfs
@mgp.read_proc
def edge_dfs(ctx: mgp.ProcCtx,
             source: mgp.Nullable[mgp.Vertex] = None,
             orientation: mgp.Nullable[str] = None
             ) -> mgp.Record(edges=mgp.List[mgp.Edge]):
    return mgp.Record(edges=list(e for _, _, e in nx.edge_dfs(
        MemgraphMultiDiGraph(ctx=ctx), source=source,
        orientation=orientation)))


# networkx.algorithms.tree.recognition.is_tree
@mgp.read_proc
def is_tree(ctx: mgp.ProcCtx) -> mgp.Record(is_tree=bool):
    return mgp.Record(is_tree=nx.is_tree(MemgraphDiGraph(ctx=ctx)))


# networkx.algorithms.tree.recognition.is_forest
@mgp.read_proc
def is_forest(ctx: mgp.ProcCtx) -> mgp.Record(is_forest=bool):
    return mgp.Record(is_forest=nx.is_forest(MemgraphDiGraph(ctx=ctx)))


# networkx.algorithms.tree.recognition.is_arborescence
@mgp.read_proc
def is_arborescence(ctx: mgp.ProcCtx) -> mgp.Record(is_arborescence=bool):
    return mgp.Record(is_arborescence=nx.is_arborescence(
        MemgraphDiGraph(ctx=ctx)))


# networkx.algorithms.tree.recognition.is_branching
@mgp.read_proc
def is_branching(ctx: mgp.ProcCtx) -> mgp.Record(is_branching=bool):
    return mgp.Record(is_branching=nx.is_branching(MemgraphDiGraph(ctx=ctx)))


# networkx.algorithms.tree.mst.minimum_spanning_tree
@mgp.read_proc
def minimum_spanning_tree(ctx: mgp.ProcCtx,
                          weight: str = 'weight',
                          algorithm: str = 'kruskal',
                          ignore_nan: bool = False
                          ) -> mgp.Record(nodes=mgp.List[mgp.Vertex],
                                          edges=mgp.List[mgp.Edge]):
    gres = nx.minimum_spanning_tree(MemgraphMultiGraph(ctx=ctx),
                                    weight, algorithm, ignore_nan)
    return mgp.Record(nodes=list(gres.nodes()),
                      edges=[e for _, _, e in gres.edges(keys=True)])


# networkx.algorithms.triads.triadic_census
@mgp.read_proc
def triadic_census(ctx: mgp.ProcCtx) -> mgp.Record(triad=str, count=int):
    return [mgp.Record(triad=t, count=c)
            for t, c in nx.triadic_census(MemgraphDiGraph(ctx=ctx)).items()]


# networkx.algorithms.voronoi.voronoi_cells
@mgp.read_proc
def voronoi_cells(ctx: mgp.ProcCtx,
                  center_nodes: mgp.List[mgp.Vertex],
                  weight: str = 'weight'
                  ) -> mgp.Record(center=mgp.Vertex,
                                  cell=mgp.List[mgp.Vertex]):
    return [mgp.Record(center=c1, cell=list(c2))
            for c1, c2 in nx.voronoi_cells(
                    MemgraphMultiDiGraph(ctx=ctx), center_nodes,
                    weight=weight).items()]


# networkx.algorithms.wiener.wiener_index
@mgp.read_proc
def wiener_index(ctx: mgp.ProcCtx, weight: mgp.Nullable[str] = None
                 ) -> mgp.Record(wiener_index=mgp.Number):
    return mgp.Record(wiener_index=nx.wiener_index(
        MemgraphMultiDiGraph(ctx=ctx), weight=weight))
