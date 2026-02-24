from collections import defaultdict
from typing import Dict, List

import mgp
from mgp_igraph import (
    CommunityDetectionObjectiveFunctionOptions,
    InvalidCommunityDetectionObjectiveFunctionException,
    InvalidPageRankImplementationOption,
    InvalidTopologicalSortingModeException,
    MemgraphIgraph,
    PageRankImplementationOptions,
    TopologicalSortException,
    TopologicalSortingModes,
)


@mgp.read_proc
def maxflow(
    ctx: mgp.ProcCtx,
    source: mgp.Vertex,
    target: mgp.Vertex,
    capacity: str = "weight",
) -> mgp.Record(max_flow=mgp.Number):
    graph = MemgraphIgraph(ctx=ctx, directed=True)
    max_flow_value = graph.maxflow(source=source, target=target, capacity=capacity)

    return mgp.Record(max_flow=max_flow_value)


@mgp.read_proc
def pagerank(
    ctx: mgp.ProcCtx,
    damping: mgp.Number = 0.85,
    weights: mgp.Nullable[str] = None,
    directed: bool = True,
    implementation: str = "prpack",
) -> mgp.Record(node=mgp.Vertex, rank=float):
    if implementation not in [
        PageRankImplementationOptions.PRPACK.value,
        PageRankImplementationOptions.ARPACK.value,
    ]:
        raise InvalidPageRankImplementationOption('Implementation argument value can be "prpack" or "arpack"')
    graph = MemgraphIgraph(ctx=ctx, directed=directed)
    pagerank_values = graph.pagerank(
        weights=weights,
        directed=directed,
        damping=damping,
        implementation=implementation,
    )

    return [mgp.Record(node=node, rank=rank) for node, rank in pagerank_values]


@mgp.read_proc
def get_all_simple_paths(
    ctx: mgp.ProcCtx,
    v: mgp.Vertex,
    to: mgp.Vertex,
    cutoff: int = -1,
) -> mgp.Record(path=mgp.List[mgp.Vertex]):
    graph = MemgraphIgraph(ctx=ctx, directed=True)

    return [mgp.Record(path=path) for path in graph.get_all_simple_paths(v=v, to=to, cutoff=cutoff)]


@mgp.read_proc
def mincut(
    ctx: mgp.ProcCtx,
    source: mgp.Vertex,
    target: mgp.Vertex,
    capacity: mgp.Nullable[str] = None,
    directed: bool = True,
) -> mgp.Record(node=mgp.Vertex, partition_id=int):
    graph = MemgraphIgraph(ctx=ctx, directed=directed)

    partition_vertices, _ = graph.mincut(source=source, target=target, capacity=capacity)

    return [
        mgp.Record(node=node, partition_id=i)
        for i, partition_nodes in enumerate(partition_vertices)
        for node in partition_nodes
    ]


@mgp.read_proc
def topological_sort(ctx: mgp.ProcCtx, mode: str = "out") -> mgp.Record(nodes=mgp.List[mgp.Vertex]):
    if mode not in [
        TopologicalSortingModes.IN.value,
        TopologicalSortingModes.OUT.value,
    ]:
        raise InvalidTopologicalSortingModeException('Mode can only be either "out" or "in"')
    if contains_cycle(ctx):
        raise TopologicalSortException("Topological sort can't be performed on graph that contains cycle!")

    graph = MemgraphIgraph(ctx=ctx, directed=True)
    sorted_nodes = graph.topological_sort(mode=mode)

    return mgp.Record(
        nodes=sorted_nodes,
    )


@mgp.read_proc
def community_leiden(
    ctx: mgp.ProcCtx,
    objective_function: str = "CPM",
    weights: mgp.Nullable[str] = None,
    resolution_parameter: float = 1.0,
    beta: float = 0.01,
    initial_membership: mgp.Nullable[mgp.Nullable[List[mgp.Nullable[int]]]] = None,
    n_iterations: int = 2,
    node_weights: mgp.Nullable[List[mgp.Nullable[float]]] = None,
) -> mgp.Record(node=mgp.Vertex, community_id=int):
    if objective_function not in [
        CommunityDetectionObjectiveFunctionOptions.CPM.value,
        CommunityDetectionObjectiveFunctionOptions.MODULARITY.value,
    ]:
        raise InvalidCommunityDetectionObjectiveFunctionException(
            'Objective function can only be "CPM" or "modularity"'
        )

    graph = MemgraphIgraph(ctx=ctx, directed=False)

    communities = graph.community_leiden(
        resolution_parameter=resolution_parameter,
        weights=weights,
        n_iterations=n_iterations,
        objective_function=objective_function,
        beta=beta,
        initial_membership=initial_membership,
        node_weights=node_weights,
    )

    return [
        mgp.Record(
            node=node,
            community_id=community_id,
        )
        for node, community_id in communities
    ]


@mgp.read_proc
def spanning_tree(
    ctx: mgp.ProcCtx, weights: mgp.Nullable[str] = None, directed: bool = False
) -> mgp.Record(tree=List[List[mgp.Vertex]]):
    graph = MemgraphIgraph(ctx=ctx, directed=directed)

    return mgp.Record(tree=graph.spanning_tree(weights=weights))


@mgp.read_proc
def shortest_path_length(
    ctx: mgp.ProcCtx,
    source: mgp.Vertex,
    target: mgp.Vertex,
    weights: mgp.Nullable[str] = None,
    directed: bool = True,
) -> mgp.Record(length=float):
    graph = MemgraphIgraph(ctx, directed=directed)
    return mgp.Record(
        length=graph.shortest_path_length(
            source=source,
            target=target,
            weights=weights,
        )
    )


@mgp.read_proc
def all_shortest_path_lengths(
    ctx: mgp.ProcCtx,
    weights: mgp.Nullable[str] = None,
    directed: bool = False,
) -> mgp.Record(src_node=mgp.Vertex, dest_node=mgp.Vertex, length=float):
    graph = MemgraphIgraph(ctx, directed=directed)
    lengths = graph.all_shortest_path_lengths(weights=weights)

    return [
        mgp.Record(
            src_node=graph.get_vertex_by_id(i),
            dest_node=graph.get_vertex_by_id(j),
            length=float(lengths[i][j]),
        )
        for i in range(len(lengths))
        for j in range(len(lengths[i]))
    ]


@mgp.read_proc
def get_shortest_path(
    ctx: mgp.ProcCtx,
    source: mgp.Vertex,
    target: mgp.Vertex,
    weights: mgp.Nullable[str] = None,
    directed: bool = True,
) -> mgp.Record(path=List[mgp.Vertex]):
    graph = MemgraphIgraph(ctx=ctx, directed=directed)

    return mgp.Record(path=graph.get_shortest_path(source=source, target=target, weights=weights))


@mgp.read_proc
def betweenness_centrality(
    ctx: mgp.ProcCtx,
    directed: bool = True,
    cutoff: int = -1,
    weights: mgp.Nullable[str] = None,
    sources: mgp.Nullable[mgp.List[mgp.Vertex]] = None,
    targets: mgp.Nullable[mgp.List[mgp.Vertex]] = None,
) -> mgp.Record(node=mgp.Vertex, betweenness=float):
    graph = MemgraphIgraph(ctx=ctx, directed=directed)
    igraph_sources = [graph.id_mappings[v.id] for v in sources] if sources else None
    igraph_targets = [graph.id_mappings[v.id] for v in targets] if targets else None
    results = graph.betweenness_centrality(
        directed=directed,
        cutoff=cutoff,
        weights=weights,
        sources=igraph_sources,
        targets=igraph_targets,
    )
    return [mgp.Record(node=node, betweenness=score) for node, score in results]


def dfs(node: mgp.Vertex, visited: Dict[int, bool], stack: Dict[int, bool]) -> bool:
    """Depth-first-search algorithm with modification.

    Args:
        node (mgp.Vertex): Current node.
        visited (Dict[int,bool]): Dictionary with all nodes id that we visited.
        stack (Dict[int,bool]): Dictionary with nodes id that we encountered while traversing a node.

    Returns:
        bool: True if there is cycle else False.
    """

    visited[node.id] = True
    stack[node.id] = True

    for edge in node.out_edges:
        neighbour = edge.to_vertex
        if not visited[neighbour.id]:
            if dfs(neighbour, visited, stack):
                return True
        elif stack[neighbour.id]:
            return True

    stack[node.id] = False
    return False


def contains_cycle(ctx: mgp.ProcCtx) -> bool:
    """Method for checking if graph contains a cycle.

    Args:
        ctx (mgp.ProcCtx): Graph

    Returns:
        bool: True if there is cycle else False
    """

    visited, stack = defaultdict(bool), defaultdict(bool)
    for node in ctx.graph.vertices:
        if not visited[node.id]:
            if dfs(node, visited, stack):
                return True
    return False
