from itertools import chain
from math import floor, log2
from typing import Dict, List

import mgp
from mage.max_flow.bfs_weight_min_max import BFS_find_weight_min_max


@mgp.read_proc
def get_flow(
    context: mgp.ProcCtx,
    start_v: mgp.Vertex,
    end_v: mgp.Vertex,
    edge_property: str = "weight",
) -> mgp.Record(max_flow=mgp.Number):
    """
    Calculates maximum flow of graph from paths found with method
    ford_fulkerson_capacity_scaling

    :param start_v: source vertex for outgoing flow
    :param end_v: sink vertex for ingoing flow
    :param edge_property: property of edge to be used as flow capacity

    return: number value of graph's maximum flow

    The procedure can be invoked in openCypher using the following call:
    MATCH (source {id: 0}), (sink {id: 5})
    CALL max_flow.get_flow(source, sink, "weight")
    YIELD max_flow
    RETURN max_flow
    """
    paths_and_flows = ford_fulkerson_capacity_scaling(start_v, end_v, edge_property)

    max_flow = 0
    for _, flow in paths_and_flows:
        max_flow += flow

    return mgp.Record(max_flow=max_flow)


@mgp.read_proc
def get_paths(
    context: mgp.ProcCtx,
    start_v: mgp.Vertex,
    end_v: mgp.Vertex,
    edge_property: str = "weight",
) -> mgp.Record(path=mgp.Path, flow=mgp.Number):
    """
    Returns each path and its flow used in max flow of a graph found with
    ford_fulkerson_capacity_scaling

    :param start_v: source vertex for outgoing flow
    :param end_v: sink vertex for ingoing flow
    :param edge_property: property of edge to be used as flow capacity

    return: flow paths and amounts

    The procedure can be invoked in openCypher using the following call:
    MATCH (source {id: 0}), (sink {id: 5})
    CALL max_flow.get_paths(source, sink, "weight")
    YIELD path, flow
    RETURN path, flow
    """
    paths_and_flows = ford_fulkerson_capacity_scaling(start_v, end_v, edge_property)

    return [mgp.Record(path=list_to_mgp_path(context, path), flow=flow) for path, flow in paths_and_flows]


def ford_fulkerson_capacity_scaling(
    start_v: mgp.Vertex,
    end_v: mgp.Vertex,
    edge_property: str = "weight",
) -> List:
    """
    Uses Ford-Fulkerson algorithm, with capacity scaling for augmenting path
    finding. Works for positive number weight values.

    :param start_v: source vertex for outgoing flow
    :param end_v: sink vertex for ingoing flow
    :param edge_property: property of edge to be used as flow capacity

    return: list of tuples of path and flow
    """

    if not isinstance(start_v, mgp.Vertex) or not isinstance(end_v, mgp.Vertex):
        return []

    max_weight, min_weight = BFS_find_weight_min_max(start_v, edge_property)

    if max_weight <= 0:
        return []

    # delta is init as largest power of 2 smaller than max_weight
    delta = 2 ** floor(log2(max_weight))

    edge_flows = dict()
    paths_and_flows = []

    while True:
        # augmenting path is a list of interchangeable
        # VertexId and EdgeId
        augmenting_path = [start_v.id]
        flow_bottleneck = DFS_path_finding(augmenting_path, start_v, end_v, edge_property, delta, edge_flows)

        if flow_bottleneck == -1:
            if delta < min_weight:
                break
            delta //= 2
            continue

        for i, e in enumerate(augmenting_path):
            if isinstance(e, mgp.Edge):
                if augmenting_path[i - 1] == e.from_vertex.id:
                    edge_flows[e.id] = edge_flows.get(e.id, 0) + flow_bottleneck
                elif augmenting_path[i - 1] == e.to_vertex.id:
                    edge_flows[e.id] = edge_flows.get(e.id, 0) - flow_bottleneck
                else:
                    raise Exception("path is not ordered correctly")

        paths_and_flows.append((augmenting_path, flow_bottleneck))

    return paths_and_flows


def DFS_path_finding(
    path: List,
    current_v: mgp.Vertex,
    end_v: mgp.Vertex,
    edge_property: str,
    delta: mgp.Number,
    edge_flows: Dict,
) -> mgp.Number:
    """
    Finds augmenting path for max_flow algorithm using recursive DFS
    with minimum edge weight delta, as defined by capacity scaling.

    :param path: list for storing path, elements are
                 alternating mgp.VertexId and mgp.Edge
    :param delta: lower bound for path flow
    :param edge_flows: dict containing existing flows of edges

    :return: flow_bottleneck, smallest remaining capacity on the path,
             -1 if no path to end_node is found
    """

    # instead of using residual edges, we check for in_edges with flow
    for edge in chain(current_v.out_edges, current_v.in_edges):
        # skip edges without the flow property to allow heterogeneous graphs
        if edge_property not in edge.properties:
            continue

        if edge.from_vertex == current_v:
            to_v = edge.to_vertex
            remaining_capacity = edge.properties[edge_property] - edge_flows.get(edge.id, 0)
        else:
            to_v = edge.from_vertex
            remaining_capacity = edge_flows.get(edge.id, 0)

        if to_v.id in path:
            continue

        if remaining_capacity > delta:
            path.append(edge)
            path.append(to_v.id)

            if to_v.id == end_v.id:
                # found path
                return remaining_capacity

            flow_bottleneck = DFS_path_finding(path, to_v, end_v, edge_property, delta, edge_flows)
            if flow_bottleneck != -1:
                # function call found path, propagate back
                return min(remaining_capacity, flow_bottleneck)

    # no path found with this vertex, remove it and its edge
    del path[-2:]

    return -1


def list_to_mgp_path(context: mgp.ProcCtx, augmenting_path: List) -> mgp.Path:
    """
    Converts a list of mgp.VertexId and mgp.EdgeId into mgp.Path

    :param augmenting_path: List of interchangeable VertexId and EdgeId

    :return: mgp.Path structure
    """
    path = mgp.Path(context.graph.get_vertex_by_id(augmenting_path[0]))
    for _, elem in enumerate(augmenting_path, start=1):
        if isinstance(elem, mgp.Edge):
            path.expand(elem)

    return path
