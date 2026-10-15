from collections import deque

import mgp


def BFS_find_weight_min_max(start_v: mgp.Vertex, edge_property: str) -> mgp.Number:
    """
    Breadth-first search for finding the largest and smallest edge weight,
    largest being used for capacity scaling, and smallest for lower bound

    :param start_v: starting vertex
    :param edge_property: str denoting the edge property used as weight

    :return: Number, the largest value of edge_property in graph
    """

    next_queue = deque([start_v])
    visited = set()
    max_weight = 0
    min_weight = float("Inf")

    while next_queue:
        current_v = next_queue.popleft()
        visited.add(current_v)

        for e in current_v.out_edges:
            # if there are edges without the given property, we ignore them,
            # in order to support heterogeneous graphs
            if edge_property not in e.properties:
                continue

            max_weight = max(max_weight, e.properties[edge_property])
            min_weight = min(min_weight, e.properties[edge_property])

            if e.to_vertex not in visited:
                next_queue.append(e.to_vertex)

    return max_weight, min_weight
