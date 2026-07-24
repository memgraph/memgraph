from typing import List

from mage.graph_coloring_module.graph import Graph


def available_colors(graph: Graph, no_of_colors: int, chromosome: List[int], node: int) -> List[int]:
    """A function that finds colors with which we can color
    the given node without creating a conflict. Conflict occurs
    when the same color is assigned to the two nodes connected by
    an edge. This function returns a list of available colors for
    a given node - colors that are not used to color neighbors of
    a given node."""

    used = [False for _ in range(no_of_colors)]
    for neigh in graph[node]:
        color = chromosome[neigh]
        if color != -1:
            used[color] = True

    available_colors = []
    for color in range(no_of_colors):
        if not used[color]:
            available_colors.append(color)

    return available_colors
