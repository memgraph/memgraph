from typing import Any, List, Tuple

import pytest
from mage.graph_coloring_module import Graph


@pytest.mark.parametrize(
    "node, neighs",
    [(0, [1, 2]), (1, [0, 3, 4]), (2, [0, 4]), (3, [1, 4]), (4, [1, 2, 3])],
)
def test_correct_get_neighbors(graph: Graph, node: int, neighs: List[int]) -> None:
    for n in graph.neighbors(node):
        assert n in neighs
    for n in neighs:
        assert n in graph.neighbors(node)


@pytest.mark.parametrize(
    "node, neighs",
    [(0, [1, 2]), (1, [0, 3, 4]), (2, [0, 4]), (3, [1, 4]), (4, [1, 2, 3])],
)
def test_correct_get_neighbors_with_mapping(graph_string_labels: Graph, node: int, neighs: List[int]) -> None:
    for n in graph_string_labels.neighbors(node):
        assert n in neighs
    for n in neighs:
        assert n in graph_string_labels.neighbors(node)


@pytest.mark.parametrize(
    "node, weight_nodes",
    [
        (0, [(1, 2), (2, 1)]),
        (1, [(0, 2), (3, 3), (4, 1)]),
        (2, [(0, 1), (4, 4)]),
        (3, [(1, 3), (4, 1)]),
        (4, [(1, 1), (2, 4), (3, 1)]),
    ],
)
def test_correct_get_weighted_neighbors(graph: Graph, node: int, weight_nodes: List[Tuple[int, float]]) -> None:
    for n in graph.weighted_neighbors(node):
        assert n in weight_nodes
    for n in weight_nodes:
        assert n in graph.weighted_neighbors(node)


@pytest.mark.parametrize("node_1, node_2, weight", [(0, 1, 2), (1, 0, 2), (2, 4, 4), (3, 1, 3), (0, 4, 0)])
def test_correct_get_weight(graph: Graph, node_1: int, node_2: int, weight: float) -> None:
    assert graph.weight(node_1, node_2) == weight


@pytest.mark.parametrize(
    "node, label",
    [
        (0, "0"),
        (1, "1"),
        (2, "2"),
    ],
)
def test_correct_get_label(graph_string_labels: Graph, node: int, label: Any) -> None:
    assert label == graph_string_labels.label(node)


def test_correct_number_of_nodes(graph: Graph) -> None:
    assert 5 == graph.number_of_nodes()


def test_correct_number_of_edges(graph: Graph) -> None:
    assert 6 == graph.number_of_edges()


def test_correct_length_of_graph(graph: Graph) -> None:
    assert 5 == len(graph)


@pytest.fixture
def graph():
    nodes = [0, 1, 2, 3, 4]
    adj = {
        0: [(1, 2), (2, 1)],
        1: [(0, 2), (3, 3), (4, 1)],
        2: [(0, 1), (4, 4)],
        3: [(1, 3), (4, 1)],
        4: [(1, 1), (2, 4), (3, 1)],
    }
    return Graph(nodes, adj)


@pytest.fixture
def graph_string_labels():
    nodes = ["0", "1", "2", "3", "4"]
    adj = {
        "0": [("1", 2), ("2", 1)],
        "1": [("0", 2), ("3", 3), ("4", 1)],
        "2": [("0", 1), ("4", 4)],
        "3": [("1", 3), ("4", 1)],
        "4": [("1", 1), ("2", 4), ("3", 1)],
    }
    return Graph(nodes, adj)
