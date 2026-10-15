import pytest
from mage.graph_coloring_module import Graph, available_colors


@pytest.fixture
def graph():
    return Graph(
        [0, 1, 2, 3, 4],
        {
            0: [(1, 2), (2, 3)],
            1: [(0, 2), (2, 2), (4, 5)],
            2: [(0, 3), (1, 2), (3, 3)],
            3: [(2, 3)],
            4: [(1, 5)],
        },
    )


def test_no_available_colors(graph):
    colors = available_colors(graph, 3, [0, 1, 0, 2, 1], 2)
    expected_colors = []
    assert sorted(colors) == sorted(expected_colors)


def test_available_colors(graph):
    colors = available_colors(graph, 3, [2, 1, 0, 2, 1], 3)
    expected_colors = [1, 2]
    assert sorted(colors) == sorted(expected_colors)
