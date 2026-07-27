import random

import pytest
from mage.graph_coloring_module import LDO, Graph, Parameter


@pytest.fixture
def set_seed():
    random.seed(42)


@pytest.fixture
def graph_1():
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


@pytest.fixture
def graph_not_connected():
    return Graph(
        [0, 1, 2, 3, 4],
        {
            0: [(1, 2), (2, 3)],
            1: [(0, 2), (2, 2)],
            2: [(0, 3), (1, 2)],
            3: [(4, 3)],
            4: [(3, 3)],
        },
    )


def test_LDO(set_seed, graph_1):
    algorithm = LDO()
    individual = algorithm.run(graph_1, {Parameter.NO_OF_COLORS: 3})

    expected_result = [0, 2, 1, 2, 0]
    assert individual.chromosome == expected_result


def test_not_connected_graph(set_seed, graph_not_connected):
    algorithm = LDO()
    individual = algorithm.run(graph_not_connected, {Parameter.NO_OF_COLORS: 3})

    expected_result = [0, 2, 1, 2, 0]
    assert individual.chromosome == expected_result


def test_empty_graph(set_seed):
    graph = Graph([], {})
    algorithm = LDO()
    individual = algorithm.run(graph, {Parameter.NO_OF_COLORS: 3})

    expected_result = []
    assert individual.chromosome == expected_result
