import math
import random

import pytest
from mage.graph_coloring_module import ChainPopulation, ConflictError, Graph, Individual, Parameter


@pytest.fixture
def set_seed():
    random.seed(42)


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


@pytest.fixture
def chain_population():
    g = graph()
    indv_1 = Individual(no_of_colors=3, graph=g, chromosome=[1, 1, 0, 2, 0], conflict_nodes={0, 1})
    indv_2 = Individual(no_of_colors=3, graph=g, chromosome=[1, 2, 0, 2, 1])
    indv_3 = Individual(no_of_colors=3, graph=g, chromosome=[2, 1, 0, 2, 1], conflict_nodes={1, 4})
    population = ChainPopulation(g, [indv_1, indv_2, indv_3])
    return population


@pytest.mark.parametrize(
    "graph, no_of_colors, chromosome, expected_error",
    [
        (graph(), 3, [1, 1, 0, 2, 0], 2),
        (graph(), 3, [1, 1, 2, 2, 0], 5),
        (graph_not_connected(), 3, [1, 1, 0, 2, 0], 2),
    ],
)
def test_individual_error_no_setting(set_seed, graph, no_of_colors, chromosome, expected_error):
    individual = Individual(no_of_colors=no_of_colors, graph=graph, chromosome=chromosome)
    error = ConflictError().individual_err(graph, individual)
    assert error == expected_error


def test_population_error(set_seed, chain_population):
    error = ConflictError().population_err(
        graph,
        chain_population,
        {Parameter.CONFLICT_ERR_ALPHA: 0.5, Parameter.CONFLICT_ERR_BETA: 0.5},
    )

    expected_error = 6.5
    assert math.fabs(error - expected_error) < 1e-5


def test_delta_error_function():
    g = graph()
    error = ConflictError().delta(
        g,
        Individual(no_of_colors=3, graph=g, chromosome=[1, 2, 0, 2, 1]),
        Individual(no_of_colors=3, graph=g, chromosome=[1, 2, 2, 2, 1]),
        -8,
        {Parameter.CONFLICT_ERR_ALPHA: 0.5, Parameter.CONFLICT_ERR_BETA: 0.5},
    )

    expected_error = -1.5
    assert math.fabs(error - expected_error) < 1e-5
