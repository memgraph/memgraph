import math

import pytest
from mage.graph_coloring_module import ChainChunk, ChainPopulation, Graph, Individual


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


@pytest.fixture
def chain_population(graph):
    indv_1 = Individual(no_of_colors=3, graph=graph, chromosome=[1, 1, 0, 2, 0], conflict_nodes={0, 1})
    indv_2 = Individual(no_of_colors=3, graph=graph, chromosome=[1, 2, 0, 2, 1])
    indv_3 = Individual(no_of_colors=3, graph=graph, chromosome=[2, 1, 0, 2, 1], conflict_nodes={1, 4})
    population = ChainPopulation(graph, [indv_1, indv_2, indv_3])
    return population


@pytest.fixture
def chain_chunk_population(graph):
    indv_1 = Individual(no_of_colors=3, graph=graph, chromosome=[1, 1, 0, 2, 0], conflict_nodes={0, 1})
    indv_2 = Individual(no_of_colors=3, graph=graph, chromosome=[1, 2, 0, 2, 1])
    indv_3 = Individual(no_of_colors=3, graph=graph, chromosome=[2, 1, 0, 2, 1], conflict_nodes={1, 4})
    indv_prev = Individual(no_of_colors=3, graph=graph, chromosome=[2, 1, 0, 0, 0], conflict_nodes={2, 3})
    indv_next = Individual(no_of_colors=3, graph=graph, chromosome=[0, 1, 2, 1, 0])
    chain_chunk = ChainChunk(graph, [indv_1, indv_2, indv_3], indv_prev, indv_next)
    return chain_chunk


def error_func(graph, individual):
    return individual.conflicts_weight


def test_chain_population_best_individual(chain_population, chain_chunk_population):
    result_indv = chain_population.best_individual(error_func)
    expected_indv = chain_population[1]
    assert result_indv == expected_indv


def test_chain_population_worst_individual(chain_population):
    result_indv = chain_population.worst_individual(error_func)
    expected_indv = chain_population[2]
    assert result_indv == expected_indv


def test_chain_population_min_error(chain_population):
    result_indv = chain_population.min_error(error_func)
    expected_indv = 0
    assert math.fabs(result_indv - expected_indv) < 1e-5


def test_chain_population_max_error(chain_population):
    result_indv = chain_population.max_error(error_func)
    expected_indv = 5
    assert math.fabs(result_indv - expected_indv) < 1e-5


def test_chain_population_mean_conflicts_weights(chain_population):
    result_indv = chain_population.mean_conflicts_weight
    expected_indv = 7 / 3
    assert math.fabs(result_indv - expected_indv) < 1e-5


def test_chain_population_sum_conflicts_weight(chain_population):
    result_indv = chain_population.sum_conflicts_weight
    expected_indv = 7
    assert math.fabs(result_indv - expected_indv) < 1e-5


def test_chain_population_correlation(chain_population):
    result = chain_population.correlation
    expected = [2, 2, 2]
    assert expected == result


def test_set_individual(chain_population, graph):
    new_indv = Individual(no_of_colors=3, graph=graph, chromosome=[1, 2, 2, 2, 1])
    chain_population.set_individual(1, new_indv, [2])
    assert chain_population.correlation == [-2, -2, 2]
    assert chain_population.sum_conflicts_weight == 12
    assert chain_population.cumulative_correlation == -2
    assert chain_population.mean_conflicts_weight == 4
