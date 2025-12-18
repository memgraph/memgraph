import random

import pytest
from mage.graph_coloring_module import (
  Graph,
  Individual,
  MISMutation,
  MultipleMutation,
  Parameter,
  RandomMutation,
  SimpleMutation,
)


@pytest.fixture
def set_seed():
    random.seed(42)


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


def test_mis_mutation(set_seed, graph):
    individual = Individual(no_of_colors=3, graph=graph, chromosome=[0, 1, 0, 2, 0])
    mutated_indv, nodes = MISMutation().mutate(graph, individual)

    expected_mutated_indv_chromosome = [0, 2, 0, 2, 0]
    expected_nodes = [3, 1]
    assert mutated_indv.chromosome == expected_mutated_indv_chromosome
    assert sorted(nodes) == sorted(expected_nodes)


def test_multiple_mutation(set_seed, graph):
    individual = Individual(no_of_colors=3, graph=graph, chromosome=[0, 1, 0, 2, 0])
    mutated_indv, nodes = MultipleMutation().mutate(
        graph,
        individual,
        {Parameter.MULTIPLE_MUTATION_NODES_NO_OF_NODES: 2, Parameter.NO_OF_COLORS: 3},
    )

    expected_mutated_indv_chromosome = [1, 1, 0, 2, 0]
    expected_nodes = [0, 0]
    assert mutated_indv.chromosome == expected_mutated_indv_chromosome
    assert sorted(nodes) == sorted(expected_nodes)


def test_random_mutation_no_conflict(set_seed, graph):
    individual = Individual(no_of_colors=3, graph=graph, chromosome=[0, 1, 2, 1, 0])
    mutated_indv, nodes = RandomMutation().mutate(
        graph,
        individual,
        {
            Parameter.RANDOM_MUTATION_PROBABILITY: 0.5,
            Parameter.NO_OF_COLORS: 3,
        },
    )
    expected_mutated_indv_chromosome = [0, 1, 2, 1, 0]
    expected_nodes = []
    assert mutated_indv.chromosome == expected_mutated_indv_chromosome
    assert sorted(nodes) == sorted(expected_nodes)


def test_random_mutation(set_seed, graph):
    individual = Individual(no_of_colors=3, graph=graph, chromosome=[1, 1, 0, 2, 0], conflict_nodes={0, 1})
    mutated_indv, nodes = RandomMutation().mutate(
        graph,
        individual,
        {
            Parameter.RANDOM_MUTATION_PROBABILITY: 0.5,
            Parameter.NO_OF_COLORS: 3,
        },
    )
    expected_mutated_indv_chromosome = [2, 1, 0, 2, 0]
    expected_nodes = [0]
    assert mutated_indv.chromosome == expected_mutated_indv_chromosome
    assert sorted(nodes) == sorted(expected_nodes)


def test_simple_no_conflict(set_seed, graph):
    individual = Individual(no_of_colors=3, graph=graph, chromosome=[0, 1, 2, 1, 0])
    mutated_indv, nodes = SimpleMutation().mutate(graph, individual)
    expected_mutated_indv_chromosome = [0, 1, 2, 1, 0]
    expected_nodes = []
    assert mutated_indv.chromosome == expected_mutated_indv_chromosome
    assert sorted(nodes) == sorted(expected_nodes)


def test_simple_mutation(set_seed, graph):
    individual = Individual(no_of_colors=3, graph=graph, chromosome=[1, 1, 0, 2, 0], conflict_nodes={0, 1})
    mutated_indv, nodes = SimpleMutation().mutate(graph, individual)
    expected_mutated_indv_chromosome = [0, 1, 0, 2, 0]
    expected_nodes = [0]
    assert mutated_indv.chromosome == expected_mutated_indv_chromosome
    assert sorted(nodes) == sorted(expected_nodes)
