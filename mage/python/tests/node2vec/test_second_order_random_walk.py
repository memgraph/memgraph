from typing import List

import numpy as np
import pytest
from mage.node2vec.graph import GraphHolder
from mage.node2vec.second_order_random_walk import SecondOrderRandomWalk

P = 2
Q = 0.5
WALK_LENGTH = 2
NUM_WALKS = 3
EDGES_WEIGHTS_DICT = {
    (0, 1): 1.5,
    (0, 2): 3,
    (0, 4): 4.1,
    (1, 5): 1.7,
    (1, 6): 2.6,
    (2, 5): 1.8,
    (3, 0): 1.9,
    (4, 6): 10,
    (7, 1): 21,
    (7, 5): 21,
    (7, 6): 13,
    (0, 5): 14,
    (6, 0): 17,
}


def normalize(array: List[float]):
    return np.array(array) / sum(array)


UNDIRECT_GRAPH_EDGE_TRANSITION_PROBS = {
    (0, 1): normalize(
        [
            EDGES_WEIGHTS_DICT[(0, 1)] * 1 / P,
            EDGES_WEIGHTS_DICT[(1, 5)] * 1,
            EDGES_WEIGHTS_DICT[(1, 6)] * 1,
            EDGES_WEIGHTS_DICT[(7, 1)] * 1 / Q,
        ]
    ),
    (3, 0): normalize(
        [
            EDGES_WEIGHTS_DICT[(0, 1)] * 1 / Q,
            EDGES_WEIGHTS_DICT[(0, 2)] * 1 / Q,
            EDGES_WEIGHTS_DICT[(3, 0)] * 1 / P,
            EDGES_WEIGHTS_DICT[(0, 4)] * 1 / Q,
            EDGES_WEIGHTS_DICT[(0, 5)] * 1 / Q,
            EDGES_WEIGHTS_DICT[(6, 0)] * 1 / Q,
        ]
    ),
}

DIRECT_GRAPH_EDGE_TRANSITION_PROBS = {
    (0, 1): normalize([EDGES_WEIGHTS_DICT[(1, 5)] * 1 / Q, EDGES_WEIGHTS_DICT[(1, 6)] * 1]),
    (3, 0): normalize(
        [
            EDGES_WEIGHTS_DICT[(0, 1)] * 1 / Q,
            EDGES_WEIGHTS_DICT[(0, 2)] * 1 / Q,
            EDGES_WEIGHTS_DICT[(0, 4)] * 1 / Q,
            EDGES_WEIGHTS_DICT[(0, 5)] * 1 / Q,
        ]
    ),
}

UNDIRECT_GRAPH_FIRST_PASS_TRANSITION_PROBS = {
    1: normalize(
        [
            EDGES_WEIGHTS_DICT[(0, 1)],
            EDGES_WEIGHTS_DICT[(1, 5)],
            EDGES_WEIGHTS_DICT[(1, 6)],
            EDGES_WEIGHTS_DICT[(7, 1)],
        ]
    ),
    0: normalize(
        [
            EDGES_WEIGHTS_DICT[(0, 1)],
            EDGES_WEIGHTS_DICT[(0, 2)],
            EDGES_WEIGHTS_DICT[(3, 0)],
            EDGES_WEIGHTS_DICT[(0, 4)],
            EDGES_WEIGHTS_DICT[(0, 5)],
            EDGES_WEIGHTS_DICT[(6, 0)],
        ]
    ),
}

DIRECT_GRAPH_FIRST_PASS_TRANSITION_PROBS = {
    1: normalize([EDGES_WEIGHTS_DICT[(1, 5)], EDGES_WEIGHTS_DICT[(1, 6)]]),
    0: normalize(
        [
            EDGES_WEIGHTS_DICT[(0, 1)],
            EDGES_WEIGHTS_DICT[(0, 2)],
            EDGES_WEIGHTS_DICT[(0, 4)],
            EDGES_WEIGHTS_DICT[(0, 5)],
        ]
    ),
}


def get_basic_graph(dataset, is_directed) -> GraphHolder:
    return GraphHolder(dataset, is_directed)


def get_transition_probs(is_directed):
    if is_directed:
        return DIRECT_GRAPH_EDGE_TRANSITION_PROBS
    return UNDIRECT_GRAPH_EDGE_TRANSITION_PROBS


def get_first_pass_transition_probs(is_directed):
    if is_directed:
        return DIRECT_GRAPH_FIRST_PASS_TRANSITION_PROBS
    return UNDIRECT_GRAPH_FIRST_PASS_TRANSITION_PROBS


def same_array_values(array_1: List[float], array_2: List[float], absolute_tolerance=1e-5) -> bool:
    return np.all(
        np.isclose(
            array_1,
            array_2,
            atol=absolute_tolerance,
        )
    )


@pytest.mark.parametrize(
    "dataset, is_directed",
    [
        (EDGES_WEIGHTS_DICT, True),
        (EDGES_WEIGHTS_DICT, False),
    ],
)
def test_graph_transition_probs(dataset, is_directed):
    basic_graph = get_basic_graph(dataset, is_directed)

    graph_transition_probs = get_transition_probs(is_directed)

    second_order_random_walk = SecondOrderRandomWalk(p=P, q=Q, walk_length=WALK_LENGTH, num_walks=NUM_WALKS)
    second_order_random_walk.set_graph_transition_probs(basic_graph)

    for edge in graph_transition_probs:
        calculated_transition_probs = basic_graph.get_edge_transition_probs(edge)
        correct_transition_probs = graph_transition_probs.get(edge)
        assert same_array_values(calculated_transition_probs, correct_transition_probs)


@pytest.mark.parametrize(
    "dataset, is_directed",
    [
        (EDGES_WEIGHTS_DICT, True),
        (EDGES_WEIGHTS_DICT, False),
    ],
)
def test_graph_first_pass_transition_probs(dataset, is_directed):
    basic_graph = get_basic_graph(dataset, is_directed)

    graph_transition_probs = get_first_pass_transition_probs(is_directed)

    second_order_random_walk = SecondOrderRandomWalk(p=P, q=Q, walk_length=WALK_LENGTH, num_walks=NUM_WALKS)
    second_order_random_walk.set_first_pass_transition_probs(basic_graph)

    for node in graph_transition_probs:
        calculated_transition_probs = basic_graph.get_node_first_pass_transition_probs(node)
        correct_transition_probs = graph_transition_probs.get(node)
        assert same_array_values(calculated_transition_probs, correct_transition_probs)


@pytest.mark.parametrize(
    "dataset, is_directed",
    [
        (EDGES_WEIGHTS_DICT, True),
        (EDGES_WEIGHTS_DICT, False),
    ],
)
def test_second_order_walks(dataset, is_directed):
    basic_graph = get_basic_graph(dataset, is_directed)
    second_order_random_walk = SecondOrderRandomWalk(p=P, q=Q, walk_length=WALK_LENGTH, num_walks=NUM_WALKS)

    walks = second_order_random_walk.sample_node_walks(basic_graph)

    assert len(walks) == second_order_random_walk.num_walks * len(basic_graph.nodes)

    for walk in walks:
        for i in range(1, len(walk)):
            assert basic_graph.has_edge(walk[i - 1], walk[i])
