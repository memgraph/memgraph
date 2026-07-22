import pytest
from mage.graph_coloring_module import ChainChunk, Graph, Individual


@pytest.fixture
def chain_chunk_population():
    graph = Graph(
        [0, 1, 2, 3, 4],
        {
            0: [(1, 2), (2, 3)],
            1: [(0, 2), (2, 2), (4, 5)],
            2: [(0, 3), (1, 2), (3, 3)],
            3: [(2, 3)],
            4: [(1, 5)],
        },
    )
    indv_1 = Individual(no_of_colors=3, graph=graph, chromosome=[1, 1, 0, 2, 0], conflict_nodes={0, 1})
    indv_2 = Individual(no_of_colors=3, graph=graph, chromosome=[1, 2, 0, 2, 1])
    indv_3 = Individual(no_of_colors=3, graph=graph, chromosome=[2, 1, 0, 2, 1], conflict_nodes={1, 4})
    indv_prev = Individual(no_of_colors=3, graph=graph, chromosome=[2, 1, 0, 0, 0], conflict_nodes={2, 3})
    indv_next = Individual(no_of_colors=3, graph=graph, chromosome=[0, 1, 2, 1, 0])
    chain_chunk = ChainChunk(graph, [indv_1, indv_2, indv_3], indv_prev, indv_next)
    return chain_chunk


def test_get_prev_individual(chain_chunk_population):
    result_indv = chain_chunk_population.get_prev_individual(2)
    expected_indv = chain_chunk_population[1]

    assert result_indv == expected_indv


def test_prev_out_of_range(chain_chunk_population):
    with pytest.raises(IndexError):
        chain_chunk_population.get_prev_individual(10)


def test_prev_individual_negative_index(chain_chunk_population):
    with pytest.raises(IndexError):
        chain_chunk_population.get_prev_individual(-2)


def test_prev_individual_of_first_item(chain_chunk_population):
    result_indv = chain_chunk_population.get_prev_individual(0)
    expected_indv = chain_chunk_population._prev_indv

    assert result_indv == expected_indv


def test_get_next_individual(chain_chunk_population):
    result_indv = chain_chunk_population.get_next_individual(1)
    expected_indv = chain_chunk_population[2]

    assert result_indv == expected_indv


def test_next_individual_last_item(chain_chunk_population):
    result_indv = chain_chunk_population.get_next_individual(2)
    expected_indv = chain_chunk_population._next_indv

    assert result_indv == expected_indv


def test_next_out_of_range(chain_chunk_population):
    with pytest.raises(IndexError):
        chain_chunk_population.get_next_individual(10)


def test_next_negative_index(chain_chunk_population):
    with pytest.raises(IndexError):
        chain_chunk_population.get_next_individual(-1)
