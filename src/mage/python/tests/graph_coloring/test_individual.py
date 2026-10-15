import pytest
from mage.graph_coloring_module import Graph, Individual


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
def conflict_individual(graph):
    indv = Individual(
        no_of_colors=3,
        graph=graph,
        chromosome=[1, 1, 1, 2, 0],
        conflict_nodes={0, 1, 2},
    )
    return indv


@pytest.fixture
def individual(graph):
    indv = Individual(no_of_colors=3, graph=graph, chromosome=[1, 2, 0, 2, 1])
    return indv


@pytest.mark.parametrize(
    "no_of_colors,graph,chromosome,expected",
    [
        (3, graph(), [1, 1, 1, 2, 0], {0, 1, 2}),
        (3, graph(), [1, 2, 0, 2, 1], set()),
        (3, graph(), [1, 1, 0, 2, 0], {0, 1}),
        (3, graph_not_connected(), [1, 1, 1, 2, 0], {0, 1, 2}),
        (3, graph_not_connected(), [1, 2, 0, 2, 1], set()),
        (3, graph_not_connected(), [1, 1, 0, 2, 2], {0, 1, 3, 4}),
    ],
)
def test_conflict_nodes(no_of_colors, graph, chromosome, expected):
    indv = Individual(no_of_colors=no_of_colors, graph=graph, chromosome=chromosome)
    result = indv.conflict_nodes
    assert result == expected


@pytest.mark.parametrize(
    "no_of_colors,graph,chromosome,expected",
    [
        (3, graph(), [1, 1, 1, 2, 0], 7),
        (3, graph(), [1, 2, 0, 2, 1], 0),
        (3, graph(), [1, 1, 0, 2, 0], 2),
        (3, graph_not_connected(), [1, 1, 1, 2, 0], 7),
        (3, graph_not_connected(), [1, 2, 0, 2, 1], 0),
        (3, graph_not_connected(), [1, 1, 0, 2, 2], 5),
    ],
)
def test_conflict_weights(no_of_colors, graph, chromosome, expected):
    indv = Individual(no_of_colors=no_of_colors, graph=graph, chromosome=chromosome)
    result = indv.conflicts_weight
    assert result == expected


@pytest.mark.parametrize(
    "no_of_colors,graph,chromosome,expected",
    [
        (3, graph(), [1, 1, 1, 2, 0], False),
        (3, graph(), [1, 2, 0, 2, 1], True),
        (3, graph(), [1, 1, 0, 2, 0], False),
        (3, graph_not_connected(), [1, 1, 1, 2, 0], False),
        (3, graph_not_connected(), [1, 2, 0, 2, 1], True),
        (3, graph_not_connected(), [1, 1, 0, 2, 2], False),
        (3, graph_not_connected(), [1, 2, 0, 2, 2], False),
    ],
)
def test_check_coloring(no_of_colors, graph, chromosome, expected):
    indv = Individual(no_of_colors=no_of_colors, graph=graph, chromosome=chromosome)
    result = indv.check_coloring()
    assert result == expected


@pytest.mark.parametrize(
    "no_of_colors,graph,chromosome, inds, colors, is_none, expected_chromosome, "
    "expected_conflicts, expected_weights, expected_coloring",
    [
        (
            3,
            graph(),
            [1, 1, 1, 2, 0],
            [2],
            [0],
            False,
            [1, 1, 0, 2, 0],
            {0, 1},
            2,
            False,
        ),
        (3, graph(), [1, 2, 0, 2, 1], [6], [0], True, None, None, None, False),
        (
            3,
            graph(),
            [1, 1, 0, 2, 0],
            [1, 3],
            [2, 0],
            False,
            [1, 2, 0, 0, 0],
            {2, 3},
            3,
            False,
        ),
        (
            3,
            graph_not_connected(),
            [1, 1, 1, 2, 0],
            [2],
            [7],
            True,
            None,
            None,
            None,
            False,
        ),
        (
            3,
            graph_not_connected(),
            [1, 2, 0, 2, 1],
            [0],
            [2],
            False,
            [2, 2, 0, 2, 1],
            {0, 1},
            2,
            False,
        ),
        (
            3,
            graph_not_connected(),
            [1, 1, 0, 2, 2],
            [0, 3],
            [2, 1],
            False,
            [2, 1, 0, 1, 2],
            set(),
            0,
            True,
        ),
        (
            3,
            graph_not_connected(),
            [1, 2, 0, 2, 2],
            [0],
            [2],
            False,
            [2, 2, 0, 2, 2],
            {0, 1, 3, 4},
            5,
            False,
        ),
    ],
)
def test_replace_units(
    no_of_colors,
    graph,
    chromosome,
    inds,
    colors,
    is_none,
    expected_chromosome,
    expected_conflicts,
    expected_weights,
    expected_coloring,
):
    indv = Individual(no_of_colors=no_of_colors, graph=graph, chromosome=chromosome)
    if is_none:
        with pytest.raises(Exception):
            new_indv = indv.replace_units(inds, colors)
    else:
        new_indv = indv.replace_units(inds, colors)
        assert new_indv.chromosome == expected_chromosome
        assert new_indv.conflict_nodes == expected_conflicts
        assert new_indv.conflicts_weight == expected_weights
        assert new_indv.check_coloring() == expected_coloring
