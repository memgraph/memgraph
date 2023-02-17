from itertools import groupby

import _mgp_mock
import mgp
import mgp_mock
import networkx as nx


def all_equal(*args):
    """Returns True if all the elements are equal to each other
    (source: https://docs.python.org/3/library/itertools.html#itertools-recipes)"""
    g = groupby(args)
    return next(g, True) and not next(g, False)


def get_mock_proc_ctx(is_write: bool) -> mgp_mock.ProcCtx:
    GRAPH_DATA = [
        (0, 1, 0),
        (5, 1, 9),
        (5, 1, 37),
        (10, 1, 15),
        (1, 2, 4),
        (1, 3, 5),
        (1, 4, 6),
        (0, 5, 1),
        (10, 5, 16),
        (22, 5, 33),
        (1, 6, 7),
        (6, 7, 12),
        (11, 7, 18),
        (13, 7, 26),
        (26, 7, 35),
        (6, 8, 13),
        (26, 8, 36),
        (0, 9, 2),
        (10, 9, 17),
        (22, 9, 34),
        (1, 11, 8),
        (9, 12, 14),
        (14, 13, 27),
        (0, 14, 3),
        (5, 14, 11),
        (12, 14, 22),
        (13, 15, 23),
        (13, 16, 24),
        (13, 17, 25),
        (11, 18, 19),
        (11, 19, 20),
        (11, 20, 21),
        (5, 21, 10),
        (22, 21, 32),
        (21, 23, 28),
        (21, 24, 29),
        (21, 25, 30),
        (21, 26, 31),
    ]
    NODE_INFO = {
        0: {"labels": "Person", "name": "Peter", "surname": "Yang", "permanent_id": 0},
        1: {"labels": "Team", "name": "Engineering", "permanent_id": 1},
        2: {"labels": "Repository", "name": "Memgraph", "permanent_id": 2},
        3: {"labels": "Repository", "name": "MAGE", "permanent_id": 3},
        4: {"labels": "Repository", "name": "GQLAlchemy", "permanent_id": 4},
        5: {"labels": "Company:Startup", "name": "Memgraph", "permanent_id": 5},
        6: {"labels": "File", "name": "welcome_to_engineering.txt", "permanent_id": 6},
        7: {"labels": "Storage", "name": "Google Drive", "permanent_id": 7},
        8: {"labels": "Storage", "name": "Notion", "permanent_id": 8},
        9: {"labels": "File", "name": "welcome_to_memgraph.txt", "permanent_id": 9},
        10: {"labels": "Person", "name": "Carl", "permanent_id": 10},
        11: {"labels": "Folder", "name": "engineering_folder", "permanent_id": 11},
        12: {"labels": "Person", "name": "Anna", "permanent_id": 12},
        13: {"labels": "Folder", "name": "operations_folder", "permanent_id": 13},
        14: {"labels": "Team", "name": "Operations", "permanent_id": 14},
        15: {"labels": "File", "name": "operations101.txt", "permanent_id": 15},
        16: {"labels": "File", "name": "expenses2022.csv", "permanent_id": 16},
        17: {"labels": "File", "name": "salaries2022.csv", "permanent_id": 17},
        18: {"labels": "File", "name": "engineering101.txt", "permanent_id": 18},
        19: {"labels": "File", "name": "working_with_github.txt", "permanent_id": 19},
        20: {"labels": "File", "name": "working_with_notion.txt", "permanent_id": 20},
        21: {"labels": "Team", "name": "Marketing", "permanent_id": 21},
        22: {"labels": "Person", "name": "Julie", "permanent_id": 22},
        23: {"labels": "Account", "name": "Facebook", "permanent_id": 23},
        24: {"labels": "Account", "name": "LinkedIn", "permanent_id": 24},
        25: {"labels": "Account", "name": "HackerNews", "permanent_id": 25},
        26: {"labels": "File", "name": "welcome_to_marketing.txt", "permanent_id": 26},
    }
    EDGE_INFO = {
        (0, 1, 0): {"type": "IS_PART_OF", "permanent_id": 0},
        (0, 5, 1): {"type": "IS_PART_OF", "permanent_id": 1},
        (0, 9, 2): {"type": "HAS_ACCESS_TO", "permanent_id": 2},
        (0, 14, 3): {"type": "IS_PART_OF", "permanent_id": 3},
        (1, 2, 4): {"type": "HAS_ACCESS_TO", "permanent_id": 4},
        (1, 3, 5): {"type": "HAS_ACCESS_TO", "permanent_id": 5},
        (1, 4, 6): {"type": "HAS_ACCESS_TO", "permanent_id": 6},
        (1, 6, 7): {"type": "HAS_ACCESS_TO", "permanent_id": 7},
        (1, 11, 8): {"type": "HAS_ACCESS_TO", "permanent_id": 8},
        (5, 1, 9): {"type": "HAS_TEAM", "permanent_id": 9},
        (5, 1, 37): {"type": "HAS_TEAM_2", "importance": "HIGH", "permanent_id": 37},
        (5, 14, 11): {"type": "HAS_TEAM", "permanent_id": 11},
        (5, 21, 10): {"type": "HAS_TEAM", "permanent_id": 10},
        (6, 7, 12): {"type": "IS_STORED_IN", "permanent_id": 12},
        (6, 8, 13): {"type": "IS_STORED_IN", "permanent_id": 13},
        (9, 12, 14): {"type": "CREATED_BY", "permanent_id": 14},
        (10, 1, 15): {"type": "IS_PART_OF", "permanent_id": 15},
        (10, 5, 16): {"type": "IS_PART_OF", "permanent_id": 16},
        (10, 9, 17): {"type": "HAS_ACCESS_TO", "permanent_id": 17},
        (11, 7, 18): {"type": "IS_STORED_IN", "permanent_id": 18},
        (11, 18, 19): {"type": "HAS_ACCESS_TO", "permanent_id": 19},
        (11, 19, 20): {"type": "HAS_ACCESS_TO", "permanent_id": 20},
        (11, 20, 21): {"type": "HAS_ACCESS_TO", "permanent_id": 21},
        (12, 14, 22): {"type": "IS_PART_OF", "permanent_id": 22},
        (13, 7, 26): {"type": "IS_STORED_IN", "permanent_id": 26},
        (13, 15, 23): {"type": "HAS_ACCESS_TO", "permanent_id": 23},
        (13, 16, 24): {"type": "HAS_ACCESS_TO", "permanent_id": 24},
        (13, 17, 25): {"type": "HAS_ACCESS_TO", "permanent_id": 25},
        (14, 13, 27): {"type": "HAS_ACCESS_TO", "permanent_id": 27},
        (21, 23, 28): {"type": "HAS_ACCESS_TO", "permanent_id": 28},
        (21, 24, 29): {"type": "HAS_ACCESS_TO", "permanent_id": 29},
        (21, 25, 30): {"type": "HAS_ACCESS_TO", "permanent_id": 30},
        (21, 26, 31): {"type": "HAS_ACCESS_TO", "permanent_id": 31},
        (22, 5, 33): {"type": "IS_PART_OF", "permanent_id": 33},
        (22, 9, 34): {"type": "HAS_ACCESS_TO", "permanent_id": 34},
        (22, 21, 32): {"type": "IS_PART_OF", "permanent_id": 32},
        (26, 7, 35): {"type": "IS_STORED_IN", "permanent_id": 35},
        (26, 8, 36): {"type": "IS_STORED_IN", "permanent_id": 36},
    }

    example_graph = nx.MultiDiGraph(GRAPH_DATA)
    nx.set_node_attributes(example_graph, NODE_INFO)
    nx.set_edge_attributes(example_graph, EDGE_INFO)

    if not is_write:
        example_graph = nx.freeze(example_graph)

    return mgp_mock.ProcCtx(_mgp_mock.Graph(example_graph))


def get_vertex(ctx, permanent_id: int) -> mgp.Vertex:
    for vertex in ctx.graph.vertices:
        if vertex.properties["permanent_id"] == permanent_id:
            return vertex

    return None


def get_edge(ctx: mgp.ProcCtx, permanent_id: int) -> mgp.Edge:
    for vertex in ctx.graph.vertices:
        for edge in vertex.out_edges:
            if edge.properties["permanent_id"] == permanent_id:
                return edge

    return None


def get_mock_edge(ctx: mgp_mock.ProcCtx, id: int) -> mgp_mock.Edge:
    for vertex in ctx.graph.vertices:
        for edge in vertex.out_edges:
            if edge.id == id:
                return edge

    return None
