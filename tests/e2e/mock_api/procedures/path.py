import copy

import mgp
import mgp_mock
import test_utils


@mgp.read_proc
def compare_apis(ctx: mgp.ProcCtx) -> mgp.Record(results_dict=mgp.Map):
    mock_ctx = test_utils.get_mock_proc_ctx(is_write=False)
    results = dict()

    START_ID = 0
    start_vertex = test_utils.get_vertex(ctx, permanent_id=START_ID)
    mock_start_vertex = mock_ctx.graph.get_vertex_by_id(START_ID)
    path = mgp.Path(start_vertex)
    mock_path = mgp_mock.Path(mock_start_vertex)

    results["is_valid"] = test_utils.all_equal(
        path.is_valid(),
        mock_path.is_valid(),
        True,
    )

    EDGE_ID = 0
    edge_to_add = test_utils.get_edge(ctx, permanent_id=EDGE_ID)
    mock_edge_to_add = test_utils.get_mock_edge(mock_ctx, id=EDGE_ID)
    path.expand(edge_to_add)
    mock_path.expand(mock_edge_to_add)
    results["expand"] = test_utils.all_equal(
        (len(path.vertices), len(path.edges)),
        (len(mock_path.vertices), len(mock_path.edges)),
        (2, 1),
    )

    path.pop()
    mock_path.pop()
    results["pop"] = test_utils.all_equal(
        (len(path.vertices), len(path.edges)),
        (len(mock_path.vertices), len(mock_path.edges)),
        (1, 0),
    )
    path.expand(edge_to_add)
    mock_path.expand(mock_edge_to_add)

    NEXT_ID = 1
    results["vertices"] = test_utils.all_equal(
        all(isinstance(vertex, mgp.Vertex) for vertex in path.vertices),
        all(isinstance(vertex, mgp_mock.Vertex) for vertex in mock_path.vertices),
        True,
    ) and test_utils.all_equal(
        [vertex.properties["permanent_id"] for vertex in path.vertices],
        [vertex.properties["permanent_id"] for vertex in mock_path.vertices],
        [START_ID, NEXT_ID],
    )

    results["edges"] = test_utils.all_equal(
        all(isinstance(edge, mgp.Edge) for edge in path.edges),
        all(isinstance(edge, mgp_mock.Edge) for edge in mock_path.edges),
        True,
    ) and test_utils.all_equal(
        [edge.properties["permanent_id"] for edge in path.edges],
        [edge.properties["permanent_id"] for edge in mock_path.edges],
        [0],
    )

    path_copy = copy.copy(path)
    mock_path_copy = copy.copy(mock_path)
    results["__copy__"] = test_utils.all_equal(
        path_copy.is_valid(),
        mock_path_copy.is_valid(),
        True,
    )

    return mgp.Record(results_dict=results)
