import typing

import mgp
import mgp_mock
import test_utils


@mgp.read_proc
def compare_apis(ctx: mgp.ProcCtx) -> mgp.Record(results_dict=mgp.Map):
    mock_ctx = test_utils.get_mock_proc_ctx(is_write=False)
    results = dict()

    ID = 1

    target_vertex = test_utils.get_vertex(ctx, permanent_id=ID)
    target_mock_vertex = mock_ctx.graph.get_vertex_by_id(ID)

    results["is_valid"] = test_utils.all_equal(
        target_vertex.is_valid(),
        target_mock_vertex.is_valid(),
        True,
    )

    results["underlying_graph_is_mutable"] = test_utils.all_equal(
        target_vertex.underlying_graph_is_mutable(),
        target_mock_vertex.underlying_graph_is_mutable(),
        False,
    )

    results["id"] = test_utils.all_equal(
        isinstance(target_vertex.id, int),
        isinstance(target_mock_vertex.id, int),
        True,
    )

    results["labels"] = test_utils.all_equal(
        isinstance(target_vertex.labels, typing.Tuple),
        isinstance(target_mock_vertex.labels, typing.Tuple),
        True,
    ) and test_utils.all_equal(
        {label.name for label in target_vertex.labels},
        {mock_label.name for mock_label in target_mock_vertex.labels},
        {"Team"},
    )

    results["properties"] = test_utils.all_equal(
        isinstance(target_vertex.properties, mgp.Properties),
        isinstance(target_mock_vertex.properties, mgp_mock.Properties),
        True,
    ) and test_utils.all_equal(
        {prop for prop in target_vertex.properties},
        {mock_prop for mock_prop in target_mock_vertex.properties},
        {"name", "permanent_id"},
    )

    results["in_edges"] = test_utils.all_equal(
        all(isinstance(edge, mgp.Edge) for edge in target_vertex.in_edges),
        all(isinstance(edge, mgp_mock.Edge) for edge in target_mock_vertex.in_edges),
        True,
    ) and test_utils.all_equal(
        {edge.properties["permanent_id"] for edge in target_vertex.in_edges},
        {edge.properties["permanent_id"] for edge in target_mock_vertex.in_edges},
        {0, 9, 15, 37},
    )

    results["out_edges"] = test_utils.all_equal(
        all(isinstance(edge, mgp.Edge) for edge in target_vertex.out_edges),
        all(isinstance(edge, mgp_mock.Edge) for edge in target_mock_vertex.out_edges),
        True,
    ) and test_utils.all_equal(
        {edge.properties["permanent_id"] for edge in target_vertex.out_edges},
        {edge.properties["permanent_id"] for edge in target_mock_vertex.out_edges},
        {4, 5, 6, 7, 8},
    )

    ID_2 = 2

    target_vertex_2 = test_utils.get_vertex(ctx, permanent_id=ID_2)
    target_mock_vertex_2 = mock_ctx.graph.get_vertex_by_id(ID_2)

    results["__eq__"] = test_utils.all_equal(
        target_vertex == target_vertex,
        target_mock_vertex == target_mock_vertex,
        True,
    ) and test_utils.all_equal(
        target_vertex == target_vertex_2,
        target_mock_vertex == target_mock_vertex_2,
        False,
    )

    results["__ne__"] = test_utils.all_equal(
        target_vertex != target_vertex_2,
        target_mock_vertex != target_mock_vertex_2,
        True,
    ) and test_utils.all_equal(
        target_vertex != target_vertex,
        target_mock_vertex != target_mock_vertex,
        False,
    )

    return mgp.Record(results_dict=results)
