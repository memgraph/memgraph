import mgp
import mgp_mock
import test_utils


@mgp.read_proc
def compare_apis(ctx: mgp.ProcCtx) -> mgp.Record(results_dict=mgp.Map):
    mock_ctx = test_utils.get_mock_proc_ctx(is_write=False)
    results = dict()

    TARGET_EDGE_1_ID = 9
    TARGET_EDGE_2_ID = 37

    target_edge_1 = test_utils.get_edge(ctx, permanent_id=TARGET_EDGE_1_ID)
    target_edge_2 = test_utils.get_edge(ctx, permanent_id=TARGET_EDGE_2_ID)
    target_mock_edge_1 = test_utils.get_mock_edge(mock_ctx, id=TARGET_EDGE_1_ID)
    target_mock_edge_2 = test_utils.get_mock_edge(mock_ctx, id=TARGET_EDGE_2_ID)

    results["is_valid"] = test_utils.all_equal(
        target_edge_1.is_valid(),
        target_mock_edge_1.is_valid(),
        True,
    )

    results["underlying_graph_is_mutable"] = test_utils.all_equal(
        target_edge_1.underlying_graph_is_mutable(),
        target_mock_edge_1.underlying_graph_is_mutable(),
        False,
    )

    results["id"] = test_utils.all_equal(
        isinstance(target_edge_1.id, int),
        isinstance(target_mock_edge_1.id, int),
        True,
    )

    results["type"] = test_utils.all_equal(
        target_edge_1.type.name,
        target_mock_edge_1.type.name,
        "HAS_TEAM",
    )

    results["from_vertex"] = test_utils.all_equal(
        isinstance(target_edge_1.from_vertex, mgp.Vertex),
        isinstance(target_mock_edge_1.from_vertex, mgp_mock.Vertex),
        True,
    )

    results["to_vertex"] = test_utils.all_equal(
        isinstance(target_edge_1.to_vertex, mgp.Vertex),
        isinstance(target_mock_edge_1.to_vertex, mgp_mock.Vertex),
        True,
    )

    results["properties"] = test_utils.all_equal(
        isinstance(target_edge_1.properties, mgp.Properties),
        isinstance(target_mock_edge_1.properties, mgp_mock.Properties),
        True,
    ) and test_utils.all_equal(
        {prop.name: prop.value for prop in target_edge_1.properties.items()},
        {prop.name: prop.value for prop in target_mock_edge_1.properties.items()},
        {"permanent_id": 9},
    )

    results["__eq__"] = test_utils.all_equal(
        target_edge_1 == target_edge_1,
        target_mock_edge_1 == target_mock_edge_1,
        True,
    ) and test_utils.all_equal(
        target_edge_1 != target_edge_1,
        target_mock_edge_1 != target_mock_edge_1,
        False,
    )

    results["__ne__"] = test_utils.all_equal(
        target_edge_1 != target_edge_2,
        target_mock_edge_1 != target_mock_edge_2,
        True,
    ) and test_utils.all_equal(
        target_edge_1 == target_edge_2,
        target_mock_edge_1 == target_mock_edge_2,
        False,
    )

    return mgp.Record(results_dict=results)
