import mgp
import mgp_mock
import test_utils


@mgp.write_proc
def compare_apis(ctx: mgp.ProcCtx) -> mgp.Record(results_dict=mgp.Map):
    VERTEX_ID = 6

    mock_ctx = test_utils.get_mock_proc_ctx(is_write=True)
    results = dict()

    results["is_valid"] = test_utils.all_equal(
        ctx.graph.is_valid(),
        mock_ctx.graph.is_valid(),
        True,
    )

    results["get_vertex_by_id"] = test_utils.all_equal(
        test_utils.get_vertex(ctx, permanent_id=VERTEX_ID).properties["permanent_id"],
        mock_ctx.graph.get_vertex_by_id(VERTEX_ID).properties["permanent_id"],
        VERTEX_ID,
    )

    results["vertices"] = test_utils.all_equal(
        len(ctx.graph.vertices),
        len(mock_ctx.graph.vertices),
        27,
    )

    results["is_mutable"] = test_utils.all_equal(
        ctx.graph.is_mutable(),
        mock_ctx.graph.is_mutable(),
        True,
    )

    new_mock_vertex = mock_ctx.graph.create_vertex()
    new_mock_vertex_id = new_mock_vertex.id
    results["create_vertex"] = test_utils.all_equal(
        new_mock_vertex_id in [v.id for v in mock_ctx.graph.vertices],
        True,
    )

    mock_ctx.graph.delete_vertex(new_mock_vertex)
    results["delete_vertex"] = test_utils.all_equal(
        new_mock_vertex_id not in [v.id for v in mock_ctx.graph.vertices],
        True,
    )

    mock_vertex_to_delete = mock_ctx.graph.get_vertex_by_id(VERTEX_ID)
    mock_ctx.graph.detach_delete_vertex(mock_vertex_to_delete)
    results["detach_delete_vertex"] = test_utils.all_equal(
        VERTEX_ID not in [v.properties["permanent_id"] for v in mock_ctx.graph.vertices],
        True,
    )

    MAX_EDGE_ID = 37

    START_ID = 10
    END1_ID = 13
    END2_ID = 14
    start_mock_vertex, end1_mock_vertex, end2_mock_vertex = (
        mock_ctx.graph.get_vertex_by_id(START_ID),
        mock_ctx.graph.get_vertex_by_id(END1_ID),
        mock_ctx.graph.get_vertex_by_id(END2_ID),
    )
    EDGE_TYPE = "CONNECTED_TO"
    mock_edge_type = mgp_mock.EdgeType(EDGE_TYPE)
    new_mock_edge = mock_ctx.graph.create_edge(start_mock_vertex, end1_mock_vertex, mock_edge_type)
    new_mock_edge_id = new_mock_edge.id
    results["create_edge"] = test_utils.all_equal(
        new_mock_edge_id,
        MAX_EDGE_ID + 1,
    )

    mock_ctx.graph.delete_edge(new_mock_edge)
    results["delete_edge"] = test_utils.all_equal(
        new_mock_edge_id not in [e.id for e in start_mock_vertex.out_edges],
        True,
    )

    another_mock_edge = mock_ctx.graph.create_edge(start_mock_vertex, end2_mock_vertex, mock_edge_type)
    results["edge_id_assignment"] = test_utils.all_equal(
        another_mock_edge.id,
        MAX_EDGE_ID + 2,
    )

    return mgp.Record(results_dict=results)


@mgp.read_proc
def test_read_proc_mutability(ctx: mgp.ProcCtx) -> mgp.Record(results_dict=mgp.Map):
    mock_ctx = test_utils.get_mock_proc_ctx(is_write=False)
    results = dict()

    results["is_not_mutable"] = test_utils.all_equal(
        ctx.graph.is_mutable(),
        mock_ctx.graph.is_mutable(),
        False,
    )

    return mgp.Record(results_dict=results)
