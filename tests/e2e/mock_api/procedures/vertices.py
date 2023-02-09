import mgp
import mgp_mock
import test_utils


@mgp.read_proc
def compare_apis(ctx: mgp.ProcCtx) -> mgp.Record(results_dict=mgp.Map):
    mock_ctx = test_utils.get_mock_proc_ctx(is_write=False)
    results = dict()

    vertices = ctx.graph.vertices
    mock_vertices = mock_ctx.graph.vertices

    results["is_valid"] = test_utils.all_equal(
        vertices.is_valid(),
        mock_vertices.is_valid(),
        True,
    )

    results["__iter__"] = test_utils.all_equal(
        all(isinstance(vertex, mgp.Vertex) for vertex in vertices),
        all(isinstance(vertex, mgp_mock.Vertex) for vertex in mock_vertices),
        True,
    )

    results["__contains__"] = test_utils.all_equal(
        all((test_utils.get_vertex(ctx, permanent_id=permanent_id) in vertices) for permanent_id in range(27)),
        all((mock_ctx.graph.get_vertex_by_id(id) in mock_vertices) for id in range(27)),
        True,
    )

    results["__len__"] = test_utils.all_equal(
        len(vertices),
        len(mock_vertices),
        27,
    )

    return mgp.Record(results_dict=results)
