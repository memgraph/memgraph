import mgp
import test_utils


@mgp.read_proc
def compare_apis(ctx: mgp.ProcCtx) -> mgp.Record(results_dict=mgp.Map):
    mock_ctx = test_utils.get_mock_proc_ctx(is_write=False)
    results = dict()

    TARGET_EDGE_ID = 0

    target_edge_type = test_utils.get_edge(ctx, permanent_id=TARGET_EDGE_ID).type
    target_mock_edge_type = test_utils.get_mock_edge(mock_ctx, id=TARGET_EDGE_ID).type

    results["name"] = test_utils.all_equal(
        target_edge_type.name,
        target_mock_edge_type.name,
        "IS_PART_OF",
    )

    results["__eq__"] = test_utils.all_equal(
        target_edge_type == target_edge_type,
        target_edge_type == "IS_PART_OF",
        target_mock_edge_type == target_mock_edge_type,
        target_mock_edge_type == "IS_PART_OF",
    )

    results["__ne__"] = test_utils.all_equal(
        target_edge_type != "HAS_TEAM",
        target_mock_edge_type != "HAS_TEAM",
    )

    return mgp.Record(results_dict=results)
