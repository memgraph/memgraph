import mgp
import test_utils


@mgp.read_proc
def compare_apis(ctx: mgp.ProcCtx) -> mgp.Record(results_dict=mgp.Map):
    mock_ctx = test_utils.get_mock_proc_ctx(is_write=False)
    results = dict()

    TARGET_LABELLED_NODE_ID = 5

    target_vertex = test_utils.get_vertex(ctx, permanent_id=TARGET_LABELLED_NODE_ID)
    target_mock_vertex = mock_ctx.graph.get_vertex_by_id(TARGET_LABELLED_NODE_ID)

    label_1, label_2 = sorted(target_vertex.labels, key=lambda l: l.name)  # ("Company", "Startup")
    mock_label_1, mock_label_2 = sorted(target_mock_vertex.labels, key=lambda l: l.name)  # ditto

    results["name"] = test_utils.all_equal(
        (label_1.name, label_2.name),
        (mock_label_1.name, mock_label_2.name),
        ("Company", "Startup"),
    )

    results["__eq__"] = test_utils.all_equal(
        label_1 == label_1,
        label_1 == "Company",
        mock_label_1 == mock_label_1,
        mock_label_1 == "Company",
        True,
    ) and test_utils.all_equal(
        label_1 == label_2,
        label_1 == "Startup",
        mock_label_1 == mock_label_2,
        mock_label_1 == "Startup",
        False,
    )

    results["__ne__"] = test_utils.all_equal(
        label_1 != label_2,
        label_1 != "Startup",
        mock_label_1 != mock_label_2,
        mock_label_1 != "Startup",
        True,
    ) and test_utils.all_equal(
        label_1 != label_1,
        label_1 != "Company",
        mock_label_1 != mock_label_1,
        mock_label_1 != "Company",
        False,
    )

    return mgp.Record(results_dict=results)
