import mgp
import test_utils


@mgp.write_proc
def compare_apis_on_vertex(ctx: mgp.ProcCtx) -> mgp.Record(results_dict=mgp.Map):
    mock_ctx = test_utils.get_mock_proc_ctx(is_write=True)
    results = dict()

    TARGET_ID = 0
    target_vertex = test_utils.get_vertex(ctx, permanent_id=TARGET_ID)
    target_mock_vertex = mock_ctx.graph.get_vertex_by_id(TARGET_ID)

    properties = target_vertex.properties
    mock_properties = target_mock_vertex.properties

    results["get"] = test_utils.all_equal(
        properties.get("name"),
        mock_properties.get("name"),
        "Peter",
    )
    results["get[default]"] = test_utils.all_equal(
        properties.get("YoE", default="N/A"),
        mock_properties.get("YoE", default="N/A"),
        "N/A",
    )

    properties.set("education", "PhD")
    mock_properties.set("education", "PhD")
    results["set"] = test_utils.all_equal(
        properties.get("education"),
        mock_properties.get("education"),
        "PhD",
    )

    results["items"] = test_utils.all_equal(
        {prop.name: prop.value for prop in properties.items()},
        {prop.name: prop.value for prop in mock_properties.items()},
        {"name": "Peter", "surname": "Yang", "education": "PhD", "permanent_id": 0},
    )

    results["keys"] = test_utils.all_equal(
        {key for key in properties.keys()},
        {key for key in mock_properties.keys()},
        {"name", "surname", "education", "permanent_id"},
    )

    results["values"] = test_utils.all_equal(
        {val for val in properties.values()},
        {val for val in mock_properties.values()},
        {"Peter", "Yang", "PhD", 0},
    )

    results["__len__"] = test_utils.all_equal(
        len(properties),
        len(mock_properties),
        4,
    )

    results["__iter__"] = test_utils.all_equal(
        {name for name in properties},
        {name for name in mock_properties},
        {"name", "surname", "education", "permanent_id"},
    )

    results["__getitem__"] = test_utils.all_equal(
        {properties[name] for name in properties},
        {mock_properties[name] for name in mock_properties},
        {"Peter", "Yang", "PhD", 0},
    )

    properties["YoE"] = 6
    mock_properties["YoE"] = 6
    results["__setitem__"] = test_utils.all_equal(
        properties["YoE"],
        mock_properties["YoE"],
        6,
    )

    results["__contains__"] = test_utils.all_equal(
        "YoE" in properties,
        "age" not in properties,
        "YoE" in mock_properties,
        "age" not in mock_properties,
        True,
    ) and test_utils.all_equal(
        "YoE" not in properties,
        "age" in properties,
        "YoE" not in mock_properties,
        "age" in mock_properties,
        False,
    )

    return mgp.Record(results_dict=results)


@mgp.write_proc
def compare_apis_on_edge(ctx: mgp.ProcCtx) -> mgp.Record(results_dict=mgp.Map):
    mock_ctx = test_utils.get_mock_proc_ctx(is_write=True)
    results = dict()

    TARGET_EDGE_ID = 37

    target_edge_properties = test_utils.get_edge(ctx, permanent_id=TARGET_EDGE_ID).properties
    target_mock_edge_properties = test_utils.get_mock_edge(mock_ctx, id=TARGET_EDGE_ID).properties

    results["get"] = test_utils.all_equal(
        target_edge_properties.get("importance"),
        target_mock_edge_properties.get("importance"),
        "HIGH",
    )
    results["get[default]"] = test_utils.all_equal(
        target_edge_properties.get("priority", default="N/A"),
        target_mock_edge_properties.get("priority", default="N/A"),
        "N/A",
    )

    target_edge_properties.set("priority", "MEDIUM")
    target_mock_edge_properties.set("priority", "MEDIUM")
    results["set"] = test_utils.all_equal(
        target_edge_properties.get("priority"),
        target_mock_edge_properties.get("priority"),
        "MEDIUM",
    )

    results["items"] = test_utils.all_equal(
        {prop.name: prop.value for prop in target_edge_properties.items()},
        {prop.name: prop.value for prop in target_mock_edge_properties.items()},
        {"importance": "HIGH", "priority": "MEDIUM", "permanent_id": 37},
    )

    results["keys"] = test_utils.all_equal(
        {key for key in target_edge_properties.keys()},
        {key for key in target_mock_edge_properties.keys()},
        {"importance", "priority", "permanent_id"},
    )

    results["values"] = test_utils.all_equal(
        {val for val in target_edge_properties.values()},
        {val for val in target_mock_edge_properties.values()},
        {"HIGH", "MEDIUM", 37},
    )

    results["__len__"] = test_utils.all_equal(
        len(target_edge_properties),
        len(target_mock_edge_properties),
        3,
    )

    results["__iter__"] = test_utils.all_equal(
        {name for name in target_edge_properties},
        {name for name in target_mock_edge_properties},
        {"importance", "priority", "permanent_id"},
    )

    results["__getitem__"] = test_utils.all_equal(
        {target_edge_properties[name] for name in target_edge_properties},
        {target_mock_edge_properties[name] for name in target_mock_edge_properties},
        {"HIGH", "MEDIUM", 37},
    )

    target_edge_properties["priority"] = "LOW"
    target_mock_edge_properties["priority"] = "LOW"
    results["__setitem__"] = test_utils.all_equal(
        target_edge_properties["priority"],
        target_mock_edge_properties["priority"],
        "LOW",
    )

    results["__contains__"] = test_utils.all_equal(
        "priority" in target_edge_properties,
        "status" not in target_edge_properties,
        "priority" in target_mock_edge_properties,
        "status" not in target_mock_edge_properties,
        True,
    ) and test_utils.all_equal(
        "priority" not in target_edge_properties,
        "status" in target_edge_properties,
        "priority" not in target_mock_edge_properties,
        "status" in target_mock_edge_properties,
        False,
    )

    return mgp.Record(results_dict=results)
