import datetime
import json

import _mgp_mock
import mgp_mock
import networkx as nx


def _make_ctx(node_info, edge_data=None, edge_info=None):
    """Build a mock ProcCtx from simple dicts."""
    edge_data = edge_data or []
    edge_info = edge_info or {}

    g = nx.MultiDiGraph(edge_data)
    for node_id in node_info:
        if not g.has_node(node_id):
            g.add_node(node_id)
    nx.set_node_attributes(g, node_info)
    if edge_info:
        nx.set_edge_attributes(g, edge_info)

    g = nx.freeze(g)
    return mgp_mock.ProcCtx(_mgp_mock.Graph(g))


def _get_vertex(ctx, node_id):
    return ctx.graph.get_vertex_by_id(node_id)


def _get_edge(ctx, edge_id):
    for v in ctx.graph.vertices:
        for e in v.out_edges:
            if e.id == edge_id:
                return e
    return None


def test_properties_to_dict_primitives():
    """String, int, float, bool, and None values pass through unchanged."""
    ctx = _make_ctx({
        0: {
            "labels": "Node",
            "name": "Alice",
            "age": 30,
            "score": 9.5,
            "active": True,
            "deleted": False,
            "nickname": None,
        },
    })
    v = _get_vertex(ctx, 0)
    d = v.properties.to_dict()

    assert d["name"] == "Alice"
    assert d["age"] == 30
    assert d["score"] == 9.5
    assert d["active"] is True
    assert d["deleted"] is False
    if "nickname" in d:
        assert d["nickname"] is None


def test_properties_to_dict_is_json_serializable():
    """to_dict() result can be passed straight to json.dumps()."""
    ctx = _make_ctx({
        0: {"labels": "Person", "name": "Bob", "age": 25},
    })
    v = _get_vertex(ctx, 0)
    d = v.properties.to_dict()

    serialized = json.dumps(d)
    assert isinstance(serialized, str)

    roundtrip = json.loads(serialized)
    assert roundtrip["name"] == "Bob"
    assert roundtrip["age"] == 25


def test_properties_to_dict_datetime():
    """datetime.datetime values are converted to ISO-8601 strings."""
    dt = datetime.datetime(2026, 2, 11, 14, 30, 0)
    ctx = _make_ctx({
        0: {"labels": "Event", "created_at": dt},
    })
    v = _get_vertex(ctx, 0)
    d = v.properties.to_dict()

    assert d["created_at"] == "2026-02-11T14:30:00"
    json.dumps(d)


def test_properties_to_dict_date():
    """datetime.date values are converted to ISO-8601 strings."""
    date_val = datetime.date(2026, 1, 15)
    ctx = _make_ctx({
        0: {"labels": "Event", "event_date": date_val},
    })
    v = _get_vertex(ctx, 0)
    d = v.properties.to_dict()

    assert d["event_date"] == "2026-01-15"
    json.dumps(d)


def test_properties_to_dict_time():
    """datetime.time values are converted to ISO-8601 strings."""
    time_val = datetime.time(9, 30, 45)
    ctx = _make_ctx({
        0: {"labels": "Schedule", "start_time": time_val},
    })
    v = _get_vertex(ctx, 0)
    d = v.properties.to_dict()

    assert d["start_time"] == "09:30:45"
    json.dumps(d)


def test_properties_to_dict_timedelta():
    """datetime.timedelta values are converted to ISO-8601 duration strings."""
    td = datetime.timedelta(
        hours=2, minutes=30, seconds=15, microseconds=123456
    )
    ctx = _make_ctx({
        0: {"labels": "Task", "duration": td},
    })
    v = _get_vertex(ctx, 0)
    d = v.properties.to_dict()

    assert d["duration"] == "P0DT2H30M15.123456S"
    json.dumps(d)


def test_properties_to_dict_nested_list():
    """Lists inside properties are recursively converted."""
    ctx = _make_ctx({
        0: {
            "labels": "Node",
            "tags": ["alpha", "beta"],
            "timestamps": [
                datetime.date(2026, 1, 1),
                datetime.date(2026, 6, 15),
            ],
        },
    })
    v = _get_vertex(ctx, 0)
    d = v.properties.to_dict()

    assert d["tags"] == ["alpha", "beta"]
    assert d["timestamps"] == ["2026-01-01", "2026-06-15"]
    json.dumps(d)


def test_properties_to_dict_nested_dict():
    """Dicts inside properties are recursively converted."""
    ctx = _make_ctx({
        0: {
            "labels": "Node",
            "metadata": {
                "created": datetime.datetime(2026, 3, 1, 12, 0, 0),
                "count": 42,
            },
        },
    })
    v = _get_vertex(ctx, 0)
    d = v.properties.to_dict()

    assert d["metadata"] == {"created": "2026-03-01T12:00:00", "count": 42}
    json.dumps(d)


def test_properties_to_dict_on_edge():
    """to_dict() works on edge properties too."""
    ctx = _make_ctx(
        node_info={
            0: {"labels": "A"},
            1: {"labels": "B"},
        },
        edge_data=[(0, 1, 0)],
        edge_info={
            (0, 1, 0): {
                "type": "CONNECTS",
                "weight": 3.14,
                "since": datetime.date(2025, 6, 1),
            },
        },
    )
    e = _get_edge(ctx, 0)
    d = e.properties.to_dict()

    assert d["weight"] == 3.14
    assert d["since"] == "2025-06-01"
    json.dumps(d)


def test_module_level_to_dict_with_properties():
    """mgp_mock.to_dict() works when passed a Properties object."""
    ctx = _make_ctx({
        0: {"labels": "X", "key": "value"},
    })
    v = _get_vertex(ctx, 0)
    d = mgp_mock.to_dict(v.properties)

    assert d["key"] == "value"


def test_module_level_to_dict_with_primitives():
    """mgp_mock.to_dict() passes primitives through."""
    assert mgp_mock.to_dict(42) == 42
    assert mgp_mock.to_dict("hello") == "hello"
    assert mgp_mock.to_dict(3.14) == 3.14
    assert mgp_mock.to_dict(True) is True
    assert mgp_mock.to_dict(None) is None


def test_module_level_to_dict_with_temporal():
    """mgp_mock.to_dict() converts temporal types."""
    dt = datetime.datetime(2026, 7, 4, 10, 0, 0)
    assert mgp_mock.to_dict(dt) == "2026-07-04T10:00:00"

    d = datetime.date(2026, 12, 25)
    assert mgp_mock.to_dict(d) == "2026-12-25"

    t = datetime.time(23, 59, 59)
    assert mgp_mock.to_dict(t) == "23:59:59"

    td = datetime.timedelta(days=1, hours=2)
    result = mgp_mock.to_dict(td)
    assert result == "P0DT26H0M0.000000S"


def test_module_level_to_dict_with_list():
    """mgp_mock.to_dict() recursively handles lists."""
    result = mgp_mock.to_dict([1, "two", datetime.date(2026, 1, 1)])
    assert result == [1, "two", "2026-01-01"]


def test_module_level_to_dict_with_dict():
    """mgp_mock.to_dict() recursively handles dicts."""
    result = mgp_mock.to_dict({"a": 1, "b": datetime.time(8, 0)})
    assert result == {"a": 1, "b": "08:00:00"}


def test_properties_to_dict_empty():
    """Vertex with no properties returns an empty dict."""
    ctx = _make_ctx({
        0: {"labels": "Empty"},
    })
    v = _get_vertex(ctx, 0)
    d = v.properties.to_dict()

    assert isinstance(d, dict)
    json.dumps(d)


def test_properties_to_dict_deeply_nested():
    """Deeply nested structures are recursively converted."""
    ctx = _make_ctx({
        0: {
            "labels": "Deep",
            "data": {
                "level1": {
                    "level2": [
                        {"ts": datetime.datetime(2026, 1, 1, 0, 0, 0)},
                    ]
                }
            },
        },
    })
    v = _get_vertex(ctx, 0)
    d = v.properties.to_dict()

    assert d["data"]["level1"]["level2"][0]["ts"] == "2026-01-01T00:00:00"
    json.dumps(d)


def test_value_to_dict_unknown_type_falls_back_to_str():
    """Unknown types are converted to their string representation."""

    class Custom:
        def __str__(self):
            return "custom_object"

    result = mgp_mock._value_to_dict(Custom())
    assert result == "custom_object"
    json.dumps(result)


def test_value_to_dict_tuple_becomes_list():
    """Tuples are converted to lists for JSON compatibility."""
    result = mgp_mock._value_to_dict((1, 2, 3))
    assert result == [1, 2, 3]
    assert isinstance(result, list)


def test_value_to_dict_empty_list():
    """Empty list passes through as empty list."""
    assert mgp_mock._value_to_dict([]) == []


def test_value_to_dict_empty_dict():
    """Empty dict passes through as empty dict."""
    assert mgp_mock._value_to_dict({}) == {}


def test_value_to_dict_datetime_with_microseconds():
    """datetime with microseconds preserves them in ISO format."""
    dt = datetime.datetime(2026, 3, 15, 10, 30, 45, 123456)
    result = mgp_mock._value_to_dict(dt)
    assert result == "2026-03-15T10:30:45.123456"


def test_value_to_dict_timedelta_zero():
    """Zero timedelta converts correctly."""
    td = datetime.timedelta(0)
    result = mgp_mock._value_to_dict(td)
    assert result == "P0DT0H0M0.000000S"


def test_value_to_dict_timedelta_large():
    """Large timedelta (multiple days) is expressed in total hours."""
    td = datetime.timedelta(days=3, hours=5, minutes=10, seconds=20)
    result = mgp_mock._value_to_dict(td)
    assert result == "P0DT77H10M20.000000S"


def test_value_to_dict_bool_not_confused_with_int():
    """True stays True, not 1 (bool is a subclass of int)."""
    assert mgp_mock._value_to_dict(True) is True
    assert mgp_mock._value_to_dict(False) is False


def test_value_to_dict_float_special_values():
    """Infinity and NaN don't crash _value_to_dict."""
    assert mgp_mock._value_to_dict(float("inf")) == float("inf")
    assert mgp_mock._value_to_dict(float("-inf")) == float("-inf")
    import math
    result = mgp_mock._value_to_dict(float("nan"))
    assert math.isnan(result)


def test_properties_to_dict_returns_new_dict():
    """Each call returns a fresh dict (not a cached/shared reference)."""
    ctx = _make_ctx({
        0: {"labels": "Node", "x": 1},
    })
    v = _get_vertex(ctx, 0)
    d1 = v.properties.to_dict()
    d2 = v.properties.to_dict()

    assert d1 == d2
    assert d1 is not d2  # must be separate objects


def test_full_export_workflow():
    """Build a JSON export using to_dict(), verify it round-trips."""
    ctx = _make_ctx(
        node_info={
            0: {
                "labels": "Person",
                "name": "Alice",
                "born": datetime.date(1990, 5, 20),
            },
            1: {
                "labels": "Person",
                "name": "Bob",
                "born": datetime.date(1985, 11, 3),
            },
        },
        edge_data=[(0, 1, 0)],
        edge_info={
            (0, 1, 0): {"type": "KNOWS", "since": datetime.date(2020, 1, 1)},
        },
    )

    nodes = []
    for v in ctx.graph.vertices:
        nodes.append({
            "id": v.id,
            "labels": [label.name for label in v.labels],
            "properties": v.properties.to_dict(),
        })

    edges = []
    for v in ctx.graph.vertices:
        for e in v.out_edges:
            edges.append({
                "id": e.id,
                "type": e.type.name,
                "start": e.from_vertex.id,
                "end": e.to_vertex.id,
                "properties": e.properties.to_dict(),
            })

    export = {"nodes": nodes, "edges": edges}

    json_str = json.dumps(export, sort_keys=True)
    roundtrip = json.loads(json_str)

    assert len(roundtrip["nodes"]) == 2
    assert len(roundtrip["edges"]) == 1
    assert roundtrip["edges"][0]["properties"]["since"] == "2020-01-01"
