"""E2E tests for export.json_data / json_all / json_graph.

Every expectation here was measured against the reference implementation; none of it is inferred.
Two things are deliberately not asserted:

* Element *id values* differ per database, so ids are rewritten to first-appearance order
  (`n0`, `n1`, `r0`, ...) before comparing. The id *format* — a string, not an int — is asserted.
* Property key order inside `properties` is not reproducible across implementations, so payloads
  are compared as parsed JSON, not as bytes.

Run with:
    pytest test_json_export.py -v

Requires a running Memgraph with the `export` query module loaded.
"""

import json
import os

import mgclient
import pytest

MEMGRAPH_HOST = os.environ.get("MEMGRAPH_HOST", "127.0.0.1")
MEMGRAPH_PORT = int(os.environ.get("MEMGRAPH_PORT", "7687"))

# Collects every node and relationship in creation order, so the exported element order is
# deterministic (the procedures preserve input list order).
COLLECT_ALL = (
    "MATCH (n) WITH n ORDER BY id(n) WITH collect(n) AS ns "
    "OPTIONAL MATCH ()-[r]->() WITH ns, r ORDER BY id(r) WITH ns, collect(r) AS rs "
)

RESULT_COLUMNS = "file, source, format, nodes, relationships, properties, time, rows, batchSize, batches, done, data"

SEED_MIXED = """CREATE
 (a:Person:Employee {name:'Alice', age:33, tags:['x','y'], score:1.5, ok:true}),
 (b:Person {name:'Bob'}),
 (c:Product {sku:'S1'}),
 (a)-[:KNOWS {since:2020, w:0.5}]->(b),
 (b)-[:BOUGHT]->(c)"""

ALICE = {
    "id": "n0",
    "labels": ["Employee", "Person"],
    "properties": {"name": "Alice", "age": 33, "tags": ["x", "y"], "score": 1.5, "ok": True},
}
BOB = {"id": "n1", "labels": ["Person"], "properties": {"name": "Bob"}}
PRODUCT = {"id": "n2", "labels": ["Product"], "properties": {"sku": "S1"}}

NODE_ELEMENTS = [{"type": "node", **node} for node in (ALICE, BOB, PRODUCT)]
KNOWS = {
    "type": "relationship",
    "id": "r0",
    "label": "KNOWS",
    "properties": {"since": 2020, "w": 0.5},
    "start": ALICE,
    "end": BOB,
}
# BOUGHT has no properties of its own, so it carries no `properties` key at all.
BOUGHT = {"type": "relationship", "id": "r1", "label": "BOUGHT", "start": BOB, "end": PRODUCT}
ALL_ELEMENTS = [*NODE_ELEMENTS, KNOWS, BOUGHT]


@pytest.fixture(scope="module")
def conn():
    connection = mgclient.connect(host=MEMGRAPH_HOST, port=MEMGRAPH_PORT)
    connection.autocommit = True
    cursor = connection.cursor()
    cursor.execute("CALL mg.procedures() YIELD name RETURN collect(name) AS names")
    names = cursor.fetchall()[0][0]
    if "export.json_data" not in names:
        pytest.skip("the `export` query module is not loaded")
    yield connection
    connection.close()


@pytest.fixture(autouse=True)
def clean(conn):
    execute(conn, "MATCH (n) DETACH DELETE n")


def execute(conn, query):
    """Runs a query and returns its rows as dicts; mgclient asserts if a result set is left unread."""
    cursor = conn.cursor()
    cursor.execute(query)
    if cursor.description is None:
        return []
    rows = cursor.fetchall()
    columns = [column.name for column in cursor.description]
    return [dict(zip(columns, row)) for row in rows]


def canonical_ids(element, seen):
    """Rewrites ids to first-appearance order. Nodes and relationships have separate id spaces."""
    if isinstance(element, dict):
        # `labels` marks a node (top-level or an inlined endpoint); `label` marks a relationship.
        prefix = "n" if "labels" in element else "r" if "label" in element else None
        out = {}
        for key, value in element.items():
            if key == "id" and prefix:
                assert isinstance(value, str), f"ids must be serialized as strings, got {value!r}"
                ids = seen.setdefault(prefix, {})
                out[key] = f"{prefix}{ids.setdefault(value, len(ids))}"
            else:
                out[key] = canonical_ids(value, seen)
        return out
    if isinstance(element, list):
        return [canonical_ids(item, seen) for item in element]
    return element


def without_ids(element):
    """Drops every `id` key, for comparisons where scan order makes first-appearance ids meaningless."""
    if isinstance(element, dict):
        return {key: without_ids(value) for key, value in element.items() if key != "id"}
    if isinstance(element, list):
        return [without_ids(item) for item in element]
    return element


def parse(payload, canonicalize=True):
    """Parses a payload in any of the three output shapes, optionally canonicalizing element ids."""
    if payload is None or payload == "":
        return payload
    if payload.lstrip().startswith('{"nodes"'):
        parsed = json.loads(payload)
    else:
        parsed = [json.loads(line) for line in payload.split("\n")]
    return canonical_ids(parsed, {}) if canonicalize else parsed


def export(conn, config="{stream: true}", *, collect=COLLECT_ALL, call=None, canonicalize=True):
    """Runs one export call and returns its single result row with `data` already parsed."""
    call = call or f"export.json_data(ns, rs, null, {config})"
    rows = execute(conn, f"{collect}CALL {call} YIELD {RESULT_COLUMNS} RETURN {RESULT_COLUMNS}")
    assert len(rows) == 1
    row = rows[0]
    row["raw_data"] = row["data"]
    row["data"] = parse(row["data"], canonicalize)
    return row


def test_json_lines_is_the_default_shape(conn):
    execute(conn, SEED_MIXED)
    row = export(conn)
    assert row["data"] == ALL_ELEMENTS


def test_scalar_columns(conn):
    execute(conn, SEED_MIXED)
    row = export(conn)
    assert row["file"] is None
    assert row["source"] == "data: nodes(3), rels(2)"
    assert row["format"] == "json"
    assert row["nodes"] == 3
    assert row["relationships"] == 2
    # Only the top-level elements' own properties: 7 on the nodes, 2 on KNOWS, 0 on BOUGHT. The
    # inlined start/end properties are not counted.
    assert row["properties"] == 9
    assert row["rows"] == 5, "rows is nodes + relationships"
    assert row["batchSize"] == -1
    assert row["batches"] == 0
    assert row["done"] is True
    assert isinstance(row["time"], int)


def test_json_format_groups_nodes_and_rels(conn):
    execute(conn, SEED_MIXED)
    row = export(conn, "{stream: true, jsonFormat: 'JSON'}")
    assert row["data"] == {"nodes": NODE_ELEMENTS, "rels": [KNOWS, BOUGHT]}


def test_json_id_as_keys_format(conn):
    execute(conn, SEED_MIXED)
    # Read the ids raw here: they are the object keys as well as the `id` fields, and the point of
    # this shape is that the two agree.
    data = export(conn, "{stream: true, jsonFormat: 'JSON_ID_AS_KEYS'}", canonicalize=False)["data"]
    assert set(data) == {"nodes", "rels"}
    for group, expected_count in (("nodes", 3), ("rels", 2)):
        assert len(data[group]) == expected_count
        for key, element in data[group].items():
            assert key == element["id"]
    # Same elements as JSON_LINES, just keyed by id.
    assert canonical_ids(list(data["nodes"].values()) + list(data["rels"].values()), {}) == ALL_ELEMENTS


def test_empty_groups_are_emitted_for_json_formats(conn):
    execute(conn, "CREATE (:Product {sku: 'S1'})")
    assert export(conn, "{stream: true, jsonFormat: 'JSON'}")["data"] == {
        "nodes": [{"type": "node", "id": "n0", "labels": ["Product"], "properties": {"sku": "S1"}}],
        "rels": [],
    }
    assert export(conn, "{stream: true, jsonFormat: 'JSON_ID_AS_KEYS'}")["data"]["rels"] == {}


@pytest.mark.parametrize("nodes,rels", [("null", "null"), ("[]", "[]")])
def test_null_and_empty_inputs_produce_an_all_zero_row(conn, nodes, rels):
    execute(conn, SEED_MIXED)
    row = export(conn, call=f"export.json_data({nodes}, {rels}, null, {{stream: true}})", collect="")
    assert row["source"] == "data: nodes(0), rels(0)"
    assert (row["nodes"], row["relationships"], row["properties"], row["rows"]) == (0, 0, 0, 0)
    assert row["done"] is True
    assert row["data"] == "", "an empty input set streams an empty payload, not null"


def test_relationship_endpoints_are_inlined_even_when_not_exported(conn):
    execute(conn, SEED_MIXED)
    row = export(conn, call="export.json_data([], rs, null, {stream: true})")
    assert row["data"] == [KNOWS, BOUGHT], "endpoints are inlined in full"
    assert row["nodes"] == 0
    assert row["properties"] == 2, "endpoint properties are not counted, only KNOWS' own two"


def test_duplicate_node_is_not_deduplicated(conn):
    execute(conn, "CREATE (:Product {sku: 'S1'})")
    row = export(conn, call="export.json_data([ns[0], ns[0]], [], null, {stream: true})")
    product = {"type": "node", "id": "n0", "labels": ["Product"], "properties": {"sku": "S1"}}
    assert row["data"] == [product, product]
    assert row["nodes"] == 2
    assert row["properties"] == 2, "the duplicate's properties are counted twice"


def test_duplicate_node_collapses_under_id_as_keys(conn):
    execute(conn, "CREATE (:Product {sku: 'S1'})")
    row = export(
        conn,
        call="export.json_data([ns[0], ns[0]], [], null, {stream: true, jsonFormat: 'JSON_ID_AS_KEYS'})",
        canonicalize=False,
    )
    # Ids are object keys here, so a duplicate can only be one entry. The reference instead emits the
    # key twice, which is ambiguous JSON that parsers collapse the same way.
    assert len(row["data"]["nodes"]) == 1
    assert row["nodes"] == 2, "the counter still reports both"


def test_labels_are_sorted_and_empty_properties_omitted(conn):
    execute(conn, "CREATE (:Zebra:Apple:Mango)")
    assert export(conn)["data"] == [{"type": "node", "id": "n0", "labels": ["Apple", "Mango", "Zebra"]}]


def test_write_node_properties_false_drops_node_and_endpoint_properties(conn):
    execute(conn, SEED_MIXED)
    row = export(conn, "{stream: true, writeNodeProperties: false}")
    bare = [{key: value for key, value in node.items() if key != "properties"} for node in (ALICE, BOB, PRODUCT)]
    assert row["data"] == [
        *({"type": "node", **node} for node in bare),
        # Relationship properties go too: writeRelationshipProperties defaults to writeNodeProperties.
        {"type": "relationship", "id": "r0", "label": "KNOWS", "start": bare[0], "end": bare[1]},
        {"type": "relationship", "id": "r1", "label": "BOUGHT", "start": bare[1], "end": bare[2]},
    ]
    assert row["properties"] == 9, "the counter ignores the write flags"


def test_write_relationship_properties_false_keeps_node_properties(conn):
    execute(conn, SEED_MIXED)
    row = export(conn, "{stream: true, writeRelationshipProperties: false}")
    assert row["data"] == [
        *NODE_ELEMENTS,
        {"type": "relationship", "id": "r0", "label": "KNOWS", "start": ALICE, "end": BOB},
        BOUGHT,
    ]
    assert row["properties"] == 9


def test_write_relationship_properties_true_overrides_the_node_flag(conn):
    execute(conn, SEED_MIXED)
    row = export(conn, "{stream: true, writeNodeProperties: false, writeRelationshipProperties: true}")
    assert "properties" in row["data"][3], "an explicit true wins over the writeNodeProperties fallback"
    assert "properties" not in row["data"][0]


def test_use_types_is_accepted_and_inert(conn):
    execute(conn, SEED_MIXED)
    assert export(conn, "{stream: true, useTypes: true}")["data"] == export(conn)["data"]


@pytest.mark.parametrize(
    "value,expected",
    [
        # Sub-second digits come in 3-digit groups, and seconds are elided only when the whole tail is zero.
        ("localtime('09:15:30')", "09:15:30"),
        ("localtime('09:15:00')", "09:15"),
        ("localtime('09:00:00')", "09:00"),
        ("localtime('09:15:30.100')", "09:15:30.100"),
        ("localtime('09:15:30.500')", "09:15:30.500"),
        ("localtime('09:15:30.000001')", "09:15:30.000001"),
        ("date('1990-01-02')", "1990-01-02"),
        ("localdatetime('2020-01-01T10:00:30')", "2020-01-01T10:00:30"),
        ("localdatetime('2020-01-01T00:00:00')", "2020-01-01T00:00"),
        ("localdatetime('2020-01-01T10:00:30.250')", "2020-01-01T10:00:30.250"),
        # A named zone gets a bracketed suffix; the offset is the one in effect at that instant.
        ("datetime('2020-01-01T10:00:00[Europe/Zagreb]')", "2020-01-01T10:00+01:00[Europe/Zagreb]"),
        ("datetime('2020-06-15T10:00:00[Europe/Zagreb]')", "2020-06-15T10:00+02:00[Europe/Zagreb]"),
        # An offset-only zone has no name, so no bracket. A zero offset renders as `Z`.
        ("datetime('2020-01-01T10:00:00+02:00')", "2020-01-01T10:00+02:00"),
        ("datetime('2020-06-01T10:00:00-05:30')", "2020-06-01T10:00-05:30"),
        ("datetime('2020-01-01T10:00:00Z')", "2020-01-01T10:00Z[Etc/UTC]"),
        # Durations carry a sign per component and strip trailing fraction zeros.
        ("duration('PT0S')", "PT0S"),
        ("duration('PT1.5S')", "PT1.5S"),
        ("duration('P1DT2H')", "P1DT2H"),
        ("duration('P7D')", "P7D"),
        ("duration('PT90M')", "PT1H30M"),
        ("duration('PT1H30.25S')", "PT1H30.25S"),
        ("duration('PT23H59M59.999999S')", "PT23H59M59.999999S"),
        ("duration('PT0.000001S')", "PT0.000001S"),
        ("duration('PT-2H-30M')", "PT-2H-30M"),
        ("duration('PT-0.5S')", "PT-0.5S"),
        ("duration('PT-1H-1.5S')", "PT-1H-1.5S"),
        ("duration('P-1DT-2H')", "P-1DT-2H"),
    ],
)
def test_temporal_serialization(conn, value, expected):
    execute(conn, f"CREATE (:T {{v: {value}}})")
    assert export(conn)["data"][0]["properties"] == {"v": expected}


@pytest.mark.parametrize(
    "value,expected",
    [
        ("point({x: 1.0, y: 2.0})", {"crs": "cartesian", "x": 1.0, "y": 2.0, "z": None}),
        ("point({x: 1.0, y: 2.0, z: 3.0})", {"crs": "cartesian-3d", "x": 1.0, "y": 2.0, "z": 3.0}),
        # WGS-84 uses geographic key names, latitude first — not x/y/z.
        (
            "point({longitude: 1.5, latitude: 2.5})",
            {"crs": "wgs-84", "latitude": 2.5, "longitude": 1.5, "height": None},
        ),
        (
            "point({longitude: 1.5, latitude: 2.5, height: 3.5})",
            {"crs": "wgs-84-3d", "latitude": 2.5, "longitude": 1.5, "height": 3.5},
        ),
    ],
)
def test_point_serialization(conn, value, expected):
    execute(conn, f"CREATE (:P {{v: {value}}})")
    assert export(conn)["data"][0]["properties"] == {"v": expected}


def test_nested_containers_and_scalars(conn):
    execute(conn, "CREATE (:S {l: [1, 'two', true, null], m: {a: 1, b: [2]}, e: [], n: -7, f: 0.25})")
    assert export(conn)["data"][0]["properties"] == {
        "l": [1, "two", True, None],
        "m": {"a": 1, "b": [2]},
        "e": [],
        "n": -7,
        "f": 0.25,
    }


def test_json_all_exports_the_whole_database(conn):
    execute(conn, SEED_MIXED)
    row = export(conn, call="export.json_all(null, {stream: true})", collect="")
    assert row["source"] == "database: nodes(3), rels(2)"
    assert (row["nodes"], row["relationships"], row["properties"], row["rows"]) == (3, 2, 9, 5)

    # Element order here is the storage engine's scan order, which also makes first-appearance ids
    # arbitrary — so compare the elements by content, ids excluded. The id relationships (start/end
    # pointing at the right nodes) are covered by the ordered json_data tests above.
    def key(element):
        return json.dumps(without_ids(element), sort_keys=True)

    assert sorted(map(key, row["data"])) == sorted(map(key, ALL_ELEMENTS))


def test_json_graph_takes_a_single_map(conn):
    execute(conn, SEED_MIXED)
    row = export(conn, call="export.json_graph({nodes: ns, relationships: rs}, null, {stream: true})")
    assert row["source"] == "graph: nodes(3), rels(2)"
    assert row["data"] == ALL_ELEMENTS


@pytest.mark.parametrize("graph", ["{}", "{nodes: null, relationships: null}"])
def test_json_graph_tolerates_missing_keys(conn, graph):
    execute(conn, SEED_MIXED)
    row = export(conn, call=f"export.json_graph({graph}, null, {{stream: true}})", collect="")
    assert (row["nodes"], row["relationships"], row["rows"]) == (0, 0, 0)
    assert row["data"] == ""


def test_file_write_returns_null_data_and_matches_the_stream(conn, tmp_path):
    execute(conn, SEED_MIXED)
    path = tmp_path / "export.json"
    row = export(conn, call=f"export.json_data(ns, rs, '{path}', {{}})")
    assert row["file"] == str(path), "the file column echoes the argument"
    assert row["data"] is None, "data is null whenever the payload went to a file"
    assert row["nodes"] == 3, "the counters are unaffected by the sink"
    assert parse(path.read_text()) == ALL_ELEMENTS
    # The two sinks must not diverge: same graph, same bytes.
    assert path.read_text() == export(conn)["raw_data"]


def test_file_argument_wins_over_stream(conn, tmp_path):
    execute(conn, SEED_MIXED)
    path = tmp_path / "both.json"
    row = export(conn, call=f"export.json_data(ns, rs, '{path}', {{stream: true}})")
    assert row["data"] is None
    assert parse(path.read_text()) == ALL_ELEMENTS


def test_no_file_and_no_stream_discards_the_payload(conn):
    execute(conn, SEED_MIXED)
    row = export(conn, "{}")
    assert row["file"] is None
    assert row["data"] is None
    assert row["nodes"] == 3


@pytest.mark.parametrize(
    "config",
    [
        # Rejected rather than ignored: a caller asking for gzip must not receive plaintext.
        "{compression: 'gzip'}",
        "{charset: 'UTF-8'}",
        "{jsonFormat: 'NOPE'}",
        "{stream: 'yes'}",
        "{writeNodeProperties: 1}",
    ],
)
def test_invalid_config_is_rejected(conn, config):
    execute(conn, SEED_MIXED)
    with pytest.raises(Exception):
        export(conn, config)


def test_unwritable_path_is_reported(conn):
    execute(conn, SEED_MIXED)
    with pytest.raises(Exception):
        export(conn, call="export.json_data(ns, rs, '/proc/nope/x.json', {})")
