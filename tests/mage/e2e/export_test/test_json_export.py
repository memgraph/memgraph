"""E2E tests for export.json_data / json_all / json_graph.

Expectations were measured against the reference implementation unless a comment says otherwise —
the exceptions are the handful of cases where the two engines cannot agree because the *input*
differs (Memgraph binds a trailing `Z` to the named zone `Etc/UTC`, and stores a duration as a
single microsecond count) and the two config values we deliberately reject rather than coerce.
Those are marked individually.

Two things are deliberately not asserted:

* Element *id values* differ per database, so ids are rewritten to first-appearance order
  (`n0`, `n1`, `r0`, ...) before comparing. The id *format* — a string, not an int — is asserted.
* Property key order inside `properties` is not reproducible across implementations, so payloads
  are compared as parsed JSON, not as bytes.

Run with:
    pytest test_json_export.py -v

Requires a running Memgraph with the `export` query module loaded.
"""

import contextlib
import glob
import itertools
import json
import os
import shutil
import stat
import subprocess

import mgclient
import pytest

MEMGRAPH_HOST = os.environ.get("MEMGRAPH_HOST", "127.0.0.1")
MEMGRAPH_PORT = int(os.environ.get("MEMGRAPH_PORT", "7687"))

# Written by the server, so the path must exist on its side; tests that read the file back need the server local, or
# MAGE_E2E_SHARED_DIR shared with it at the same path. The `server_files` fixture skips them otherwise.
SERVER_SHARED_DIR = os.environ.get("MAGE_E2E_SHARED_DIR", "/tmp")

# Always fails mid-write: every write succeeds in appearance but stores nothing, so a payload smaller than the
# stream buffer only surfaces when the file is closed.
UNWRITABLE_DEVICE = "/dev/full"

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
        # fail, not skip: the module is built unconditionally, so its absence is a regression. A skip would exit 0 and
        # turn a packaging or dlopen failure into a green run with zero coverage.
        pytest.fail("the `export` query module is not loaded")
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


def id_space(element):
    """`n` for a node, `r` for a relationship, None for anything else (a property map, say).

    A top-level element carries `type`. An inlined endpoint does not, and is always a node — recognised by its key
    set rather than by `labels`, which is absent when the node has none.
    """
    if "type" in element:
        return {"node": "n", "relationship": "r"}.get(element["type"])
    if "id" in element and set(element) <= {"id", "labels", "properties"}:
        return "n"
    return None


def canonical_ids(element, seen):
    """Rewrites ids to first-appearance order. Nodes and relationships have separate id spaces."""
    if isinstance(element, dict):
        prefix = id_space(element)
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


@pytest.fixture(scope="module")
def server_files(conn):
    """Skips a test that inspects export files when the server writes them where this process cannot see."""
    probe = os.path.join(SERVER_SHARED_DIR, f"mage_export_json_probe_{os.getpid()}.json")
    with contextlib.suppress(mgclient.DatabaseError):
        execute(conn, f"CALL export.json_data([], [], '{probe}', {{}}) YIELD done RETURN done")
    if not os.path.exists(probe):
        pytest.skip("the server's export files are not reachable from the test process")
    os.remove(probe)


_export_paths = itertools.count()


@pytest.fixture
def export_path():
    """A fresh export path per test, removed afterwards together with anything created beside it."""
    path = os.path.join(SERVER_SHARED_DIR, f"mage_export_json_e2e_{os.getpid()}_{next(_export_paths)}.json")
    yield path
    for leftover in [path, *glob.glob(f"{path}.*")]:
        with contextlib.suppress(OSError):
            if os.path.isdir(leftover) and not os.path.islink(leftover):
                shutil.rmtree(leftover)
            else:
                os.remove(leftover)


def read_server_file(path):
    """Returns the server-written file's text, or None when the server is on a different filesystem."""
    try:
        with open(path, encoding="utf-8") as handle:
            return handle.read()
    except OSError:
        return None


def require_server_file(path):
    """Returns the server-written file's text; callers use `server_files`, so an unreadable file is a failure."""
    text = read_server_file(path)
    if text is None:
        pytest.fail(f"{path} is unreadable although the server's files are reachable")
    return text


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


@pytest.mark.parametrize(
    "json_format,expected",
    [("JSON", {"nodes": [], "rels": []}), ("JSON_ID_AS_KEYS", {"nodes": {}, "rels": {}})],
)
def test_empty_export_still_emits_the_wrapper_for_object_formats(conn, json_format, expected):
    # Only JSON_LINES collapses to "" — the object shapes must stay parseable, so json.loads(data)["nodes"] works on
    # an empty result set instead of raising.
    row = export(
        conn,
        call=f"export.json_data([], [], null, {{stream: true, jsonFormat: '{json_format}'}})",
        collect="",
    )
    assert row["data"] == expected
    assert (row["nodes"], row["relationships"], row["rows"]) == (0, 0, 0)


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


def test_duplicate_relationship_is_exported_once_in_first_seen_order(conn):
    # Nodes keep their duplicates, relationships do not: the reference holds them in a set.
    execute(conn, SEED_MIXED)
    row = export(conn, call="export.json_data(ns, [rs[1], rs[0], rs[1], rs[0]], null, {stream: true})")
    assert without_ids(row["data"][3:]) == without_ids([BOUGHT, KNOWS])
    assert (row["relationships"], row["rows"]) == (2, 5)
    assert row["properties"] == 9, "a skipped duplicate's properties are not counted"


def test_duplicate_node_collapses_under_id_as_keys(conn):
    execute(conn, "CREATE (:Product {sku: 'S1'})")
    row = export(
        conn,
        call="export.json_data([ns[0], ns[0]], [], null, {stream: true, jsonFormat: 'JSON_ID_AS_KEYS'})",
        canonicalize=False,
    )
    # Ids are object keys here, so a duplicate can only be one entry. The reference instead emits the
    # key twice, which is ambiguous JSON that parsers collapse the same way.
    assert row["raw_data"].count('"type":"node"') == 1, "a parser would hide a repeated key"
    assert (row["nodes"], row["properties"]) == (2, 2), "the counters still report both"


def test_labels_are_sorted_and_empty_properties_omitted(conn):
    execute(conn, "CREATE (:Zebra:Apple:Mango)")
    assert export(conn)["data"] == [{"type": "node", "id": "n0", "labels": ["Apple", "Mango", "Zebra"]}]


@pytest.mark.parametrize("json_format", ["JSON_LINES", "JSON", "JSON_ID_AS_KEYS"])
def test_unlabeled_node_omits_the_labels_key(conn, json_format):
    # `labels` follows the same omit-when-empty rule as `properties`, for the element and for an inlined endpoint.
    execute(conn, "CREATE (a {p: 1}), (b:Lbl {q: 2}), (a)-[:R]->(b)")
    row = export(conn, f"{{stream: true, jsonFormat: '{json_format}'}}")
    elements = row["data"] if json_format == "JSON_LINES" else [*row["data"]["nodes"], *row["data"]["rels"]]
    if json_format == "JSON_ID_AS_KEYS":
        elements = [*row["data"]["nodes"].values(), *row["data"]["rels"].values()]
    bare, labelled, relationship = elements
    assert bare == {"type": "node", "id": "n0", "properties": {"p": 1}}
    assert labelled["labels"] == ["Lbl"]
    assert relationship["start"] == {"id": "n0", "properties": {"p": 1}}, "endpoints follow the same rule"
    assert relationship["end"]["labels"] == ["Lbl"]


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
        # Diverges from the reference (`2020-01-01T10:00Z`) on the *input* side, not here: Memgraph binds a trailing
        # `Z` to the named zone Etc/UTC, so the "bracket iff named" rule then applies. The `+00:00` row above, where
        # both engines store an anonymous offset, is byte-identical on both.
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


@pytest.mark.parametrize("key", ["relationships", "edges"])
def test_json_graph_accepts_edges_as_an_alias_for_relationships(conn, key):
    # `edges` is the key project() produces, and reading only `relationships` silently exported none of them.
    execute(conn, SEED_MIXED)
    row = export(conn, call=f"export.json_graph({{nodes: ns, {key}: rs}}, null, {{stream: true}})")
    assert (row["nodes"], row["relationships"]) == (3, 2)
    assert row["source"] == "graph: nodes(3), rels(2)"
    assert row["data"] == ALL_ELEMENTS


def test_json_graph_composes_with_project(conn):
    execute(conn, SEED_MIXED)
    row = export(
        conn,
        call="export.json_graph({nodes: g.nodes, edges: g.edges}, null, {stream: true})",
        collect="MATCH p = ()-[]->() WITH project(p) AS g ",
    )
    assert (row["nodes"], row["relationships"]) == (3, 2)


@pytest.mark.parametrize(
    "graph,missing",
    [
        ("{}", "nodes"),
        ("{nodes: null, relationships: null}", "nodes"),
        ("{node: ns, rels: rs}", "nodes"),  # both keys mistyped
        ("{nodes: ns}", "relationships"),
        ("{nodes: ns, relationships: null}", "relationships"),
    ],
)
def test_json_graph_requires_both_halves(conn, graph, missing):
    # Silence here means exporting half a graph — or none of it — under `done: true`, which is exactly what a
    # mistyped key produces. The reference throws for either half missing.
    execute(conn, SEED_MIXED)
    with pytest.raises(Exception, match=f"no '{missing}' key"):
        export(conn, call=f"export.json_graph({graph}, null, {{stream: true}})")


def test_json_graph_accepts_an_explicitly_empty_relationship_list(conn):
    # Distinct from the key being absent: `relationships: []` says "no relationships", and is honoured.
    execute(conn, SEED_MIXED)
    row = export(conn, call="export.json_graph({nodes: ns, relationships: []}, null, {stream: true})")
    assert (row["nodes"], row["relationships"]) == (3, 0)


@pytest.mark.parametrize("keys", ["relationships: [], edges: rs", "relationships: rs, edges: []"])
def test_json_graph_empty_list_does_not_shadow_the_other_key(conn, keys):
    # An empty list counts as absent when choosing between the two spellings, so it cannot drop a populated one.
    execute(conn, SEED_MIXED)
    row = export(conn, call=f"export.json_graph({{nodes: ns, {keys}}}, null, {{stream: true}})")
    assert (row["nodes"], row["relationships"]) == (3, 2)


@pytest.mark.parametrize(
    "graph,error",
    [
        ("{nodes: 1, relationships: rs}", "'nodes' must be a list or null"),
        ("{nodes: ns, relationships: [1]}", "'relationships' must contain only relationships"),
        ("{nodes: ns, edges: [ns[0]]}", "'edges' must contain only relationships"),
    ],
)
def test_json_graph_rejects_malformed_lists(conn, graph, error):
    execute(conn, SEED_MIXED)
    with pytest.raises(Exception, match=error):
        export(conn, call=f"export.json_graph({graph}, null, {{stream: true}})")


def test_json_graph_refuses_two_populated_relationship_keys(conn):
    # Picking one and ignoring the other would silently discard half the caller's input.
    execute(conn, SEED_MIXED)
    with pytest.raises(Exception, match="both 'relationships' and 'edges'"):
        export(conn, call="export.json_graph({nodes: ns, relationships: rs, edges: rs}, null, {stream: true})")


def test_file_write_returns_null_data(conn, export_path):
    execute(conn, SEED_MIXED)
    row = export(conn, call=f"export.json_data(ns, rs, '{export_path}', {{stream: true}})")
    assert row["file"] == export_path, "the file column echoes the argument"
    assert row["data"] is None, "a file argument wins over stream"
    assert row["nodes"] == 3, "the counters are unaffected by the sink"


def test_file_contents_match_the_stream(conn, server_files, export_path):
    execute(conn, SEED_MIXED)
    export(conn, call=f"export.json_data(ns, rs, '{export_path}', {{}})")
    written = require_server_file(export_path)
    assert parse(written) == ALL_ELEMENTS
    # The two sinks must not diverge: same graph, same bytes.
    assert written == export(conn)["raw_data"]


@pytest.mark.parametrize(
    "config,error",
    [
        # Rejected rather than ignored: a caller asking for gzip must not receive plaintext.
        ("{compression: 'gzip'}", "'compression' is not supported"),
        ("{charset: 'UTF-8'}", "'charset' is not supported"),
        ("{jsonFormat: 'NOPE'}", "Unknown jsonFormat"),
        ("{jsonFormat: 1}", "'jsonFormat' must be a string"),
        # The reference reads an unknown value as true, so `stream: 'ture'` there turns streaming on; reported here.
        ("{stream: 'ture'}", "'stream' must be a boolean"),
        ("{writeNodeProperties: 2}", "'writeNodeProperties' must be a boolean"),
    ],
)
def test_invalid_config_is_rejected(conn, config, error):
    execute(conn, SEED_MIXED)
    with pytest.raises(Exception, match=error):
        export(conn, config)


@pytest.mark.parametrize("spelling", ["'JSON'", "'json'", "'Json'"])
def test_json_format_is_case_insensitive(conn, spelling):
    execute(conn, "CREATE (:Product {sku: 'S1'})")
    assert export(conn, f"{{stream: true, jsonFormat: {spelling}}}")["data"]["rels"] == []


@pytest.mark.parametrize("truthy", ["'true'", "'yes'", "1", "'1'"])
def test_boolean_config_accepts_the_spellings_the_reference_coerces(conn, truthy):
    execute(conn, SEED_MIXED)
    assert export(conn, f"{{stream: {truthy}}}")["data"] == ALL_ELEMENTS


@pytest.mark.parametrize("falsy", ["false", "'false'", "'no'", "0", "'0'", "''"])
def test_falsy_config_spellings_suppress_properties(conn, falsy):
    execute(conn, "CREATE (:Product {sku: 'S1'})")
    row = export(conn, f"{{stream: true, writeNodeProperties: {falsy}}}")
    assert row["data"] == [{"type": "node", "id": "n0", "labels": ["Product"]}]


def test_unwritable_path_is_reported(conn):
    execute(conn, SEED_MIXED)
    with pytest.raises(Exception, match="Cannot open"):
        export(conn, call="export.json_data(ns, rs, '/proc/nope/x.json', {})")


def test_write_failure_after_open_is_reported(conn):
    # The payload is far smaller than the stream buffer, so the failure only shows when the file is closed; reporting
    # `done: true` for it would mean a silently empty export.
    execute(conn, SEED_MIXED)
    with pytest.raises(Exception, match="Failed writing"):
        export(conn, call=f"export.json_data(ns, rs, '{UNWRITABLE_DEVICE}', {{}})")


def test_failed_export_leaves_the_previous_file_intact(conn, server_files, export_path):
    # A new target goes through a temporary renamed into place on success, so a failure part-way through must leave
    # the previous export, not a truncated file.
    execute(conn, SEED_MIXED)
    export(conn, call=f"export.json_data(ns, rs, '{export_path}', {{}})")
    good = require_server_file(export_path)

    # An enum property is refused mid-export, after some elements have already been written.
    if "ExportTestEnum" not in [row["Enum Name"] for row in execute(conn, "SHOW ENUMS")]:
        execute(conn, "CREATE ENUM ExportTestEnum VALUES { Active, Done }")
    execute(conn, "MATCH (n) DETACH DELETE n")
    execute(conn, "CREATE (:Plain {a: 1}), (:WithEnum {v: ExportTestEnum::Active})")
    with pytest.raises(Exception, match="Cannot export an enum property"):
        export(conn, call=f"export.json_data(ns, rs, '{export_path}', {{}})")

    assert read_server_file(export_path) == good, "the previous export survives a failed one"
    # Globbed rather than named: the temporary carries a per-writer suffix, so asserting on a fixed
    # `<file>.part` would pass without testing anything.
    leftovers = glob.glob(f"{export_path}.*")
    assert leftovers == [], f"a partial file was left behind: {leftovers}"


def test_empty_file_argument_means_no_file(conn):
    execute(conn, SEED_MIXED)
    row = export(conn, call="export.json_data(ns, rs, '', {stream: true})")
    assert row["file"] == "", "the file column still echoes the argument"
    assert row["data"] == ALL_ELEMENTS, "an empty path is not a path; the payload still streams"


def test_null_config_is_accepted(conn):
    execute(conn, SEED_MIXED)
    row = export(conn, call="export.json_data(ns, rs, null, null)")
    assert (row["nodes"], row["relationships"]) == (3, 2)
    assert row["data"] is None


def test_non_finite_doubles_serialize_as_strings(conn):
    # JSON has no non-finite literals, and emitting null would be indistinguishable from a stored null.
    execute(conn, "CREATE (:N {pos: 1e308 * 10.0, neg: -1e308 * 10.0, nan: 0.0 / 0.0})")
    assert export(conn)["data"][0]["properties"] == {"pos": "Infinity", "neg": "-Infinity", "nan": "NaN"}


def test_enum_property_is_refused(conn):
    # The property path cannot resolve enum names, so the only thing this could write is "::".
    # Enums are not stored data and survive the per-test wipe, so creating one twice is an error, not a fresh start.
    if "ExportTestEnum" not in [row["Enum Name"] for row in execute(conn, "SHOW ENUMS")]:
        execute(conn, "CREATE ENUM ExportTestEnum VALUES { Active, Done }")
    execute(conn, "CREATE (:E {v: ExportTestEnum::Active})")
    with pytest.raises(Exception, match="Cannot export an enum property"):
        export(conn)


def test_sinkless_export_validates_exactly_like_one_with_a_sink(conn):
    # With no file and no stream the payload goes nowhere, but the elements are still built: the value checks live on
    # that path, so skipping it would let a dry run report `done: true` for a graph the very same call cannot export.
    if "ExportTestEnum" not in [row["Enum Name"] for row in execute(conn, "SHOW ENUMS")]:
        execute(conn, "CREATE ENUM ExportTestEnum VALUES { Active, Done }")
    execute(conn, "CREATE (:E {v: ExportTestEnum::Active})")
    with pytest.raises(Exception, match="Cannot export an enum property"):
        export(conn, call="export.json_data(ns, rs, null, {})")


def test_sinkless_export_still_reports_its_counters(conn):
    execute(conn, SEED_MIXED)
    row = export(conn, call="export.json_data(ns, rs, null, {})")
    assert (row["nodes"], row["relationships"], row["properties"], row["rows"]) == (3, 2, 9, 5)
    assert (row["file"], row["data"]) == (None, None), "nothing was retained, so there is no payload to return"


def test_successful_export_leaves_no_temporary_behind(conn, server_files, export_path):
    execute(conn, SEED_MIXED)
    export(conn, call=f"export.json_data(ns, rs, '{export_path}', {{}})")
    require_server_file(export_path)
    leftovers = glob.glob(f"{export_path}.*")
    assert leftovers == [], f"the temporary was not renamed or cleaned up: {leftovers}"


def make_target(path, mode, content="stale"):
    """Creates an export target owned by the test process with an explicit mode.

    When the server runs as the test user the target is server-owned and goes through the rename path; otherwise it
    is written in place. The modes give "other" the access a server running as another user needs.
    """
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(content)
    os.chmod(path, mode)
    return content


def test_export_preserves_the_target_file_mode(conn, server_files, export_path):
    # A target the server owns is replaced by a renamed temporary, which must carry the mode; any other target is
    # written in place and keeps it anyway.
    execute(conn, SEED_MIXED)
    # rw for owner and other, nothing for group: writable by the server, and unlike any default umask would give.
    make_target(export_path, 0o606)
    export(conn, call=f"export.json_data(ns, rs, '{export_path}', {{}})")
    require_server_file(export_path)
    mode = stat.S_IMODE(os.stat(export_path).st_mode)
    assert mode == 0o606, f"the rewrite changed the mode to {oct(mode)}"


def test_export_refuses_a_read_only_target(conn, server_files, export_path):
    # A rename needs write permission on the directory, not on the target, so a read-only target the server owns
    # would otherwise be replaced without an error.
    execute(conn, SEED_MIXED)
    before = make_target(export_path, 0o444)
    with pytest.raises(Exception, match="Cannot open"):
        export(conn, call=f"export.json_data(ns, rs, '{export_path}', {{}})")
    assert read_server_file(export_path) == before, "a refused export must not have touched the target"


def test_export_writes_through_a_symlink(conn, server_files, export_path):
    # `latest.json -> dumps/<date>.json` is an ordinary layout; renaming over the link would replace it with a
    # regular file and leave what it pointed at stale.
    real = export_path + ".real"
    link = export_path + ".link"
    execute(conn, SEED_MIXED)
    try:
        stale = make_target(real, 0o606)
        os.symlink(real, link)
        export(conn, call=f"export.json_data(ns, rs, '{link}', {{}})")
        assert os.path.islink(link), "the symlink was replaced by a regular file"
        assert require_server_file(real) != stale, "the export did not land on what the link pointed at"
    finally:
        for path in (link, real):
            if os.path.lexists(path):
                os.remove(path)


def test_export_creates_the_target_of_a_dangling_symlink(conn, server_files, export_path):
    # A link to the export that does not exist yet is the first run of the `latest.json` layout.
    real = export_path + ".new"
    link = export_path + ".link"
    execute(conn, SEED_MIXED)
    try:
        os.symlink(real, link)
        export(conn, call=f"export.json_data(ns, rs, '{link}', {{}})")
        assert os.path.islink(link), "the symlink was replaced by a regular file"
        assert parse(require_server_file(real)) == ALL_ELEMENTS
    finally:
        for path in (link, real):
            if os.path.lexists(path):
                os.remove(path)


def test_export_keeps_the_target_owner_and_group(conn, server_files, export_path):
    # Owner and group decide who the mode bits apply to; replacing the file would hand both to the server.
    execute(conn, SEED_MIXED)
    make_target(export_path, 0o666)
    other_groups = [gid for gid in os.getgroups() if gid != os.getegid()]
    if not other_groups:
        pytest.skip("a group change is visible only with a supplementary group to give the target")
    os.chown(export_path, -1, other_groups[0])
    before = os.stat(export_path)
    export(conn, call=f"export.json_data(ns, rs, '{export_path}', {{}})")
    after = os.stat(export_path)
    assert parse(require_server_file(export_path)) == ALL_ELEMENTS
    assert (after.st_uid, after.st_gid) == (before.st_uid, before.st_gid)


def test_export_through_a_hard_link_updates_every_name(conn, server_files, export_path):
    # Renaming over one name of a hard-linked file would detach it, leaving the other name with the old export.
    alias = export_path + ".alias"
    execute(conn, SEED_MIXED)
    make_target(export_path, 0o666)
    os.link(export_path, alias)
    export(conn, call=f"export.json_data(ns, rs, '{alias}', {{}})")
    assert parse(require_server_file(export_path)) == ALL_ELEMENTS


def test_json_graph_argument_error_leaves_a_hard_linked_target_intact(conn, server_files, export_path):
    # A hard-linked target is written in place, so nothing may open it before the arguments are known to be valid.
    alias = export_path + ".alias"
    execute(conn, SEED_MIXED)
    try:
        before = make_target(export_path, 0o666)
        os.link(export_path, alias)
        with pytest.raises(Exception, match="both 'relationships' and 'edges'"):
            export(conn, call=f"export.json_graph({{nodes: ns, relationships: rs, edges: rs}}, '{alias}', {{}})")
        with pytest.raises(Exception, match="must contain only nodes"):
            export(conn, call=f"export.json_graph({{nodes: [1], relationships: rs}}, '{alias}', {{}})")
        assert read_server_file(export_path) == before
    finally:
        for path in (alias, export_path):
            if os.path.lexists(path):
                os.remove(path)


@pytest.mark.parametrize("json_format", ["JSON_LINES", "JSON", "JSON_ID_AS_KEYS"])
def test_whole_database_export_keeps_no_per_element_state(conn, json_format):
    # The graph yields each element once, so json_all needs no record of the ones it has written.
    execute(conn, "UNWIND range(1, 100000) AS i CREATE (:M)-[:R]->(:M)")
    rows = execute(
        conn,
        f"CALL export.json_all(null, {{jsonFormat: '{json_format}'}}) PROCEDURE MEMORY LIMIT 1 MB "
        "YIELD nodes, relationships RETURN *",
    )
    assert rows == [{"nodes": 200000, "relationships": 100000}]


ACL_ACCESS = "system.posix_acl_access"


def has_acl(path):
    try:
        os.getxattr(path, ACL_ACCESS)
        return True
    except OSError:
        return False


@pytest.mark.skipif(shutil.which("setfacl") is None, reason="needs setfacl")
def test_export_keeps_the_target_acl(conn, server_files, export_path):
    # An ACL lives on the inode, and with one the mode's group bits are its mask, so a replaced file would widen.
    execute(conn, SEED_MIXED)
    make_target(export_path, 0o606)
    subprocess.run(["setfacl", "-m", "u:4242:r", export_path], check=True)
    before = os.getxattr(export_path, ACL_ACCESS)
    export(conn, call=f"export.json_data(ns, rs, '{export_path}', {{}})")
    assert parse(require_server_file(export_path)) == ALL_ELEMENTS
    assert has_acl(export_path), "the export dropped the target's ACL"
    assert os.getxattr(export_path, ACL_ACCESS) == before


@pytest.mark.skipif(shutil.which("setfacl") is None, reason="needs setfacl")
def test_export_does_not_add_the_directory_default_acl_to_an_existing_target(conn, server_files, export_path):
    # A new file inherits the directory's default ACL; an existing one without an ACL must not acquire it.
    directory = export_path + ".acl"
    target = os.path.join(directory, "export.json")
    execute(conn, SEED_MIXED)
    os.makedirs(directory, exist_ok=True)
    try:
        make_target(target, 0o666)
        subprocess.run(["setfacl", "-d", "-m", "u:4242:rw", directory], check=True)
        assert not has_acl(target)
        export(conn, call=f"export.json_data(ns, rs, '{target}', {{}})")
        assert parse(require_server_file(target)) == ALL_ELEMENTS
        assert not has_acl(target), "the export gave the target the directory's default ACL"
    finally:
        shutil.rmtree(directory)
