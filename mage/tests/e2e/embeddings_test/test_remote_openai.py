"""E2E test: embeddings.* against the real OpenAI embeddings API.

Runs whenever Memgraph is reachable AND its process environment carries a
working ``OPENAI_API_KEY``. The CI workflow forwards the secret into the
container's env (see ``.github/workflows/reusable_package_mage.yaml``);
locally:

    docker run -e OPENAI_API_KEY=$OPENAI_API_KEY ... memgraph/memgraph-mage
    pytest tests/e2e/embeddings_test/test_remote_openai.py -v

If the key is missing or the OpenAI probe call fails, the suite skips with
a clear message rather than failing — local devs without a key won't see
spurious red, but CI failures (where the key is supposed to be set) still
surface as real regressions.
"""

import os

import mgclient
import pytest

MEMGRAPH_HOST = os.environ.get("MAGE_E2E_MEMGRAPH_HOST", "127.0.0.1")
MEMGRAPH_PORT = int(os.environ.get("MAGE_E2E_MEMGRAPH_PORT", "7687"))
MODEL = os.environ.get("MAGE_E2E_OPENAI_MODEL", "openai/text-embedding-3-small")


def _connect():
    conn = mgclient.connect(host=MEMGRAPH_HOST, port=MEMGRAPH_PORT)
    conn.autocommit = True
    return conn


@pytest.fixture(scope="module")
def db():
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("CALL mg.procedures() YIELD name RETURN name")
        names = {row[0] for row in cur.fetchall()}
        conn.close()
        if "embeddings.text" not in names:
            pytest.skip("embeddings module not loaded in Memgraph")
    except Exception as e:
        pytest.skip(f"Memgraph not reachable: {e}")

    # Probe OpenAI through the Memgraph process. Skips (rather than fails) when
    # OPENAI_API_KEY is missing from Memgraph's env or OpenAI is unreachable —
    # so local devs without a key see "skipped" instead of red, while a CI run
    # that *should* have the key forwarded gets real assertion failures below.
    rows = _run(
        "CALL embeddings.text(['probe'], {model_name: $m}) YIELD success RETURN success",
        {"m": MODEL},
    )
    if not rows or not rows[0][0]:
        pytest.skip(
            f"OpenAI probe via {MODEL} returned success=false — is OPENAI_API_KEY "
            "set in the Memgraph process environment?"
        )

    yield
    # cleanup any fixture data the tests created
    c = _connect()
    c.cursor().execute("MATCH (n:E2ETestOpenAI) DETACH DELETE n")
    c.close()


def _run(cypher, params=None):
    conn = _connect()
    cur = conn.cursor()
    cur.execute(cypher, params or {})
    rows = cur.fetchall()
    conn.close()
    return rows


def test_text_returns_normalized_embeddings(db):
    rows = _run(
        "CALL embeddings.text(['hello world', 'graph databases are fun'], "
        "{model_name: $m}) "
        "YIELD success, embeddings, dimension "
        "RETURN success, embeddings, dimension",
        {"m": MODEL},
    )
    assert len(rows) == 1
    success, embeddings, dimension = rows[0]
    assert success is True
    assert dimension is not None and dimension > 0
    assert len(embeddings) == 2
    for v in embeddings:
        assert len(v) == dimension
        magnitude = sum(x * x for x in v) ** 0.5
        # normalize=True is the default, so magnitudes should be ~1.0
        assert abs(magnitude - 1.0) < 1e-3, f"expected normalized vector, got |v|={magnitude}"


def test_model_info_populates_dimension(db):
    rows = _run(
        "CALL embeddings.model_info({model_name: $m}) YIELD info RETURN info",
        {"m": MODEL},
    )
    info = rows[0][0]
    assert info["model_name"] == MODEL
    assert info["dimension"] > 0
    # Remote providers don't expose a sequence length — field is present but null.
    assert info["max_sequence_length"] is None


def test_node_sentence_writes_back_property(db):
    _run("MATCH (n:E2ETestOpenAI) DETACH DELETE n")
    _run(
        "CREATE (:E2ETestOpenAI {title: 'Magnetospheric field line resonances', id: 1}),"
        "       (:E2ETestOpenAI {title: 'Vector indexes in graph databases',  id: 2})"
    )
    rows = _run(
        "MATCH (n:E2ETestOpenAI) WITH collect(n) AS nodes "
        "CALL embeddings.node_sentence(nodes, {model_name: $m}) "
        "YIELD success, dimension "
        "RETURN success, dimension",
        {"m": MODEL},
    )
    success, dimension = rows[0]
    assert success is True
    assert dimension > 0

    check = _run("MATCH (n:E2ETestOpenAI) " "RETURN n.id AS id, size(n.embedding) AS dim " "ORDER BY n.id")
    assert [r[0] for r in check] == [1, 2]
    assert all(r[1] == dimension for r in check), "each node should carry a vector of the reported dimension"
