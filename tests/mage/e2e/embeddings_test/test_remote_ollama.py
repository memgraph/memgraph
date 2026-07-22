"""Opt-in e2e test: embeddings.* against a local Ollama daemon.

Skipped by default. To run locally:

    # 1. install & start ollama (https://ollama.com), then pull a small model
    ollama pull nomic-embed-text

    # 2. Ensure Memgraph+MAGE is running on localhost:7687. If Memgraph runs
    #    inside Docker and Ollama runs on the host, set MAGE_E2E_OLLAMA_BASE
    #    to a URL the Memgraph container can reach (e.g. http://host.docker.internal:11434).

    # 3. opt in and run
    export MAGE_E2E_OLLAMA=1
    pytest tests/e2e/embeddings_test/test_remote_ollama.py -v

Not run in CI: the gating env var is unset in the CI workflow.
"""

import os
from urllib.parse import urlparse
from urllib.request import Request, urlopen

import mgclient
import pytest

MEMGRAPH_HOST = os.environ.get("MAGE_E2E_MEMGRAPH_HOST", "127.0.0.1")
MEMGRAPH_PORT = int(os.environ.get("MAGE_E2E_MEMGRAPH_PORT", "7687"))
OLLAMA_BASE = os.environ.get("MAGE_E2E_OLLAMA_BASE", "http://127.0.0.1:11434")
OLLAMA_MODEL = os.environ.get("MAGE_E2E_OLLAMA_MODEL", "nomic-embed-text")
MODEL = f"ollama/{OLLAMA_MODEL}"

pytestmark = pytest.mark.skipif(
    not os.environ.get("MAGE_E2E_OLLAMA"),
    reason="MAGE_E2E_OLLAMA not set — skipping opt-in Ollama e2e test",
)


def _probe_ollama():
    """Return True if the Ollama daemon responds at OLLAMA_BASE."""
    try:
        parsed = urlparse(OLLAMA_BASE)
        assert parsed.scheme in ("http", "https")
        with urlopen(Request(f"{OLLAMA_BASE}/api/tags"), timeout=2) as r:
            return r.status == 200
    except Exception:
        return False


def _connect():
    conn = mgclient.connect(host=MEMGRAPH_HOST, port=MEMGRAPH_PORT)
    conn.autocommit = True
    return conn


@pytest.fixture(scope="module")
def db():
    if not _probe_ollama():
        pytest.skip(
            f"Ollama not reachable at {OLLAMA_BASE} — start it or set " "MAGE_E2E_OLLAMA_BASE to a reachable URL"
        )
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("CALL mg.procedures() YIELD name RETURN name")
        names = {row[0] for row in cur.fetchall()}
        if "embeddings.text" not in names:
            pytest.skip("embeddings module not loaded in Memgraph")
    except Exception as e:
        pytest.skip(f"Memgraph not reachable: {e}")
    yield
    c = _connect()
    c.cursor().execute("MATCH (n:E2ETestOllama) DETACH DELETE n")
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
        "{model_name: $m, api_base: $b}) "
        "YIELD success, embeddings, dimension "
        "RETURN success, embeddings, dimension",
        {"m": MODEL, "b": OLLAMA_BASE},
    )
    assert len(rows) == 1
    success, embeddings, dimension = rows[0]
    assert success is True, f"call failed; is '{OLLAMA_MODEL}' pulled? (ollama pull {OLLAMA_MODEL})"
    assert dimension is not None and dimension > 0
    assert len(embeddings) == 2
    for v in embeddings:
        assert len(v) == dimension
        magnitude = sum(x * x for x in v) ** 0.5
        assert abs(magnitude - 1.0) < 1e-3, f"expected normalized vector, got |v|={magnitude}"


def test_model_info_populates_dimension(db):
    rows = _run(
        "CALL embeddings.model_info({model_name: $m, api_base: $b}) YIELD info RETURN info",
        {"m": MODEL, "b": OLLAMA_BASE},
    )
    info = rows[0][0]
    assert info["model_name"] == MODEL
    assert info["dimension"] > 0
    assert info["max_sequence_length"] is None


def test_node_sentence_writes_back_property(db):
    _run("MATCH (n:E2ETestOllama) DETACH DELETE n")
    _run(
        "CREATE (:E2ETestOllama {title: 'Magnetospheric field line resonances', id: 1}),"
        "       (:E2ETestOllama {title: 'Vector indexes in graph databases',  id: 2})"
    )
    rows = _run(
        "MATCH (n:E2ETestOllama) WITH collect(n) AS nodes "
        "CALL embeddings.node_sentence(nodes, {model_name: $m, api_base: $b}) "
        "YIELD success, dimension "
        "RETURN success, dimension",
        {"m": MODEL, "b": OLLAMA_BASE},
    )
    success, dimension = rows[0]
    assert success is True
    assert dimension > 0

    check = _run("MATCH (n:E2ETestOllama) " "RETURN n.id AS id, size(n.embedding) AS dim " "ORDER BY n.id")
    assert [r[0] for r in check] == [1, 2]
    assert all(r[1] == dimension for r in check), "each node should carry a vector of the reported dimension"
