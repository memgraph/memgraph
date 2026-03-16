"""
E2E test: parallel embeddings requests against a live Memgraph instance.

This test opens several concurrent connections and fires
embeddings.text() calls simultaneously to verify thread-safety
of the query-module under real load.

Run with:
    pytest test_parallel_embeddings.py -v

Requires a running Memgraph with the embeddings query module loaded.
"""

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import mgclient
import pytest

MEMGRAPH_HOST = "127.0.0.1"
MEMGRAPH_PORT = 7687
NUM_WORKERS = 4
EXPECTED_DIMENSION = 384


def _run_embed_query(text: str) -> dict:
    """Open a fresh connection, call embeddings.text(), return result."""
    conn = mgclient.connect(host=MEMGRAPH_HOST, port=MEMGRAPH_PORT)
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(
        "WITH {device: 'cpu'} AS cfg "
        "CALL embeddings.text([$text], cfg) "
        "YIELD success, embeddings, dimension "
        "RETURN success, "
        "       size(embeddings) AS num, "
        "       size(embeddings[0]) AS dim, "
        "       dimension",
        {"text": text},
    )
    row = cursor.fetchone()
    conn.close()
    return {
        "success": row[0],
        "num": row[1],
        "dim": row[2],
        "dimension": row[3],
    }


@pytest.fixture(scope="module")
def memgraph_available():
    """Skip the whole module if Memgraph is not reachable or
    embeddings module is not loaded."""
    try:
        conn = mgclient.connect(host=MEMGRAPH_HOST, port=MEMGRAPH_PORT)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("CALL mg.procedures() YIELD name RETURN name")
        names = {row[0] for row in cur.fetchall()}
        conn.close()
        if "embeddings.text" not in names:
            pytest.skip("embeddings module not loaded in Memgraph")
    except Exception:
        pytest.skip("Memgraph not reachable, skipping parallel e2e")


class TestParallelEmbeddings:
    """Fire multiple embeddings.text() calls at the same time."""

    def test_concurrent_text_embed(self, memgraph_available):
        """All parallel requests must succeed and return the
        correct embedding dimension."""
        texts = [f"Sentence number {i}" for i in range(NUM_WORKERS)]
        results = [None] * NUM_WORKERS
        errors = [None] * NUM_WORKERS
        barrier = threading.Barrier(NUM_WORKERS)

        def worker(idx):
            try:
                barrier.wait(timeout=10)
                results[idx] = _run_embed_query(texts[idx])
            except Exception as exc:
                errors[idx] = exc

        with ThreadPoolExecutor(max_workers=NUM_WORKERS) as pool:
            futs = [pool.submit(worker, i) for i in range(NUM_WORKERS)]
            for f in as_completed(futs):
                f.result()

        for i in range(NUM_WORKERS):
            assert errors[i] is None, f"Worker {i} raised: {errors[i]}"
            r = results[i]
            assert r is not None, f"Worker {i} returned None"
            assert r["success"] is True
            assert r["num"] == 1
            assert r["dim"] == EXPECTED_DIMENSION
            assert r["dimension"] == EXPECTED_DIMENSION

    def test_concurrent_node_sentence(self, memgraph_available):
        """Parallel node_sentence calls on graph data must all
        succeed without crashing."""
        conn = mgclient.connect(host=MEMGRAPH_HOST, port=MEMGRAPH_PORT)
        conn.autocommit = True
        cursor = conn.cursor()
        # Setup: create a few nodes
        cursor.execute("MATCH (n:__ParTestNode) DETACH DELETE n")
        for i in range(5):
            cursor.execute(
                "CREATE (:__ParTestNode {text: $t})",
                {"t": f"parallel test node {i}"},
            )
        conn.close()

        def run_node_sentence():
            c = mgclient.connect(host=MEMGRAPH_HOST, port=MEMGRAPH_PORT)
            c.autocommit = True
            cur = c.cursor()
            cur.execute(
                "MATCH (n:__ParTestNode) "
                "WITH collect(n) AS nodes, {device: 'cpu'} AS cfg "
                "CALL embeddings.node_sentence(nodes, cfg) "
                "YIELD success, dimension "
                "RETURN success, dimension"
            )
            row = cur.fetchone()
            c.close()
            return {"success": row[0], "dimension": row[1]}

        barrier = threading.Barrier(NUM_WORKERS)
        results = [None] * NUM_WORKERS
        errors = [None] * NUM_WORKERS

        def worker(idx):
            try:
                barrier.wait(timeout=10)
                results[idx] = run_node_sentence()
            except Exception as exc:
                errors[idx] = exc

        with ThreadPoolExecutor(max_workers=NUM_WORKERS) as pool:
            futs = [pool.submit(worker, i) for i in range(NUM_WORKERS)]
            for f in as_completed(futs):
                f.result()

        # Cleanup
        conn = mgclient.connect(host=MEMGRAPH_HOST, port=MEMGRAPH_PORT)
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute("MATCH (n:__ParTestNode) DETACH DELETE n")
        conn.close()

        for i in range(NUM_WORKERS):
            assert errors[i] is None, f"Worker {i} raised: {errors[i]}"
            r = results[i]
            assert r is not None
            assert r["success"] is True
            assert r["dimension"] == EXPECTED_DIMENSION
