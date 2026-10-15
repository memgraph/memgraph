"""
E2E test: parallel embeddings requests against a live Memgraph instance.

This test spawns several processes that open concurrent connections
and fire embeddings.text() calls simultaneously to verify the
query-module behaves correctly under truly parallel load.

Run with:
    pytest test_parallel_embeddings.py -v

Requires a running Memgraph with the embeddings query module loaded.
"""

from concurrent.futures import ProcessPoolExecutor, as_completed

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
    """Fire multiple embeddings.text() calls at the same time
    using only the read-only text embedding endpoint."""

    def test_concurrent_text_embed(self, memgraph_available):
        """All parallel requests must succeed and return the
        correct embedding dimension."""
        texts = [f"Sentence number {i}" for i in range(NUM_WORKERS)]

        with ProcessPoolExecutor(max_workers=NUM_WORKERS) as pool:
            futs = {pool.submit(_run_embed_query, t): i for i, t in enumerate(texts)}
            for fut in as_completed(futs):
                i = futs[fut]
                r = fut.result()
                assert r["success"] is True, f"Worker {i} failed"
                assert r["num"] == 1
                assert r["dim"] == EXPECTED_DIMENSION
                assert r["dimension"] == EXPECTED_DIMENSION

    def test_concurrent_text_embed_multiple_strings(self, memgraph_available):
        """Each worker embeds a batch of strings in parallel."""

        def make_batch(worker_id):
            return [f"Worker {worker_id} sentence {j}" for j in range(5)]

        with ProcessPoolExecutor(max_workers=NUM_WORKERS) as pool:
            futs = {}
            for w in range(NUM_WORKERS):
                batch = make_batch(w)
                # Send all 5 strings in one call
                futs[pool.submit(_run_batch_query, batch)] = w

            for fut in as_completed(futs):
                w = futs[fut]
                r = fut.result()
                assert r["success"] is True, f"Worker {w} failed"
                assert r["num"] == 5
                assert r["dim"] == EXPECTED_DIMENSION


def _run_batch_query(texts: list) -> dict:
    """Embed a list of strings via embeddings.text()."""
    conn = mgclient.connect(host=MEMGRAPH_HOST, port=MEMGRAPH_PORT)
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(
        "WITH {device: 'cpu'} AS cfg "
        "CALL embeddings.text($texts, cfg) "
        "YIELD success, embeddings, dimension "
        "RETURN success, "
        "       size(embeddings) AS num, "
        "       size(embeddings[0]) AS dim, "
        "       dimension",
        {"texts": texts},
    )
    row = cursor.fetchone()
    conn.close()
    return {
        "success": row[0],
        "num": row[1],
        "dim": row[2],
        "dimension": row[3],
    }
