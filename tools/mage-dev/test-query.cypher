call mg.load_all();
CALL embeddings.compute(NULL, "embedding", NULL, "all-MiniLM-L6-v2", 2000, 48, "cuda") YIELD success;
MATCH (n)
WHERE n.embedding IS NOT NULL
RETURN n, n.embedding
LIMIT 5;
