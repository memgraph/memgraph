---
name: memgraph-mage-e2e-testing
description: Build a MAGE Docker image and run e2e tests for MAGE query modules. Use when the user wants to test MAGE modules end-to-end, run the MAGE e2e test suite, or verify query module changes in a Docker container.
---

# Testing MAGE Query Modules (E2E)

Two steps: build a Docker image with the latest code, then run the e2e test suite against it.

## Step 1 — Build the MAGE Docker Image

From the repository root (`memgraph/`):

```bash
./tools/ci/mage-build/build-docker-image.sh --image-tag <tag>
```

This compiles C++, Rust, and Python modules and packages them into `memgraph/memgraph-mage:<tag>`. See the `memgraph-generate-docker-images` skill for full build options.

## Step 2 — Run the Container

```bash
docker run --rm -d -p 7687:7687 -p 7444:7444 --name mage-test memgraph/memgraph-mage:<tag>
```

Wait a few seconds for Memgraph to start before running tests.

## Step 3 — Run E2E Tests

Activate the shared virtual environment and run the test runner from `mage/tests/`:

```bash
source tests/query_modules/ve3/bin/activate
cd mage/tests
python3 test_e2e
```

### Filtering Tests

Use `-k` to run tests for a specific module:

```bash
python3 test_e2e -k "igraphalg"
python3 test_e2e -k "pagerank"
python3 test_e2e -k "igraphalg and not leiden"
```

The `-k` flag is passed through to pytest, so any pytest filter expression works.

### Connecting to a Non-Default Host/Port

The test fixtures read `MG_HOST` and `MG_PORT` from the environment (defaults: `127.0.0.1:7687`):

```bash
MG_HOST=192.168.1.10 MG_PORT=17687 python3 test_e2e -k "igraphalg"
```

## How E2E Tests Work

Each test is a subdirectory under `mage/tests/e2e/<module>_test/`:

```
mage/tests/e2e/igraphalg_test/
├── test_pagerank_normal_directed/
│   ├── input.cyp        # Cypher statements to set up the graph
│   └── test.yml         # Query to run + expected output
├── test_betweenness_centrality_directed/
│   ├── input.cyp
│   └── test.yml
└── ...
```

- **`input.cyp`** — Cypher statements executed before the test (graph setup). Can be empty for empty-graph tests.
- **`test.yml`** — Contains the `query` to execute and the `output` to assert against.

Example `test.yml`:

```yaml
query: >
  CALL igraphalg.betweenness_centrality()
  YIELD node, betweenness
  RETURN node.id as node_id, betweenness
  ORDER BY node_id ASC

output:
  - node_id: 0
    betweenness: 0.0
  - node_id: 1
    betweenness: 3.0
```

The test framework (`mage/tests/e2e/test_module.py`) discovers all subdirectories, runs `input.cyp`, executes the query, and asserts the result matches `output`. The database is cleared (`DROP DATABASE`) between tests.

## Full Example Workflow

```bash
# 1. Build
./tools/ci/mage-build/build-docker-image.sh --image-tag my-feature

# 2. Run container
docker run --rm -d -p 7687:7687 -p 7444:7444 --name mage-test memgraph/memgraph-mage:my-feature

# 3. Activate venv and run tests
source tests/query_modules/ve3/bin/activate
cd mage/tests
python3 test_e2e -k "igraphalg"

# 4. Clean up
docker stop mage-test
```

## Key Files

| File | Purpose |
|------|---------|
| `mage/tests/test_e2e` | Test runner script (wraps pytest) |
| `mage/tests/e2e/test_module.py` | Pytest test logic — discovers and runs test directories |
| `mage/tests/e2e/conftest.py` | Pytest fixtures (`db` connects to Memgraph via `MG_HOST`/`MG_PORT`) |
| `mage/tests/e2e/pytest.ini` | Pytest config (`--maxfail=10`, verbose output) |
| `mage/tests/e2e/<module>_test/` | Test directories, one per module |
| `tests/query_modules/ve3/` | Shared Python venv with test dependencies (pytest, gqlalchemy, PyYAML) |
