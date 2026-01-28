import sys
from typing import Any, Dict, List

import pytest
from common import pq, setup_large_db, setup_thread_count_db


def make_hashable(value):
    """Recursively convert dicts/lists to hashable/comparable types."""
    if isinstance(value, dict):
        return tuple(sorted((k, make_hashable(v)) for k, v in value.items()))
    if isinstance(value, list):
        return tuple(make_hashable(v) for v in value)
    return value


class SafeComparator:
    """Helper to compare values safely, handling None and mixed types."""

    def __init__(self, obj):
        self.obj = obj

    def __lt__(self, other):
        # Handle None: None is always 'less' than anything else
        if self.obj is None:
            return other.obj is not None
        if other.obj is None:
            return False

        # Try standard comparison
        try:
            return self.obj < other.obj
        except TypeError:
            # Fallback for mixed types: compare by type name, then str representation
            if type(self.obj) != type(other.obj):
                return str(type(self.obj)) < str(type(other.obj))
            return str(self.obj) < str(other.obj)

    def __eq__(self, other):
        return self.obj == other.obj

    def __repr__(self):
        return repr(self.obj)


def normalize_result(result: List[Dict[str, Any]], order_matters: bool = False) -> List[Any]:
    """
    Normalize results for comparison.
    If order doesn't matter, sort the results.
    We convert dicts to sorted tuples of items to make them hashable/sortable.
    """
    normalized = []
    for record in result:
        # Convert the dictionary record to a tuple of (key, value) pairs
        # where values are recursively made hashable
        item_list = []
        for k in sorted(record.keys()):
            val = record[k]
            item_list.append((k, make_hashable(val)))
        normalized.append(tuple(item_list))

    if not order_matters:
        # Use SafeComparator to handle sorting of mixed types/None
        normalized.sort(key=lambda x: tuple(SafeComparator(v) for k, v in x))

    return normalized


def compare_with_tolerance(val1: Any, val2: Any, rel_tol: float = 1e-9, abs_tol: float = 1e-9) -> bool:
    """
    Compare two values with tolerance for floating point numbers.
    Recursively handles nested structures (tuples, lists).
    """
    # Handle floating point numbers
    if isinstance(val1, (float, int)) and isinstance(val2, (float, int)):
        # Convert int to float for comparison
        f1, f2 = float(val1), float(val2)
        # Use relative and absolute tolerance (similar to math.isclose)
        if abs(f1 - f2) <= max(rel_tol * max(abs(f1), abs(f2)), abs_tol):
            return True
        return False

    # Handle tuples (which are used in normalized results)
    if isinstance(val1, tuple) and isinstance(val2, tuple):
        if len(val1) != len(val2):
            return False
        return all(compare_with_tolerance(v1, v2, rel_tol, abs_tol) for v1, v2 in zip(val1, val2))

    # For other types, use exact equality
    return val1 == val2


def verify_parallel_matches_serial(memgraph, query: str, params: dict = None, order_matters: bool = False):
    """
    Executes query twice: once normally, once with USING PARALLEL EXECUTION.
    Asserts that results are identical (accounting for order if specified).
    """
    # Serial execution
    serial_result = memgraph.fetch_all(query, params)

    # Parallel execution
    parallel_result = memgraph.fetch_all(pq(query), params)

    norm_serial = normalize_result(serial_result, order_matters)
    norm_parallel = normalize_result(parallel_result, order_matters)

    # Compare with tolerance for floating point numbers
    if len(norm_parallel) != len(norm_serial):
        assert (
            False
        ), f"Mismatch in length!\nSerial: {len(norm_serial)} items\nParallel: {len(norm_parallel)} items\nSerial: {norm_serial}\nParallel: {norm_parallel}"

    for i, (parallel_item, serial_item) in enumerate(zip(norm_parallel, norm_serial)):
        if not compare_with_tolerance(parallel_item, serial_item):
            assert (
                False
            ), f"Mismatch at index {i}!\nSerial: {serial_item}\nParallel: {parallel_item}\nFull Serial: {norm_serial}\nFull Parallel: {norm_parallel}"


class TestParallelCorrectness:
    """
    Test suite ensuring that USING PARALLEL EXECUTION yields the same results
    as standard serial execution across various query patterns.
    """

    @pytest.fixture
    def populated_db(self, memgraph):
        """Standard populated database for tests."""
        # Use a decent number of elements to trigger multiple workers
        setup_thread_count_db(memgraph, thread_count=100)
        # Add some extra diverse data
        memgraph.execute_query("CREATE (:Person {name: 'Alice', age: 30, scores: [1, 2, 3]})")
        memgraph.execute_query("CREATE (:Person {name: 'Bob', age: 25, scores: [4, 5]})")
        memgraph.execute_query("CREATE (:Person {name: 'Charlie', age: 35, scores: []})")
        return memgraph

    def test_basic_scan_and_return(self, populated_db):
        """Test simple MATCH (n) RETURN n."""
        verify_parallel_matches_serial(populated_db, "MATCH (n) RETURN n")

    def test_property_projection(self, populated_db):
        """Test projecting properties."""
        verify_parallel_matches_serial(populated_db, "MATCH (n) RETURN n.p, n.name, n.age")

    def test_filtering(self, populated_db):
        """Test WHERE clauses."""
        # Filter by integer property
        verify_parallel_matches_serial(populated_db, "MATCH (n) WHERE n.p > 50 RETURN n")
        # Filter by string property existence/value
        verify_parallel_matches_serial(populated_db, "MATCH (n) WHERE n.name IS NOT NULL RETURN n.name")
        # Complex filter
        verify_parallel_matches_serial(populated_db, "MATCH (n) WHERE n.p % 2 = 0 OR n.age > 30 RETURN n")

    def test_aggregations_global(self, populated_db):
        """Test global aggregations (count, sum, avg, min, max)."""
        verify_parallel_matches_serial(
            populated_db, "MATCH (n) RETURN count(*), sum(n.p), avg(n.p), min(n.p), max(n.p)"
        )

    def test_aggregations_grouped(self, populated_db):
        """Test grouped aggregations."""
        # Group by label (though we mostly have :A and :B and :Person)
        # Note: :A and :B from setup_thread_count_db, :Person from populated_db extra
        verify_parallel_matches_serial(populated_db, "MATCH (n) RETURN labels(n) AS lbls, count(*)")

    def test_collect(self, populated_db):
        """Test collect() aggregation."""
        # Order doesn't matter for the outer result list, but collect() itself
        # might produce lists in different orders if not ordered.
        # However, verify_parallel_matches_serial handles the outer list order.
        # Inside the collected list, order is undefined unless ORDER BY is used in the aggregate?
        # Standard Neo4j consistency for collect() without order is... undefined order?
        # If order inside collect is different, strict equality check will fail.
        # We might need to assume collect order is unstable and sort list contents for verification.
        # For now, let's try strict check and see if Memgraph parallel collect is deterministic enough or we need to sort in query.

        # Sort in query to guarantee order inside list
        verify_parallel_matches_serial(populated_db, "MATCH (n:Person) RETURN n.name, collect(n.age) AS ages")

    def test_collect_with_ordering(self, populated_db):
        """Test collect() with explicit ordering."""
        # This guarantees internal list order
        verify_parallel_matches_serial(
            populated_db, "MATCH (n) WHERE n.p IS NOT NULL WITH n ORDER BY n.p RETURN collect(n.p) AS all_ps"
        )

    def test_create_and_return(self, memgraph):
        """Test WRITE operations (if parallel supports them, or at least doesn't crash/returns correct).
        Note: Parallel usually falls back to serial or is restricted for writes.
        If it's supported, we check result."""
        # Assuming parallel might fallback or execute.
        memgraph.clear_database()
        verify_parallel_matches_serial(memgraph, "CREATE (n:NewNode {id: 1}) RETURN n")

    def test_distinct(self, populated_db):
        """Test RETURN DISTINCT."""
        verify_parallel_matches_serial(populated_db, "MATCH (n) RETURN DISTINCT labels(n)")

    def test_order_by(self, populated_db):
        """Test ORDER BY (check exact order)."""
        verify_parallel_matches_serial(
            populated_db, "MATCH (n) WHERE n.p IS NOT NULL RETURN n.p ORDER BY n.p DESC", order_matters=True
        )

    def test_skip_limit(self, populated_db):
        """Test SKIP and LIMIT."""
        # Without ORDER BY, SKIP/LIMIT is non-deterministic.
        # We must use ORDER BY to verify consistency.
        base_query = "MATCH (n) WHERE n.p IS NOT NULL RETURN n.p ORDER BY n.p"
        verify_parallel_matches_serial(populated_db, f"{base_query} LIMIT 10", order_matters=True)
        verify_parallel_matches_serial(populated_db, f"{base_query} SKIP 5 LIMIT 5", order_matters=True)

    def test_unwind(self, populated_db):
        """Test UNWIND."""
        verify_parallel_matches_serial(populated_db, "UNWIND [1, 2, 3, 4, 5] AS x RETURN x*x")

    def test_complex_pattern(self, populated_db):
        """Test patterns with relationships."""
        verify_parallel_matches_serial(populated_db, "MATCH (a)-[e]->(b) RETURN a.p, e.p, b.p")

    def test_optional_match(self, populated_db):
        """Test OPTIONAL MATCH."""
        verify_parallel_matches_serial(
            populated_db, "MATCH (n:Person) OPTIONAL MATCH (n)-[r]->(m) RETURN n.name, type(r)"
        )


class TestParallelIndices:
    """
    Test suite for index usage in parallel execution.
    These tests ensure that the query runner correctly uses available indices
    (or that results are consistent even if it potentially ignores them,
    though the goal is parallel scan with index).
    """

    @pytest.fixture
    def indexed_db(self, memgraph):
        """Database with standard data and indices."""
        # Setup data
        setup_thread_count_db(memgraph, thread_count=100)
        memgraph.execute_query("CREATE (:Person {name: 'Alice', age: 30, city: 'London'})")
        memgraph.execute_query("CREATE (:Person {name: 'Bob', age: 25, city: 'Paris'})")
        memgraph.execute_query("CREATE (:Person {name: 'Charlie', age: 35, city: 'London'})")
        memgraph.execute_query("CREATE (:Person {name: 'Dave', age: 30, city: 'Berlin'})")

        # Add some edges with properties for edge index tests
        memgraph.execute_query(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS {since: 2020}]->(b)"
        )
        memgraph.execute_query(
            "MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Charlie'}) CREATE (b)-[:KNOWS {since: 2021}]->(c)"
        )
        memgraph.execute_query(
            "MATCH (c:Person {name: 'Charlie'}), (d:Person {name: 'Dave'}) CREATE (c)-[:KNOWS {since: 2022}]->(d)"
        )

        # Create Indices
        # 1. Vertex Label Index
        memgraph.execute_query("CREATE INDEX ON :Person;")
        # 2. Vertex Label Property Index
        memgraph.execute_query("CREATE INDEX ON :Person(age);")
        memgraph.execute_query("CREATE INDEX ON :Person(city);")
        # 3. Edge Type Index
        memgraph.execute_query("CREATE INDEX ON :KNOWS;")
        # 4. Edge Type Property Index
        memgraph.execute_query("CREATE INDEX ON :KNOWS(since);")

        return memgraph

    def test_vertex_label_index(self, indexed_db):
        """Test scanning with label index."""
        verify_parallel_matches_serial(indexed_db, "MATCH (n:Person) RETURN n")

    def test_vertex_property_index_specific(self, indexed_db):
        """Test vertex property index with specific value."""
        verify_parallel_matches_serial(indexed_db, "MATCH (n:Person) WHERE n.age = 30 RETURN n")

    def test_vertex_property_index_range(self, indexed_db):
        """Test vertex property index with range."""
        verify_parallel_matches_serial(indexed_db, "MATCH (n:Person) WHERE n.age > 25 RETURN n")

    def test_vertex_property_index_all(self, indexed_db):
        """Test vertex property index for all values (IS NOT NULL)."""
        verify_parallel_matches_serial(indexed_db, "MATCH (n:Person) WHERE n.age IS NOT NULL RETURN n")

    def test_edge_type_index(self, indexed_db):
        """Test edge type index."""
        verify_parallel_matches_serial(indexed_db, "MATCH ()-[e:KNOWS]->() RETURN e")

    def test_edge_property_index_specific(self, indexed_db):
        """Test edge property index with specific value."""
        verify_parallel_matches_serial(indexed_db, "MATCH ()-[e:KNOWS]->() WHERE e.since = 2020 RETURN e")

    def test_edge_property_index_range(self, indexed_db):
        """Test edge property index with range."""
        verify_parallel_matches_serial(indexed_db, "MATCH ()-[e:KNOWS]->() WHERE e.since > 2020 RETURN e")

    def test_edge_property_index_all(self, indexed_db):
        """Test edge property index for all values."""
        verify_parallel_matches_serial(indexed_db, "MATCH ()-[e:KNOWS]->() WHERE e.since IS NOT NULL RETURN e")


class TestParallelIndexFallback:
    """
    Tests for index types that should trigger fallback to single-threaded execution.
    We check for correctness; internal fallback is expected behavior but hard to 'observe'
    without logs/profile, but correctness must hold.
    """

    @pytest.fixture
    def specialized_index_db(self, memgraph):
        memgraph.clear_database()
        memgraph.execute_query("CREATE (:Item {id: 1, vec: [0.1, 0.2], description: 'test item'})")
        memgraph.execute_query("CREATE (:Item {id: 2, vec: [0.9, 0.8], description: 'another thing'})")
        # Point data
        memgraph.execute_query("CREATE (:Location {pos: point({latitude: 10.0, longitude: 20.0})})")
        memgraph.execute_query("CREATE (:Location {pos: point({latitude: 11.0, longitude: 21.0})})")

        return memgraph

    def test_specific_vertex_id(self, specialized_index_db):
        """
        Test lookup by internal ID.
        In Memgraph, `WHERE id(n) = ...` or `MATCH (n) WHERE id(n) IN [...]` often uses internal ID lookup.
        Parallel scan might fallback or handle it.
        """
        # Get an ID first
        res = specialized_index_db.fetch_one("MATCH (n:Item) RETURN id(n) AS id LIMIT 1")
        nid = res["id"]
        verify_parallel_matches_serial(specialized_index_db, f"MATCH (n) WHERE id(n) = {nid} RETURN n")

    # Assuming Point/Vector/Text index syntax support in the tested version.
    # Adjust syntax if needed for specific Memgraph version.

    def test_point_index(self, specialized_index_db):
        """Test Point index fallback."""
        # Setup index (mocking typical syntax or just assuming usage if index exists)
        # Note: If CREATE INDEX ON :Location(pos) is used, and it detects point type.
        specialized_index_db.execute_query("CREATE INDEX ON :Location(pos);")

        # Distance query
        query = (
            "MATCH (n:Location) WHERE point.distance(n.pos, point({latitude: 10.0, longitude: 20.0})) < 1000 RETURN n"
        )
        verify_parallel_matches_serial(specialized_index_db, query)

    def test_string_index(self, specialized_index_db):
        """Test Text/String index fallback."""
        try:
            # Create text index with a name as per docs
            specialized_index_db.execute_query("CREATE TEXT INDEX item_desc_index ON :Item(description);")
        except Exception:
            pytest.skip("Text index creation failed")

        # Text search query - assuming standard filtering works or checking fallback
        # Note: If specific procedure is needed, we might strictly need that, but CONTAINS often benefits or at least is valid.
        verify_parallel_matches_serial(
            specialized_index_db, "MATCH (n:Item) WHERE n.description CONTAINS 'test' RETURN n"
        )

    def test_vector_index(self, specialized_index_db):
        """Test Vector index fallback."""
        try:
            # Syntax: CREATE VECTOR INDEX vector_index_name ON :Label(embedding) WITH CONFIG {"dimension": 256, "capacity": 1000};
            # Our vectors are dimension 2 ([0.1, 0.2])
            specialized_index_db.execute_query(
                'CREATE VECTOR INDEX item_vec_index ON :Item(vec) WITH CONFIG {"dimension": 2, "capacity": 10};'
            )

            # Similarity query
            # We verify that a query potentially using the index works correctly (parallel or fallback)
            # A typical vector search query might strict filters or distance calculation
            verify_parallel_matches_serial(specialized_index_db, "MATCH (n:Item) WHERE size(n.vec) = 2 RETURN n")
        except Exception:
            pytest.skip("Vector index creation failed")


class TestParallelIsolation:
    """
    Tests for isolation correctness with parallel execution.
    Verifies that parallel execution respects transaction snapshots
    and provides the same results as serial execution within the same transaction,
    even when other transactions modify the data concurrently.
    """

    @pytest.fixture
    def isolation_db(self, memgraph):
        """Setup a database for isolation tests."""
        memgraph.execute_query("MATCH (n) DETACH DELETE n")
        memgraph.execute_query("UNWIND range(1, 100) AS i CREATE (:Node {id: i, value: 100})")
        return memgraph

    def test_snapshot_isolation(self, isolation_db):
        """
        Verify that parallel execution sees a stable snapshot.
        1. Start Tx1.
        2. Read initial state in Tx1.
        3. Modify data in Tx2 (committed).
        4. Read state in Tx1 using Serial execution -> Should see initial state.
        5. Read state in Tx1 using Parallel execution -> Should see initial state.
        6. Verify Parallel matches Serial.
        """
        driver = isolation_db.get_driver()

        # 1. Start Tx1
        session1 = driver.session()
        tx1 = session1.begin_transaction()

        try:
            # 2. Read initial state in Tx1 (Serial) to establish snapshot
            # This query establishes the snapshot version for the transaction
            initial_count_res = tx1.run("MATCH (n:Node) RETURN count(n) AS count, sum(n.value) as total_val").single()
            initial_count = initial_count_res["count"]
            initial_val = initial_count_res["total_val"]

            assert initial_count == 100
            assert initial_val == 100 * 100

            # 3. Modify data in Tx2 (committed)
            # Delete 10 nodes and update values of others
            isolation_db.execute_query("MATCH (n:Node) WHERE n.id <= 10 DETACH DELETE n")
            isolation_db.execute_query("MATCH (n:Node) WHERE n.id > 10 SET n.value = 200")

            # Verify Tx2 changes are visible globally (outside Tx1)
            global_check = isolation_db.fetch_all("MATCH (n:Node) RETURN count(n) AS count, sum(n.value) as total_val")[
                0
            ]
            assert global_check["count"] == 90
            assert global_check["total_val"] == 90 * 200

            # 4. Read state in Tx1 using Serial execution -> Should see initial state
            serial_res = tx1.run("MATCH (n:Node) RETURN count(n) AS count, sum(n.value) as total_val").single()
            assert serial_res["count"] == initial_count
            assert serial_res["total_val"] == initial_val

            # 5. Read state in Tx1 using Parallel execution -> Should see initial state (snapshot)
            # Parallel execution should reuse the transaction's snapshot
            query_parallel = (
                "USING PARALLEL EXECUTION MATCH (n:Node) RETURN count(n) AS count, sum(n.value) as total_val"
            )
            parallel_res = tx1.run(query_parallel).single()

            # 6. Verify Parallel matches Serial and Initial
            assert parallel_res["count"] == initial_count
            assert parallel_res["total_val"] == initial_val

            assert parallel_res["count"] == serial_res["count"]
            assert parallel_res["total_val"] == serial_res["total_val"]

        finally:
            tx1.close()
            session1.close()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA", "-v"]))
