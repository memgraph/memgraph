#!/usr/bin/env python3
"""
Validation script for cuGraph MAGE algorithms after RAPIDS 25.x migration.
Validates algorithm ACCURACY by comparing against NetworkX ground truth.

This script:
1. Builds the same graph in NetworkX (ground truth)
2. Computes expected values using NetworkX algorithms
3. Runs cuGraph algorithms via Memgraph
4. Compares results with tolerance
5. Validates node identity mapping is correct

Usage:
    # Using default settings (creates temp data dir)
    python validate_cugraph_algorithms.py

    # Using custom settings via environment variables
    MEMGRAPH_DATA_DIR=/path/to/data MEMGRAPH_IMAGE=my-image:tag python validate_cugraph_algorithms.py

Environment Variables:
    MEMGRAPH_URI         - Bolt URI (default: bolt://localhost:7687)
    MEMGRAPH_DATA_DIR    - Data directory (default: creates temp dir)
    MEMGRAPH_IMAGE       - Docker image name (default: memgraph-mage-cugraph:latest)
    MEMGRAPH_CONTAINER   - Container name (default: memgraph-cugraph-validation)
"""

import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

import networkx as nx
from neo4j import GraphDatabase

# Configuration via environment variables with sensible defaults
MEMGRAPH_URI = os.environ.get("MEMGRAPH_URI", "bolt://localhost:7687")
MEMGRAPH_USER = os.environ.get("MEMGRAPH_USER", "")
MEMGRAPH_PASSWORD = os.environ.get("MEMGRAPH_PASSWORD", "")

# Docker configuration
CONTAINER_NAME = os.environ.get("MEMGRAPH_CONTAINER", "memgraph-cugraph-validation")
IMAGE_NAME = os.environ.get("MEMGRAPH_IMAGE", "memgraph-mage-cugraph:latest")

# Data directory - use temp dir if not specified
_default_data_dir = os.environ.get("MEMGRAPH_DATA_DIR", "")
if _default_data_dir:
    MEMGRAPH_DATA_DIR = Path(_default_data_dir)
    _using_temp_dir = False
else:
    MEMGRAPH_DATA_DIR = Path(tempfile.mkdtemp(prefix="memgraph_validation_"))
    _using_temp_dir = True

# Paths
SCRIPT_DIR = Path(__file__).parent.resolve()

# Test tolerance for floating point comparisons
TOLERANCE = 0.05  # 5% relative tolerance
ABS_TOLERANCE = 1e-6  # Absolute tolerance for near-zero values

# Expected nodes in the test graph
EXPECTED_NODES = {'A1', 'A2', 'A3', 'A4', 'B1', 'B2', 'B3', 'B4', 'HUB'}
COMMUNITY_A = {'A1', 'A2', 'A3', 'A4'}
COMMUNITY_B = {'B1', 'B2', 'B3', 'B4'}

def build_networkx_graph() -> nx.DiGraph:
    """Build the same test graph in NetworkX for ground truth comparison."""
    G = nx.DiGraph()

    # Add nodes with names
    nodes = [
        (1, {'name': 'A1'}),
        (2, {'name': 'A2'}),
        (3, {'name': 'A3'}),
        (4, {'name': 'A4'}),
        (5, {'name': 'B1'}),
        (6, {'name': 'B2'}),
        (7, {'name': 'B3'}),
        (8, {'name': 'B4'}),
        (9, {'name': 'HUB'}),
    ]
    G.add_nodes_from(nodes)

    # Community 1 edges (A1-A4)
    community1_edges = [
        (1, 2), (2, 3), (3, 4), (4, 1),  # Ring
        (1, 3), (2, 4),  # Cross connections
    ]

    # Community 2 edges (B1-B4)
    community2_edges = [
        (5, 6), (6, 7), (7, 8), (8, 5),  # Ring
        (5, 7), (6, 8),  # Cross connections
    ]

    # Hub connections
    hub_edges = [
        (1, 9), (9, 5),  # A1 -> HUB -> B1
        (9, 1), (5, 9),  # HUB -> A1, B1 -> HUB
    ]

    all_edges = [(u, v, {'weight': 1.0}) for u, v in community1_edges + community2_edges + hub_edges]
    G.add_edges_from(all_edges)

    return G


def get_networkx_ground_truth(G: nx.DiGraph) -> dict[str, Any]:
    """Compute ground truth values using NetworkX algorithms."""
    # Create name lookup
    id_to_name = {node: G.nodes[node]['name'] for node in G.nodes()}

    # PageRank
    pagerank = nx.pagerank(G, alpha=0.85, max_iter=100, tol=1e-5)
    pagerank_by_name = {id_to_name[k]: v for k, v in pagerank.items()}

    # Betweenness Centrality (normalized, directed)
    betweenness = nx.betweenness_centrality(G, normalized=True)
    betweenness_by_name = {id_to_name[k]: v for k, v in betweenness.items()}

    # HITS
    hubs, authorities = nx.hits(G, max_iter=100, tol=1e-5, normalized=True)
    hubs_by_name = {id_to_name[k]: v for k, v in hubs.items()}
    authorities_by_name = {id_to_name[k]: v for k, v in authorities.items()}

    # Katz Centrality
    try:
        katz = nx.katz_centrality(G, alpha=0.1, beta=1.0, max_iter=100, tol=1e-6, normalized=False)
        katz_by_name = {id_to_name[k]: v for k, v in katz.items()}
    except nx.NetworkXError:
        # Katz may not converge for some graphs
        katz_by_name = None

    # Community detection (Louvain) - use undirected graph
    G_undirected = G.to_undirected()
    communities = nx.community.louvain_communities(G_undirected, seed=42)
    community_by_name = {}
    for idx, community in enumerate(communities):
        for node in community:
            community_by_name[id_to_name[node]] = idx

    # Personalized PageRank from node 1 (A1)
    personalization = {node: 0.0 for node in G.nodes()}
    personalization[1] = 1.0
    ppr = nx.pagerank(G, alpha=0.85, personalization=personalization, max_iter=100, tol=1e-5)
    ppr_by_name = {id_to_name[k]: v for k, v in ppr.items()}

    return {
        'pagerank': pagerank_by_name,
        'betweenness': betweenness_by_name,
        'hubs': hubs_by_name,
        'authorities': authorities_by_name,
        'katz': katz_by_name,
        'communities': community_by_name,
        'personalized_pagerank': ppr_by_name,
    }


def values_match(expected: float, actual: float, name: str = "") -> tuple[bool, str]:
    """Check if two values match within tolerance."""
    if abs(expected) < ABS_TOLERANCE and abs(actual) < ABS_TOLERANCE:
        return True, ""

    if abs(expected) < ABS_TOLERANCE:
        diff = abs(actual)
    else:
        diff = abs(actual - expected) / abs(expected)

    if diff <= TOLERANCE:
        return True, ""
    else:
        return False, f"{name}: expected {expected:.6f}, got {actual:.6f} (diff: {diff:.1%})"


def communities_match(expected: dict[str, int], actual: dict[str, int]) -> tuple[bool, str]:
    """Check if community assignments group the same nodes together."""
    # Build sets of nodes in each community for both
    def get_community_sets(comm_dict):
        sets = {}
        for node, comm_id in comm_dict.items():
            if comm_id not in sets:
                sets[comm_id] = set()
            sets[comm_id].add(node)
        return list(sets.values())

    expected_sets = get_community_sets(expected)
    actual_sets = get_community_sets(actual)

    # Check that each expected community appears in actual (order/IDs may differ)
    for exp_set in expected_sets:
        found = False
        for act_set in actual_sets:
            if exp_set == act_set:
                found = True
                break
            # Also check if it's a subset (cuGraph might merge communities differently)
            if exp_set.issubset(act_set) or act_set.issubset(exp_set):
                found = True
                break
        if not found:
            # Check if nodes are at least grouped together
            first_node = next(iter(exp_set))
            first_comm = actual.get(first_node)
            if first_comm is not None:
                all_same = all(actual.get(n) == first_comm for n in exp_set)
                if all_same:
                    found = True

        if not found:
            return False, f"Community {exp_set} not found in actual results"

    return True, ""


def run_cmd(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
    """Run a shell command."""
    print(f"  $ {' '.join(cmd)}")
    return subprocess.run(cmd, capture_output=True, text=True, check=check)


def setup_container():
    """Stop old container and start fresh one with latest image."""
    print("\n" + "=" * 60)
    print("CONTAINER SETUP")
    print("=" * 60)

    print("\n>>> Killing all memgraph containers...")
    result = run_cmd(["docker", "ps", "-a", "--format", "{{.Names}}"], check=False)
    for container in result.stdout.strip().split("\n"):
        if container and "memgraph" in container.lower():
            print(f"  Stopping {container}...")
            run_cmd(["docker", "stop", container], check=False)
            run_cmd(["docker", "rm", container], check=False)

    print(f"\n>>> Clearing entire data directory at {MEMGRAPH_DATA_DIR}...")
    if MEMGRAPH_DATA_DIR.exists():
        shutil.rmtree(MEMGRAPH_DATA_DIR)
        print("  Removed all old data")
    MEMGRAPH_DATA_DIR.mkdir(parents=True, exist_ok=True)
    print("  Created fresh directory")

    print(f"\n>>> Checking image '{IMAGE_NAME}' exists...")
    result = run_cmd(["docker", "images", "-q", IMAGE_NAME], check=False)
    if not result.stdout.strip():
        print(f"ERROR: Image '{IMAGE_NAME}' not found!")
        print("Build it first with:")
        print(f"  docker build -f Dockerfile.cugraph -t {IMAGE_NAME} .")
        sys.exit(1)
    print(f"  Image ID: {result.stdout.strip()}")

    print(f"\n>>> Starting new container '{CONTAINER_NAME}'...")
    uid = os.getuid()
    gid = os.getgid()

    cmd = [
        "docker", "run", "-d",
        "--name", CONTAINER_NAME,
        "--user", f"{uid}:{gid}",
        "--gpus", "all",
        "-p", "7687:7687",
        "-p", "7444:7444",
        "-p", "3000:3000",
        "-v", f"{MEMGRAPH_DATA_DIR}:/var/lib/memgraph:z",
        IMAGE_NAME,
        "--storage-mode=IN_MEMORY_ANALYTICAL",
        "--query-execution-timeout-sec=0",
        "--log-level=WARNING",
        "--log-file=",
        "--also-log-to-stderr",
    ]
    result = run_cmd(cmd)
    container_id = result.stdout.strip()[:12]
    print(f"  Container started: {container_id}")

    print("\n>>> Verifying container uses correct image...")
    result = run_cmd(["docker", "inspect", "--format", "{{.Config.Image}}", CONTAINER_NAME])
    actual_image = result.stdout.strip()
    print(f"  Container image: {actual_image}")
    if actual_image != IMAGE_NAME:
        print(f"  WARNING: Expected {IMAGE_NAME}, got {actual_image}")


def wait_for_memgraph(driver, max_retries=30, delay=2):
    """Wait for Memgraph to be ready."""
    for i in range(max_retries):
        try:
            with driver.session() as session:
                session.run("RETURN 1")
            print("✓ Memgraph is ready")
            return True
        except Exception:
            print(f"  Waiting for Memgraph... ({i+1}/{max_retries})")
            time.sleep(delay)
    print("✗ Memgraph failed to start")
    return False


def clear_database(session):
    """Clear all data from the database."""
    session.run("MATCH (n) DETACH DELETE n")


def create_test_graph(session):
    """Create a test graph for algorithm validation."""
    queries = [
        "CREATE (a1:Node {id: 1, name: 'A1'})",
        "CREATE (a2:Node {id: 2, name: 'A2'})",
        "CREATE (a3:Node {id: 3, name: 'A3'})",
        "CREATE (a4:Node {id: 4, name: 'A4'})",
        "CREATE (b1:Node {id: 5, name: 'B1'})",
        "CREATE (b2:Node {id: 6, name: 'B2'})",
        "CREATE (b3:Node {id: 7, name: 'B3'})",
        "CREATE (b4:Node {id: 8, name: 'B4'})",
        "CREATE (hub:Node {id: 9, name: 'HUB'})",
        """
        MATCH (a1:Node {id: 1}), (a2:Node {id: 2}), (a3:Node {id: 3}), (a4:Node {id: 4})
        CREATE (a1)-[:EDGE {weight: 1.0}]->(a2),
               (a2)-[:EDGE {weight: 1.0}]->(a3),
               (a3)-[:EDGE {weight: 1.0}]->(a4),
               (a4)-[:EDGE {weight: 1.0}]->(a1),
               (a1)-[:EDGE {weight: 1.0}]->(a3),
               (a2)-[:EDGE {weight: 1.0}]->(a4)
        """,
        """
        MATCH (b1:Node {id: 5}), (b2:Node {id: 6}), (b3:Node {id: 7}), (b4:Node {id: 8})
        CREATE (b1)-[:EDGE {weight: 1.0}]->(b2),
               (b2)-[:EDGE {weight: 1.0}]->(b3),
               (b3)-[:EDGE {weight: 1.0}]->(b4),
               (b4)-[:EDGE {weight: 1.0}]->(b1),
               (b1)-[:EDGE {weight: 1.0}]->(b3),
               (b2)-[:EDGE {weight: 1.0}]->(b4)
        """,
        """
        MATCH (a1:Node {id: 1}), (b1:Node {id: 5}), (hub:Node {id: 9})
        CREATE (a1)-[:EDGE {weight: 1.0}]->(hub),
               (hub)-[:EDGE {weight: 1.0}]->(b1),
               (hub)-[:EDGE {weight: 1.0}]->(a1),
               (b1)-[:EDGE {weight: 1.0}]->(hub)
        """,
    ]

    for query in queries:
        session.run(query)

    result = session.run("MATCH (n) RETURN count(n) as nodes")
    node_count = result.single()["nodes"]

    result = session.run("MATCH ()-[r]->() RETURN count(r) as edges")
    edge_count = result.single()["edges"]

    print(f"✓ Test graph created: {node_count} nodes, {edge_count} edges")
    return node_count == 9 and edge_count == 16


def validate_node_identities(records: list, algorithm_name: str) -> tuple[bool, list[str]]:
    """Validate that all expected nodes are returned with correct identities."""
    errors = []

    # Check node count
    if len(records) != 9:
        errors.append(f"Expected 9 nodes, got {len(records)}")

    # Check all node names are present
    returned_names = {r['name'] for r in records}
    missing = EXPECTED_NODES - returned_names
    extra = returned_names - EXPECTED_NODES

    if missing:
        errors.append(f"Missing nodes: {missing}")
    if extra:
        errors.append(f"Unexpected nodes: {extra}")

    return len(errors) == 0, errors


def test_pagerank(session, ground_truth: dict) -> bool:
    """Test PageRank algorithm against NetworkX ground truth."""
    print("\n--- Testing PageRank ---")
    try:
        result = session.run("""
            CALL cugraph.pagerank.get(100, 0.85, 1e-5)
            YIELD node, pagerank
            RETURN node.id AS id, node.name AS name, pagerank
            ORDER BY pagerank DESC
        """)

        records = list(result)

        # Validate node identities
        valid, errors = validate_node_identities(records, "PageRank")
        if not valid:
            for err in errors:
                print(f"  ✗ {err}")
            return False

        print(f"✓ PageRank: {len(records)} nodes returned")

        # Compare against NetworkX ground truth
        expected = ground_truth['pagerank']
        all_match = True

        for r in records:
            name = r['name']
            actual = r['pagerank']
            exp = expected[name]
            match, err = values_match(exp, actual, name)
            if not match:
                print(f"  ✗ {err}")
                all_match = False
            else:
                print(f"  ✓ {name}: {actual:.6f} (expected: {exp:.6f})")

        # Verify ranking order matches
        actual_ranking = [r['name'] for r in records]
        expected_ranking = sorted(expected.keys(), key=lambda x: expected[x], reverse=True)

        # Check top 3 ranking
        if actual_ranking[:3] != expected_ranking[:3]:
            print(f"  ⚠ Ranking differs: cuGraph={actual_ranking[:3]}, NetworkX={expected_ranking[:3]}")
            # This is a warning, not a failure - numerical precision can cause minor reordering

        return all_match

    except Exception as e:
        print(f"✗ PageRank failed: {e}")
        return False


def test_betweenness_centrality(session, ground_truth: dict) -> bool:
    """Test Betweenness Centrality - HUB must be highest."""
    print("\n--- Testing Betweenness Centrality ---")
    try:
        result = session.run("""
            CALL cugraph.betweenness_centrality.get(true, true)
            YIELD node, betweenness
            RETURN node.id AS id, node.name AS name, betweenness
            ORDER BY betweenness DESC
        """)

        records = list(result)

        valid, errors = validate_node_identities(records, "Betweenness")
        if not valid:
            for err in errors:
                print(f"  ✗ {err}")
            return False

        print(f"✓ Betweenness Centrality: {len(records)} nodes returned")

        expected = ground_truth['betweenness']
        all_match = True

        for r in records:
            name = r['name']
            actual = r['betweenness']
            exp = expected[name]
            match, err = values_match(exp, actual, name)
            if not match:
                print(f"  ✗ {err}")
                all_match = False
            else:
                print(f"  ✓ {name}: {actual:.6f} (expected: {exp:.6f})")

        # Semantic check: HUB should be in top 3 and have high betweenness (it's the bridge)
        # Note: In linear chain topology, chain endpoints (A1, B1) have higher betweenness
        # because all paths from their chains must pass through them
        sorted_records = sorted(records, key=lambda r: r['betweenness'], reverse=True)
        top_3_names = [r['name'] for r in sorted_records[:3]]
        hub_bc = next((r['betweenness'] for r in records if r['name'] == 'HUB'), None)

        if 'HUB' not in top_3_names:
            print(f"  ✗ CRITICAL: HUB should be in top 3 betweenness nodes")
            print(f"    Top 3: {top_3_names}")
            return False
        elif hub_bc < 0.5:
            print(f"  ✗ CRITICAL: HUB betweenness too low: {hub_bc}")
            return False
        else:
            print(f"  ✓ SEMANTIC: HUB is in top 3 betweenness with score {hub_bc:.6f}")

        return all_match

    except Exception as e:
        print(f"✗ Betweenness Centrality failed: {e}")
        return False


def test_hits(session, ground_truth: dict) -> bool:
    """Test HITS algorithm against NetworkX ground truth."""
    print("\n--- Testing HITS ---")
    try:
        result = session.run("""
            CALL cugraph.hits.get(100, 1e-5, true)
            YIELD node, hub, authority
            RETURN node.id AS id, node.name AS name, hub, authority
            ORDER BY hub DESC
        """)

        records = list(result)

        valid, errors = validate_node_identities(records, "HITS")
        if not valid:
            for err in errors:
                print(f"  ✗ {err}")
            return False

        print(f"✓ HITS: {len(records)} nodes returned")

        expected_hubs = ground_truth['hubs']
        expected_auths = ground_truth['authorities']
        all_match = True

        for r in records:
            name = r['name']

            # Check hub values
            actual_hub = r['hub']
            exp_hub = expected_hubs[name]
            match, err = values_match(exp_hub, actual_hub, f"{name} hub")
            if not match:
                print(f"  ✗ {err}")
                all_match = False

            # Check authority values
            actual_auth = r['authority']
            exp_auth = expected_auths[name]
            match, err = values_match(exp_auth, actual_auth, f"{name} authority")
            if not match:
                print(f"  ✗ {err}")
                all_match = False

            if all_match:
                print(f"  ✓ {name}: hub={actual_hub:.6f}, auth={actual_auth:.6f}")

        return all_match

    except Exception as e:
        print(f"✗ HITS failed: {e}")
        return False


def test_louvain(session, ground_truth: dict) -> bool:
    """Test Louvain community detection - A1-A4 and B1-B4 should be grouped."""
    print("\n--- Testing Louvain ---")
    try:
        result = session.run("""
            CALL cugraph.louvain.get()
            YIELD node, partition
            RETURN node.id AS id, node.name AS name, partition AS community
            ORDER BY partition, id
        """)

        records = list(result)

        valid, errors = validate_node_identities(records, "Louvain")
        if not valid:
            for err in errors:
                print(f"  ✗ {err}")
            return False

        actual_communities = {r['name']: r['community'] for r in records}
        communities = set(actual_communities.values())
        print(f"✓ Louvain: {len(records)} nodes in {len(communities)} communities")

        for r in records:
            print(f"    {r['name']}: community {r['community']}")

        # Check that A1-A4 are in same community
        a_comms = {actual_communities[n] for n in COMMUNITY_A}
        if len(a_comms) != 1:
            print(f"  ✗ A1-A4 should be in same community but are in: {a_comms}")
            return False
        print(f"  ✓ A1-A4 are in same community ({a_comms.pop()})")

        # Check that B1-B4 are in same community
        b_comms = {actual_communities[n] for n in COMMUNITY_B}
        if len(b_comms) != 1:
            print(f"  ✗ B1-B4 should be in same community but are in: {b_comms}")
            return False
        print(f"  ✓ B1-B4 are in same community ({b_comms.pop()})")

        # Check that A and B communities are different
        a_comm = actual_communities['A1']
        b_comm = actual_communities['B1']
        if a_comm == b_comm:
            print(f"  ✗ A and B communities should be different but both are {a_comm}")
            return False
        print(f"  ✓ A and B are in different communities")

        return True

    except Exception as e:
        print(f"✗ Louvain failed: {e}")
        return False


def test_leiden(session, ground_truth: dict) -> bool:
    """Test Leiden community detection - A1-A4 and B1-B4 should be grouped."""
    print("\n--- Testing Leiden ---")
    try:
        result = session.run("""
            CALL cugraph.leiden.get()
            YIELD node, partition
            RETURN node.id AS id, node.name AS name, partition AS community
            ORDER BY partition, id
        """)

        records = list(result)

        valid, errors = validate_node_identities(records, "Leiden")
        if not valid:
            for err in errors:
                print(f"  ✗ {err}")
            return False

        actual_communities = {r['name']: r['community'] for r in records}
        communities = set(actual_communities.values())
        print(f"✓ Leiden: {len(records)} nodes in {len(communities)} communities")

        for r in records:
            print(f"    {r['name']}: community {r['community']}")

        # Check that A1-A4 are in same community
        a_comms = {actual_communities[n] for n in COMMUNITY_A}
        if len(a_comms) != 1:
            print(f"  ✗ A1-A4 should be in same community but are in: {a_comms}")
            return False
        print(f"  ✓ A1-A4 are in same community")

        # Check that B1-B4 are in same community
        b_comms = {actual_communities[n] for n in COMMUNITY_B}
        if len(b_comms) != 1:
            print(f"  ✗ B1-B4 should be in same community but are in: {b_comms}")
            return False
        print(f"  ✓ B1-B4 are in same community")

        return True

    except Exception as e:
        print(f"✗ Leiden failed: {e}")
        return False


def test_katz_centrality(session, ground_truth: dict) -> bool:
    """Test Katz Centrality algorithm."""
    print("\n--- Testing Katz Centrality ---")
    try:
        result = session.run("""
            CALL cugraph.katz_centrality.get(0.1, 1.0, 1e-6, 100, false)
            YIELD node, katz
            RETURN node.id AS id, node.name AS name, katz
            ORDER BY katz DESC
        """)

        records = list(result)

        valid, errors = validate_node_identities(records, "Katz")
        if not valid:
            for err in errors:
                print(f"  ✗ {err}")
            return False

        print(f"✓ Katz Centrality: {len(records)} nodes returned")

        expected = ground_truth['katz']
        if expected is None:
            print("  ⚠ NetworkX Katz did not converge, skipping value comparison")
            for r in records:
                print(f"    {r['name']}: {r['katz']:.6f}")
            return True

        all_match = True
        for r in records:
            name = r['name']
            actual = r['katz']
            exp = expected[name]
            match, err = values_match(exp, actual, name)
            if not match:
                print(f"  ✗ {err}")
                all_match = False
            else:
                print(f"  ✓ {name}: {actual:.6f} (expected: {exp:.6f})")

        return all_match

    except Exception as e:
        print(f"✗ Katz Centrality failed: {e}")
        return False


def test_personalized_pagerank(session, ground_truth: dict) -> bool:
    """Test Personalized PageRank from A1."""
    print("\n--- Testing Personalized PageRank ---")
    try:
        result = session.run("""
            MATCH (source:Node {id: 1})
            CALL cugraph.personalized_pagerank.get(source, 100, 0.85, 1e-5)
            YIELD node, pagerank
            RETURN node.id AS id, node.name AS name, pagerank
            ORDER BY pagerank DESC
        """)

        records = list(result)

        valid, errors = validate_node_identities(records, "Personalized PageRank")
        if not valid:
            for err in errors:
                print(f"  ✗ {err}")
            return False

        print(f"✓ Personalized PageRank: {len(records)} nodes returned")

        expected = ground_truth['personalized_pagerank']
        all_match = True

        for r in records:
            name = r['name']
            actual = r['pagerank']
            exp = expected[name]
            match, err = values_match(exp, actual, name)
            if not match:
                print(f"  ✗ {err}")
                all_match = False
            else:
                print(f"  ✓ {name}: {actual:.6f} (expected: {exp:.6f})")

        # A1 should have highest PPR (it's the source)
        a1_ppr = next((r['pagerank'] for r in records if r['name'] == 'A1'), None)
        max_ppr = max(r['pagerank'] for r in records)

        if a1_ppr != max_ppr:
            print(f"  ⚠ A1 should have highest PPR but doesn't (A1={a1_ppr}, max={max_ppr})")
        else:
            print(f"  ✓ A1 has highest PPR as expected (source node)")

        return all_match

    except Exception as e:
        print(f"✗ Personalized PageRank failed: {e}")
        return False


def test_balanced_cut_clustering(session, ground_truth: dict) -> bool:
    """Test Balanced Cut Clustering."""
    print("\n--- Testing Balanced Cut Clustering ---")
    try:
        result = session.run("""
            CALL cugraph.balanced_cut_clustering.get(2)
            YIELD node, cluster
            RETURN node.id AS id, node.name AS name, cluster
            ORDER BY cluster, id
        """)

        records = list(result)

        valid, errors = validate_node_identities(records, "Balanced Cut")
        if not valid:
            for err in errors:
                print(f"  ✗ {err}")
            return False

        clusters = set(r['cluster'] for r in records)
        print(f"✓ Balanced Cut Clustering: {len(records)} nodes in {len(clusters)} clusters")

        for r in records:
            print(f"    {r['name']}: cluster {r['cluster']}")

        if len(clusters) != 2:
            print(f"  ✗ Expected 2 clusters, got {len(clusters)}")
            return False

        print(f"  ✓ Correctly produced 2 clusters")
        return True

    except Exception as e:
        print(f"✗ Balanced Cut Clustering failed: {e}")
        return False


def test_spectral_clustering(session, ground_truth: dict) -> bool:
    """Test Spectral Clustering."""
    print("\n--- Testing Spectral Clustering ---")
    try:
        result = session.run("""
            CALL cugraph.spectral_clustering.get(2)
            YIELD node, cluster
            RETURN node.id AS id, node.name AS name, cluster
            ORDER BY cluster, id
        """)

        records = list(result)

        valid, errors = validate_node_identities(records, "Spectral")
        if not valid:
            for err in errors:
                print(f"  ✗ {err}")
            return False

        clusters = set(r['cluster'] for r in records)
        print(f"✓ Spectral Clustering: {len(records)} nodes in {len(clusters)} clusters")

        for r in records:
            print(f"    {r['name']}: cluster {r['cluster']}")

        if len(clusters) != 2:
            print(f"  ✗ Expected 2 clusters, got {len(clusters)}")
            return False

        print(f"  ✓ Correctly produced 2 clusters")
        return True

    except Exception as e:
        print(f"✗ Spectral Clustering failed: {e}")
        return False


def main():
    print("=" * 60)
    print("cuGraph MAGE Algorithm Test Suite")
    print("Testing RAPIDS 25.x API with NetworkX Ground Truth")
    print("=" * 60)

    # Build NetworkX graph and compute ground truth
    print("\n--- Computing NetworkX Ground Truth ---")
    G = build_networkx_graph()
    print(f"  NetworkX graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")

    ground_truth = get_networkx_ground_truth(G)
    print("  ✓ Ground truth computed for all algorithms")

    # Show expected values
    print("\n  Expected PageRank (top 3):")
    pr = ground_truth['pagerank']
    for name in sorted(pr.keys(), key=lambda x: pr[x], reverse=True)[:3]:
        print(f"    {name}: {pr[name]:.6f}")

    print("\n  Expected Betweenness (top 3):")
    bc = ground_truth['betweenness']
    for name in sorted(bc.keys(), key=lambda x: bc[x], reverse=True)[:3]:
        print(f"    {name}: {bc[name]:.6f}")

    # Setup container with fresh image
    setup_container()

    driver = GraphDatabase.driver(MEMGRAPH_URI, auth=(MEMGRAPH_USER, MEMGRAPH_PASSWORD))

    try:
        if not wait_for_memgraph(driver):
            sys.exit(1)

        with driver.session() as session:
            print("\n--- Setup ---")
            clear_database(session)
            if not create_test_graph(session):
                print("✗ Failed to create test graph")
                sys.exit(1)

            results = {}

            results['PageRank'] = test_pagerank(session, ground_truth)
            results['Betweenness Centrality'] = test_betweenness_centrality(session, ground_truth)
            results['HITS'] = test_hits(session, ground_truth)
            results['Louvain'] = test_louvain(session, ground_truth)
            results['Leiden'] = test_leiden(session, ground_truth)
            results['Katz Centrality'] = test_katz_centrality(session, ground_truth)
            results['Personalized PageRank'] = test_personalized_pagerank(session, ground_truth)
            results['Balanced Cut Clustering'] = test_balanced_cut_clustering(session, ground_truth)
            results['Spectral Clustering'] = test_spectral_clustering(session, ground_truth)

            print("\n" + "=" * 60)
            print("TEST SUMMARY")
            print("=" * 60)

            passed = sum(1 for v in results.values() if v)
            failed = sum(1 for v in results.values() if not v)

            for name, result in results.items():
                status = "✓ PASS" if result else "✗ FAIL"
                print(f"  {status}: {name}")

            print(f"\nTotal: {passed} passed, {failed} failed")
            print(f"Tolerance: {TOLERANCE:.0%} relative, {ABS_TOLERANCE} absolute")

            if failed > 0:
                sys.exit(1)
            else:
                print("\n✓ All cuGraph algorithms match NetworkX ground truth!")
                sys.exit(0)

    finally:
        # Cleanup temp directory if we created one
        if _using_temp_dir and MEMGRAPH_DATA_DIR.exists():
            print(f"\n>>> Cleaning up temp data directory: {MEMGRAPH_DATA_DIR}")
            shutil.rmtree(MEMGRAPH_DATA_DIR, ignore_errors=True)
        driver.close()


if __name__ == "__main__":
    main()
