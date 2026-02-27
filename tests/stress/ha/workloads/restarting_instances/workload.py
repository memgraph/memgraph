#!/usr/bin/env python3
"""
Restarting instances workload.

1) Imports the publications dataset from S3.
2) Repeats 100 times:
   - Create 1000 random edges between existing nodes with 4 workers.
   - Restart all instances (coordinators + data).
   - Wait for cluster to become healthy again.
"""
import os
import random
import sys
import time

from cluster_monitor import ClusterMonitor
from ha_common import Protocol, QueryType, execute_and_fetch, execute_query, get_restart_all_fn, run_parallel

COORDINATOR = "coord_1"
COORDINATORS = ["coord_1", "coord_2", "coord_3"]

S3_BUCKET = "memgraph-stress-tests-bucket"
S3_DATASET_PATH = "publications-dataset-1-percent"
S3_REGION = "eu-west-1"

EDGES_PER_ITERATION = 1000
NUM_WORKERS = 4
NUM_ITERATIONS = 100

HEALTH_CHECK_RETRIES = 30
HEALTH_CHECK_INTERVAL = 10


def get_s3_secret() -> str:
    access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

    if not access_key or not secret_key:
        raise ValueError(
            "AWS credentials not found. Please set AWS_ACCESS_KEY_ID and "
            "AWS_SECRET_ACCESS_KEY environment variables."
        )

    return f"CREATE SECRET (TYPE s3, KEY_ID '{access_key}', SECRET '{secret_key}', REGION '{S3_REGION}');"


def create_publications_indices() -> None:
    print("Creating publications indices and constraints...")
    queries = [
        "CREATE INDEX ON :Concept;",
        "CREATE INDEX ON :Concept(concept_id);",
        "CREATE CONSTRAINT ON (n:Concept) ASSERT n.concept_id IS UNIQUE;",
        "CREATE INDEX ON :Journal;",
        "CREATE INDEX ON :Journal(journal_name);",
        "CREATE CONSTRAINT ON (n:Journal) ASSERT n.journal_name IS UNIQUE;",
        "CREATE INDEX ON :Date;",
        "CREATE INDEX ON :Date(publish_date);",
        "CREATE CONSTRAINT ON (n:Date) ASSERT n.publish_date IS UNIQUE;",
        "CREATE INDEX ON :Publication;",
        "CREATE INDEX ON :Publication(doi);",
        "CREATE CONSTRAINT ON (n:Publication) ASSERT n.doi IS UNIQUE;",
        "CREATE INDEX ON :Section;",
        "CREATE INDEX ON :Section(section_id);",
        "CREATE CONSTRAINT ON (n:Section) ASSERT n.section_id IS UNIQUE;",
        "CREATE INDEX ON :Author;",
        "CREATE INDEX ON :Author(author_id);",
        "CREATE CONSTRAINT ON (n:Author) ASSERT n.author_id IS UNIQUE;",
    ]
    for query in queries:
        print(f"  {query}")
        execute_query(COORDINATOR, query, protocol=Protocol.BOLT_ROUTING, query_type=QueryType.WRITE)
    print("Publications indices created.")


def import_publications(s3_secret: str) -> None:
    s3_base = f"s3://{S3_BUCKET}/{S3_DATASET_PATH}"

    imports = [
        (
            "Concept nodes",
            f"""
USING PERIODIC COMMIT 100000
CALL migrate.duckdb("SELECT * FROM read_parquet('{s3_base}/concepts.parquet')", ["{s3_secret}"])
YIELD row MERGE (n:Concept {{concept_id: row.concept_id}}) SET n += row;""",
        ),
        (
            "Journal nodes",
            f"""
USING PERIODIC COMMIT 100000
CALL migrate.duckdb("SELECT * FROM read_parquet('{s3_base}/journals.parquet')", ["{s3_secret}"])
YIELD row MERGE (n:Journal {{journal_name: row.journal_name}}) SET n += row;""",
        ),
        (
            "Publication nodes",
            f"""
USING PERIODIC COMMIT 100000
CALL migrate.duckdb("SELECT * FROM read_parquet('{s3_base}/publications.parquet')", ["{s3_secret}"])
YIELD row MERGE (n:Publication {{doi: row.doi}});""",
        ),
        (
            "Date nodes",
            f"""
USING PERIODIC COMMIT 100000
CALL migrate.duckdb("SELECT * FROM read_parquet('{s3_base}/publish_dates.parquet')", ["{s3_secret}"])
YIELD row MERGE (n:Date {{publish_date: row.publish_date}}) SET n += row;""",
        ),
        (
            "Section nodes",
            f"""
USING PERIODIC COMMIT 100000
CALL migrate.duckdb("SELECT * FROM read_parquet('{s3_base}/sections.parquet')", ["{s3_secret}"])
YIELD row MERGE (n:Section {{section_id: row.section_id}}) SET n += row;""",
        ),
        (
            "Author nodes",
            f"""
USING PERIODIC COMMIT 100000
CALL migrate.duckdb("SELECT * FROM read_parquet('{s3_base}/authors.parquet')", ["{s3_secret}"])
YIELD row MERGE (n:Author {{author_id: row.author_id}}) SET n += row;""",
        ),
        (
            "AUTHORED_BY relationships",
            f"""
USING PERIODIC COMMIT 100000
CALL migrate.duckdb("SELECT * FROM read_parquet('{s3_base}/rel_authored_by.parquet')", ["{s3_secret}"])
YIELD row MERGE (a:Author {{author_id: row.author_id}}) MERGE (p:Publication {{doi: row.publication_doi}}) MERGE (p)-[:AUTHORED_BY]->(a);""",
        ),
        (
            "CITES relationships",
            f"""
USING PERIODIC COMMIT 100000
CALL migrate.duckdb("SELECT * FROM read_parquet('{s3_base}/rel_cites_paper.parquet')", ["{s3_secret}"])
YIELD row MERGE (citing:Publication {{doi: row.citing_doi}}) MERGE (cited:Publication {{doi: row.cited_doi}}) MERGE (citing)-[:CITES]->(cited);""",
        ),
        (
            "PART_OF relationships",
            f"""
USING PERIODIC COMMIT 100000
CALL migrate.duckdb("SELECT * FROM read_parquet('{s3_base}/rel_part_of.parquet')", ["{s3_secret}"])
YIELD row MERGE (section:Section {{section_id: row.section_id}}) MERGE (publication:Publication {{doi: row.publication_doi}}) MERGE (section)-[:PART_OF]->(publication);""",
        ),
        (
            "PUBLISHED_IN relationships",
            f"""
USING PERIODIC COMMIT 100000
CALL migrate.duckdb("SELECT * FROM read_parquet('{s3_base}/rel_published_in.parquet')", ["{s3_secret}"])
YIELD row MERGE (journal:Journal {{journal_name: row.journal_name}}) MERGE (publication:Publication {{doi: row.publication_doi}}) MERGE (publication)-[:PUBLISHED_IN]->(journal);""",
        ),
        (
            "PUBLISHED_ON relationships",
            f"""
USING PERIODIC COMMIT 100000
CALL migrate.duckdb("SELECT * FROM read_parquet('{s3_base}/rel_published_on.parquet')", ["{s3_secret}"])
YIELD row MERGE (publication:Publication {{doi: row.publication_doi}}) MERGE (d:Date {{publish_date: row.publish_date}}) MERGE (publication)-[:PUBLISHED_ON]->(d);""",
        ),
        (
            "REFERENCES relationships",
            f"""
USING PERIODIC COMMIT 100000
CALL migrate.duckdb("SELECT * FROM read_parquet('{s3_base}/rel_references.parquet')", ["{s3_secret}"])
YIELD row MERGE (concept:Concept {{concept_id: row.concept_id}}) MERGE (section:Section {{section_id: row.section_id}}) CREATE (section)-[:row.type {{score: row.confidence_score}}]->(concept);""",
        ),
        (
            "SUBFIELD_OF relationships",
            f"""
USING PERIODIC COMMIT 100000
CALL migrate.duckdb("SELECT * FROM read_parquet('{s3_base}/rel_subfield_of.parquet')", ["{s3_secret}"])
YIELD row MERGE (parent:Concept {{concept_id: row.parent_concept_id}}) MERGE (child:Concept {{concept_id: row.child_concept_id}}) MERGE (child)-[:SUBFIELD_OF]->(parent);""",
        ),
        (
            "SYNONYM_OF relationships",
            f"""
USING PERIODIC COMMIT 100000
CALL migrate.duckdb("SELECT * FROM read_parquet('{s3_base}/rel_synonym_of.parquet')", ["{s3_secret}"])
YIELD row MERGE (concept:Concept {{concept_id: row.concept_id}}) MERGE (similar:Concept {{concept_id: row.synonym_id}}) MERGE (concept)-[:SYNONYM_OF]->(similar);""",
        ),
    ]

    for name, query in imports:
        print(f"\nImporting {name}...")
        t0 = time.time()
        execute_query(COORDINATOR, query, protocol=Protocol.BOLT_ROUTING, query_type=QueryType.WRITE)
        print(f"  Completed in {time.time() - t0:.1f}s")


def get_node_count() -> int:
    result = execute_and_fetch(COORDINATOR, "MATCH (n) RETURN count(n) AS cnt", protocol=Protocol.BOLT_ROUTING)
    return result[0]["cnt"] if result else 0


def create_random_edges(worker_id: int, edges_per_worker: int, max_node_id: int) -> dict:
    """Each worker creates random edges between existing nodes using internal IDs."""
    pairs = [
        {"src": random.randint(0, max_node_id), "dst": random.randint(0, max_node_id)} for _ in range(edges_per_worker)
    ]
    query = """
UNWIND $pairs AS p
MATCH (a) WHERE ID(a) = p.src
MATCH (b) WHERE ID(b) = p.dst
CREATE (a)-[:STRESS_EDGE]->(b)"""

    t0 = time.time()
    execute_query(
        COORDINATOR,
        query,
        params={"pairs": pairs},
        protocol=Protocol.BOLT_ROUTING,
        query_type=QueryType.WRITE,
        apply_retry_mechanism=True,
    )
    elapsed = time.time() - t0
    print(f"  [Worker {worker_id}] Created {edges_per_worker} edges in {elapsed:.1f}s")
    return {"worker_id": worker_id, "elapsed": elapsed}


def wait_for_healthy_cluster(monitor: ClusterMonitor) -> None:
    """Poll until all instances are up and replicas are ready."""
    for attempt in range(1, HEALTH_CHECK_RETRIES + 1):
        try:
            instances_ok = monitor.verify_instances_up()
            replicas_ok = monitor.verify_all_ready()
            if instances_ok and replicas_ok:
                return
        except Exception as e:
            print(f"  Health check attempt {attempt}/{HEALTH_CHECK_RETRIES} failed: {e}")
        print(f"  Waiting {HEALTH_CHECK_INTERVAL}s before next health check...")
        time.sleep(HEALTH_CHECK_INTERVAL)

    print("ERROR: Cluster did not become healthy after restart!")
    sys.exit(1)


def main():
    restart_all_fn = get_restart_all_fn()
    if restart_all_fn is None:
        print("ERROR: restart_all function not available for this deployment")
        sys.exit(1)

    print("=" * 60)
    print("Restarting Instances Workload")
    print("=" * 60)
    print(f"Phase 1: Import publications dataset from S3")
    print(f"Phase 2: {NUM_ITERATIONS} iterations of edge creation + restart")
    print(f"  Edges per iteration: {EDGES_PER_ITERATION}")
    print(f"  Workers: {NUM_WORKERS}")
    print("-" * 60)

    s3_secret = get_s3_secret()
    print("S3 credentials loaded from environment variables.")

    monitor = ClusterMonitor(
        coordinators=COORDINATORS,
        show_replicas=True,
        verify_up=False,
        storage_info=["vertex_count", "edge_count", "memory_res", "allocation_limit"],
        interval=10,
    )

    total_start = time.time()

    # Phase 1: Import publications
    print("\n" + "=" * 60)
    print("PHASE 1: Publications Import")
    print("=" * 60)

    with monitor:
        create_publications_indices()
        import_publications(s3_secret)

    print("\nPhase 1 complete.")

    # Phase 2: Edge creation + restart loop
    print("\n" + "=" * 60)
    print("PHASE 2: Edge Creation + Restart Loop")
    print("=" * 60)

    node_count = get_node_count()
    if node_count == 0:
        print("ERROR: No nodes in the database after publications import!")
        sys.exit(1)
    max_node_id = node_count - 1
    print(f"Node count: {node_count:,} (max internal ID ≈ {max_node_id})")

    edges_per_worker = EDGES_PER_ITERATION // NUM_WORKERS

    for iteration in range(NUM_ITERATIONS):
        print(f"\n--- Iteration {iteration + 1}/{NUM_ITERATIONS} ---")

        # Create 1000 random edges with 4 workers
        print(f"Creating {EDGES_PER_ITERATION} random edges...")
        tasks = [(worker_id, edges_per_worker, max_node_id) for worker_id in range(NUM_WORKERS)]
        t0 = time.time()
        run_parallel(create_random_edges, tasks, num_workers=NUM_WORKERS)
        print(f"  Edge creation complete in {time.time() - t0:.1f}s")

        # Restart all instances
        print("Restarting all instances...")
        t0 = time.time()
        restart_all_fn()
        print(f"  Restart triggered in {time.time() - t0:.1f}s")

        # Wait for healthy cluster
        print("Waiting for cluster to become healthy...")
        wait_for_healthy_cluster(monitor)
        print(f"  Cluster healthy after restart")

    total_elapsed = time.time() - total_start

    print("\n" + "=" * 60)
    print(f"Total time: {total_elapsed:.1f}s ({total_elapsed / 60:.1f} minutes)")

    result = execute_and_fetch(
        COORDINATOR,
        "MATCH ()-[r:STRESS_EDGE]->() RETURN count(r) as cnt",
        protocol=Protocol.BOLT_ROUTING,
    )
    edge_count = result[0]["cnt"] if result else 0
    print(f"STRESS_EDGE edges created: {edge_count:,} (expected: {EDGES_PER_ITERATION * NUM_ITERATIONS:,})")

    print("\nFinal verification:")
    monitor.show_replicas()

    ok = monitor.verify_all_ready() and monitor.verify_instances_up()
    if not ok:
        sys.exit(1)
    print("Workload completed successfully!")


if __name__ == "__main__":
    main()
