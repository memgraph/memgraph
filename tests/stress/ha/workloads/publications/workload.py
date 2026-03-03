#!/usr/bin/env python3
"""
Publications dataset import workload.
Imports scientific publications data from S3 parquet files using DuckDB migration.
Then runs a concurrent workload with 10 workers performing random graph mutations.

Uses bolt+routing protocol.
"""
import os
import random
import sys
import time

from cluster_monitor import ClusterMonitor
from ha_common import Protocol, QueryType, execute_and_fetch, execute_query, run_parallel

COORDINATOR = "coord_1"
COORDINATORS = ["coord_1", "coord_2", "coord_3"]

S3_BUCKET = "memgraph-stress-tests-bucket"
S3_DATASET_PATH = "publications-dataset-1-percent"
S3_REGION = "eu-west-1"

# Concurrent workload settings
NUM_WORKERS = 10
ITERATIONS = 50_000  # total iterations across all workers

LABELS = ["Concept", "Journal", "Publication", "Date", "Section", "Author"]
LABEL_UNIQUE_PROP = {
    "Concept": "concept_id",
    "Journal": "journal_name",
    "Publication": "doi",
    "Date": "publish_date",
    "Section": "section_id",
    "Author": "author_id",
}


def get_s3_secret() -> str:
    access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

    if not access_key or not secret_key:
        raise ValueError(
            "AWS credentials not found. Please set AWS_ACCESS_KEY_ID and "
            "AWS_SECRET_ACCESS_KEY environment variables."
        )

    return f"CREATE SECRET (TYPE s3, KEY_ID '{access_key}', SECRET '{secret_key}', REGION '{S3_REGION}');"


def create_indices_and_constraints() -> None:
    print("Creating indices and constraints...")

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

    print("Indices and constraints created.")


def import_data(s3_secret: str) -> None:
    s3_base = f"s3://{S3_BUCKET}/{S3_DATASET_PATH}"

    imports = [
        (
            "Concept nodes",
            f"""
USING PERIODIC COMMIT 100000
CALL migrate.duckdb(
  "SELECT * FROM read_parquet('{s3_base}/concepts.parquet')",
  ["{s3_secret}"])
YIELD row
MERGE (n:Concept {{concept_id: row.concept_id}})
SET n += row;""",
        ),
        (
            "Journal nodes",
            f"""
USING PERIODIC COMMIT 100000
CALL migrate.duckdb(
  "SELECT * FROM read_parquet('{s3_base}/journals.parquet')",
  ["{s3_secret}"])
YIELD row
MERGE (n:Journal {{journal_name: row.journal_name}})
SET n += row;""",
        ),
        (
            "Publication nodes",
            f"""
USING PERIODIC COMMIT 100000
CALL migrate.duckdb(
  "SELECT * FROM read_parquet('{s3_base}/publications.parquet')",
  ["{s3_secret}"])
YIELD row
MERGE (n:Publication {{doi: row.doi}});""",
        ),
        (
            "Date nodes",
            f"""
USING PERIODIC COMMIT 100000
CALL migrate.duckdb(
  "SELECT * FROM read_parquet('{s3_base}/publish_dates.parquet')",
  ["{s3_secret}"])
YIELD row
MERGE (n:Date {{publish_date: row.publish_date}})
SET n += row;""",
        ),
        (
            "Section nodes",
            f"""
USING PERIODIC COMMIT 100000
CALL migrate.duckdb(
  "SELECT * FROM read_parquet('{s3_base}/sections.parquet')",
  ["{s3_secret}"])
YIELD row
MERGE (n:Section {{section_id: row.section_id}})
SET n += row;""",
        ),
        (
            "Author nodes",
            f"""
USING PERIODIC COMMIT 100000
CALL migrate.duckdb(
  "SELECT * FROM read_parquet('{s3_base}/authors.parquet')",
  ["{s3_secret}"])
YIELD row
MERGE (n:Author {{author_id: row.author_id}})
SET n += row;""",
        ),
        (
            "AUTHORED_BY relationships",
            f"""
USING PERIODIC COMMIT 100000
CALL migrate.duckdb(
  "SELECT * FROM read_parquet('{s3_base}/rel_authored_by.parquet')",
  ["{s3_secret}"])
YIELD row
MERGE (a:Author {{author_id: row.author_id}})
MERGE (p:Publication {{doi: row.publication_doi}})
MERGE (p)-[:AUTHORED_BY]->(a);""",
        ),
        (
            "CITES relationships",
            f"""
USING PERIODIC COMMIT 100000
CALL migrate.duckdb(
  "SELECT * FROM read_parquet('{s3_base}/rel_cites_paper.parquet')",
  ["{s3_secret}"])
YIELD row
MERGE (citing:Publication {{doi: row.citing_doi}})
MERGE (cited:Publication {{doi: row.cited_doi}})
MERGE (citing)-[:CITES]->(cited);""",
        ),
        (
            "PART_OF relationships",
            f"""
USING PERIODIC COMMIT 100000
CALL migrate.duckdb(
  "SELECT * FROM read_parquet('{s3_base}/rel_part_of.parquet')",
  ["{s3_secret}"])
YIELD row
MERGE (section:Section {{section_id: row.section_id}})
MERGE (publication:Publication {{doi: row.publication_doi}})
MERGE (section)-[:PART_OF]->(publication);""",
        ),
        (
            "PUBLISHED_IN relationships",
            f"""
USING PERIODIC COMMIT 100000
CALL migrate.duckdb(
  "SELECT * FROM read_parquet('{s3_base}/rel_published_in.parquet')",
  ["{s3_secret}"])
YIELD row
MERGE (journal:Journal {{journal_name: row.journal_name}})
MERGE (publication:Publication {{doi: row.publication_doi}})
MERGE (publication)-[:PUBLISHED_IN]->(journal);""",
        ),
        (
            "PUBLISHED_ON relationships",
            f"""
USING PERIODIC COMMIT 100000
CALL migrate.duckdb(
  "SELECT * FROM read_parquet('{s3_base}/rel_published_on.parquet')",
  ["{s3_secret}"])
YIELD row
MERGE (publication:Publication {{doi: row.publication_doi}})
MERGE (d:Date {{publish_date: row.publish_date}})
MERGE (publication)-[:PUBLISHED_ON]->(d);""",
        ),
        (
            "REFERENCES relationships",
            f"""
USING PERIODIC COMMIT 100000
CALL migrate.duckdb(
  "SELECT * FROM read_parquet('{s3_base}/rel_references.parquet')",
  ["{s3_secret}"])
YIELD row
MERGE (concept:Concept {{concept_id: row.concept_id}})
MERGE (section:Section {{section_id: row.section_id}})
CREATE (section)-[:row.type {{score: row.confidence_score}}]->(concept);""",
        ),
        (
            "SUBFIELD_OF relationships",
            f"""
USING PERIODIC COMMIT 100000
CALL migrate.duckdb(
  "SELECT * FROM read_parquet('{s3_base}/rel_subfield_of.parquet')",
  ["{s3_secret}"])
YIELD row
MERGE (parent_concept:Concept {{concept_id: row.parent_concept_id}})
MERGE (child_concept:Concept {{concept_id: row.child_concept_id}})
MERGE (child_concept)-[:SUBFIELD_OF]->(parent_concept);""",
        ),
        (
            "SYNONYM_OF relationships",
            f"""
USING PERIODIC COMMIT 100000
CALL migrate.duckdb(
  "SELECT * FROM read_parquet('{s3_base}/rel_synonym_of.parquet')",
  ["{s3_secret}"])
YIELD row
MERGE (concept:Concept {{concept_id: row.concept_id}})
MERGE (similar_concept:Concept {{concept_id: row.synonym_id}})
MERGE (concept)-[:SYNONYM_OF]->(similar_concept);""",
        ),
    ]

    for name, query in imports:
        print(f"\nImporting {name}...")
        start_time = time.time()
        execute_query(COORDINATOR, query, protocol=Protocol.BOLT_ROUTING, query_type=QueryType.WRITE)
        elapsed = time.time() - start_time
        print(f"  Completed in {elapsed:.1f}s")


def get_node_counts() -> dict[str, int]:
    labels = ["Concept", "Journal", "Publication", "Date", "Section", "Author"]
    counts = {}

    for label in labels:
        result = execute_and_fetch(
            COORDINATOR, f"MATCH (n:{label}) RETURN count(n) as cnt", protocol=Protocol.BOLT_ROUTING
        )
        counts[label] = result[0]["cnt"] if result else 0

    return counts


# ---------------------------------------------------------------------------
# Concurrent workload operations
# ---------------------------------------------------------------------------


def _random_unique_value(label: str) -> int | str:
    """Generate a random unique property value for a stress-test node."""
    rand = random.randint(10_000_000, 999_999_999)
    if label == "Journal":
        return f"stress_journal_{rand}"
    if label == "Publication":
        return f"10.stress/{rand}"
    if label == "Date":
        return f"stress_date_{rand}"
    # Concept.concept_id, Author.author_id, Section.section_id — use large ints
    return rand


def op_create_node_no_edges(label: str) -> None:
    """Pick a random existing node, create a new node with the same labels and
    properties but a freshly generated unique property value (no edges)."""
    prop = LABEL_UNIQUE_PROP[label]
    rows = execute_and_fetch(
        COORDINATOR,
        f"MATCH (n:{label}) WITH n ORDER BY rand() LIMIT 1 RETURN properties(n) AS props",
        protocol=Protocol.BOLT_ROUTING,
    )
    props: dict = dict(rows[0]["props"]) if rows else {}
    props[prop] = _random_unique_value(label)
    execute_query(
        COORDINATOR,
        f"CREATE (n:{label}) SET n = $props",
        params={"props": props},
        protocol=Protocol.BOLT_ROUTING,
        query_type=QueryType.WRITE,
        apply_retry_mechanism=True,
    )


def op_create_node_with_edges() -> None:
    """Pick a random Publication, create a new one with a fresh DOI linked to
    the same Journal, Date, and Authors — all in a single query."""
    new_doi = f"10.stress/{random.randint(10_000_000, 999_999_999)}"
    execute_query(
        COORDINATOR,
        """
        MATCH (p:Publication)
        WITH p ORDER BY rand() LIMIT 1
        OPTIONAL MATCH (p)-[:PUBLISHED_IN]->(j:Journal)
        WITH p, collect(j.journal_name)[0] AS journal_name
        OPTIONAL MATCH (p)-[:PUBLISHED_ON]->(d:Date)
        WITH p, journal_name, collect(d.publish_date)[0] AS publish_date
        OPTIONAL MATCH (p)-[:AUTHORED_BY]->(a:Author)
        WITH journal_name, publish_date, collect(a.author_id) AS author_ids
        CREATE (new:Publication {doi: $new_doi})
        WITH new, journal_name, publish_date, author_ids
        OPTIONAL MATCH (j:Journal {journal_name: journal_name})
        FOREACH (_ IN CASE WHEN j IS NOT NULL THEN [1] ELSE [] END | MERGE (new)-[:PUBLISHED_IN]->(j))
        WITH new, publish_date, author_ids
        OPTIONAL MATCH (d:Date {publish_date: publish_date})
        FOREACH (_ IN CASE WHEN d IS NOT NULL THEN [1] ELSE [] END | MERGE (new)-[:PUBLISHED_ON]->(d))
        WITH new, author_ids
        UNWIND author_ids AS aid
        MATCH (a:Author {author_id: aid})
        MERGE (new)-[:AUTHORED_BY]->(a)
        """,
        params={"new_doi": new_doi},
        protocol=Protocol.BOLT_ROUTING,
        query_type=QueryType.WRITE,
        apply_retry_mechanism=True,
    )


def op_detach_delete_random() -> None:
    """Pick a random node from a random label and detach delete it."""
    label = random.choice(LABELS)
    execute_query(
        COORDINATOR,
        f"MATCH (n:{label}) WITH n ORDER BY rand() LIMIT 1 DETACH DELETE n",
        protocol=Protocol.BOLT_ROUTING,
        query_type=QueryType.WRITE,
        apply_retry_mechanism=True,
    )


def op_add_journal_edge() -> None:
    """Add a PUBLISHED_IN edge from a random Publication to a random Journal."""
    execute_query(
        COORDINATOR,
        """
        MATCH (p:Publication) WITH p ORDER BY rand() LIMIT 1
        MATCH (j:Journal) WITH p, j ORDER BY rand() LIMIT 1
        MERGE (p)-[:PUBLISHED_IN]->(j)
        """,
        protocol=Protocol.BOLT_ROUTING,
        query_type=QueryType.WRITE,
        apply_retry_mechanism=True,
    )


def op_remove_journal_edge() -> None:
    """Remove a random PUBLISHED_IN edge between a Publication and a Journal."""
    execute_query(
        COORDINATOR,
        """
        MATCH (p:Publication)-[r:PUBLISHED_IN]->(j:Journal)
        WITH r ORDER BY rand() LIMIT 1
        DELETE r
        """,
        protocol=Protocol.BOLT_ROUTING,
        query_type=QueryType.WRITE,
        apply_retry_mechanism=True,
    )


def run_worker(worker_id: int, num_iterations: int) -> dict:
    """Run random concurrent workload iterations for one worker."""
    counts = {"create_no_edge": 0, "create_with_edge": 0, "delete": 0, "add_edge": 0, "remove_edge": 0}
    ops = list(counts.keys())

    for _ in range(num_iterations):
        choice = random.choice(ops)
        try:
            if choice == "create_no_edge":
                op_create_node_no_edges(random.choice(LABELS))
            elif choice == "create_with_edge":
                op_create_node_with_edges()
            elif choice == "delete":
                op_detach_delete_random()
            elif choice == "add_edge":
                op_add_journal_edge()
            else:
                op_remove_journal_edge()
            counts[choice] += 1
        except Exception as e:
            print(f"  [Worker {worker_id}] {choice} error: {e}")
            raise

    print(f"  [Worker {worker_id}] done: " + ", ".join(f"{k}={v:,}" for k, v in counts.items()))
    return {"worker_id": worker_id, **counts}


def run_concurrent_workload() -> None:
    iters_per_worker = ITERATIONS // NUM_WORKERS
    print(f"Running {ITERATIONS:,} iterations across {NUM_WORKERS} workers " f"({iters_per_worker:,} per worker)...")
    tasks = [(worker_id, iters_per_worker) for worker_id in range(NUM_WORKERS)]
    results = run_parallel(run_worker, tasks, num_workers=NUM_WORKERS)

    totals: dict[str, int] = {}
    for r in results:
        for k, v in r.items():
            if k != "worker_id":
                totals[k] = totals.get(k, 0) + v
    print("Totals: " + ", ".join(f"{k}={v:,}" for k, v in totals.items()))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    print("=" * 60)
    print("Publications Dataset Import Workload")
    print("=" * 60)
    print(f"S3 Bucket: {S3_BUCKET}")
    print(f"Dataset Path: {S3_DATASET_PATH}")
    print(f"Region: {S3_REGION}")
    print("-" * 60)

    s3_secret = get_s3_secret()
    print("S3 credentials loaded from environment variables.")

    monitor = ClusterMonitor(
        coordinators=COORDINATORS,
        show_replicas=True,
        verify_up=True,
        storage_info=["vertex_count", "edge_count", "memory_res", "allocation_limit"],
        interval=5,
    )

    # Phase 1: Import
    print("\n" + "=" * 60)
    print("PHASE 1: Import data")
    print("=" * 60)

    phase1_start = time.time()
    with monitor:
        create_indices_and_constraints()
        import_data(s3_secret)
    phase1_elapsed = time.time() - phase1_start

    print(f"\nPhase 1 import time: {phase1_elapsed:.1f}s ({phase1_elapsed / 60:.1f} minutes)")
    print("\nNode counts after import:")
    counts = get_node_counts()
    for label, count in counts.items():
        print(f"  {label}: {count:,}")

    # Phase 2: Concurrent workload
    print("\n" + "=" * 60)
    print("PHASE 2: Concurrent workload")
    print("=" * 60)
    print(f"Workers: {NUM_WORKERS}, Total iterations: {ITERATIONS:,}")
    print("Operations: create_no_edge, create_with_edge, delete, add_edge, remove_edge")
    print("-" * 60)

    with monitor:
        run_concurrent_workload()

    print("\nWaiting 30 seconds before final verification...")
    time.sleep(30)

    print("\nFinal replica status:")
    monitor.show_replicas()

    ok = monitor.verify_all_ready() and monitor.verify_instances_up()
    if not ok:
        sys.exit(1)
    print("Workload completed successfully!")


if __name__ == "__main__":
    main()
