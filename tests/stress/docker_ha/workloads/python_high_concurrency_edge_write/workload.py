#!/usr/bin/env python3
"""
Publications dataset import workload (python high-concurrency variant).
Imports scientific publications data from S3 parquet files using DuckDB migration.

Uses bolt+routing protocol with a single worker.
"""
import os
import sys
import threading
import time

# Add docker_ha directory to path for common imports
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))

from common import Protocol, QueryType, execute_and_fetch, execute_query, run_parallel

# Instance names
COORDINATOR = "coord_1"

# S3 configuration
S3_BUCKET = "memgraph-stress-tests-bucket"
S3_DATASET_PATH = "publications-dataset-1-percent"
S3_REGION = "eu-west-1"

# Queries
SHOW_REPLICAS_QUERY = "SHOW REPLICAS;"
SHOW_STORAGE_INFO_QUERY = "SHOW STORAGE INFO;"

# Concurrent journal cartesian product
NUM_JOURNAL_WORKERS = 5
NUM_JOURNAL_ITERATIONS = 1000

# Background worker configuration
STORAGE_INFO_INTERVAL_SECONDS = 5


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
CALL migrate.duckdb(
  "SELECT * FROM read_parquet('{s3_base}/publications.parquet')",
  ["{s3_secret}"])
YIELD row
MERGE (n:Publication {{doi: row.doi}});""",
        ),
        (
            "Date nodes",
            f"""
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


def journal_cartesian_worker(worker_id: int, s3_secret: str, s3_base: str) -> dict:
    """
    Each worker migrates the full journals cartesian product from DuckDB,
    but only processes rows where row_number % NUM_JOURNAL_WORKERS == worker_id.
    Pairs are ordered by journal name so partitions are deterministic and non-overlapping.
    """
    query = f"""
CALL migrate.duckdb(
  "SELECT j1_name, j2_name FROM (
    SELECT j1.journal_name AS j1_name,
           j2.journal_name AS j2_name,
           ROW_NUMBER() OVER (ORDER BY j1.journal_name, j2.journal_name) - 1 AS rn
    FROM read_parquet('{s3_base}/journals.parquet') j1,
         read_parquet('{s3_base}/journals.parquet') j2
    WHERE j1.journal_name < j2.journal_name
  ) WHERE rn % {NUM_JOURNAL_WORKERS} = {worker_id}",
  ["{s3_secret}"])
YIELD row
MATCH (a:Journal {{journal_name: row.j1_name}})
MATCH (b:Journal {{journal_name: row.j2_name}})
MERGE (a)-[:RELATED_TO]->(b);"""

    t0 = time.time()
    for iteration in range(NUM_JOURNAL_ITERATIONS):
        execute_query(
            COORDINATOR,
            query,
            protocol=Protocol.BOLT_ROUTING,
            query_type=QueryType.WRITE,
        )
        if (iteration + 1) % 10 == 0:
            print(f"  [Journal worker {worker_id}] {iteration + 1}/{NUM_JOURNAL_ITERATIONS} iterations")
    elapsed = time.time() - t0
    print(f"  [Journal worker {worker_id}] Completed {NUM_JOURNAL_ITERATIONS} iterations in {elapsed:.1f}s")
    return {"worker_id": worker_id, "elapsed": elapsed}


def run_journal_cartesian(s3_secret: str) -> None:
    s3_base = f"s3://{S3_BUCKET}/{S3_DATASET_PATH}"

    print(f"\nRunning {NUM_JOURNAL_WORKERS} concurrent journal cartesian product workers...")
    tasks = [(i, s3_secret, s3_base) for i in range(NUM_JOURNAL_WORKERS)]

    t0 = time.time()
    results = run_parallel(journal_cartesian_worker, tasks, num_workers=NUM_JOURNAL_WORKERS)
    elapsed = time.time() - t0

    print(f"  All {NUM_JOURNAL_WORKERS} workers completed in {elapsed:.1f}s")


def storage_info_worker(stop_event: threading.Event) -> None:
    while not stop_event.is_set():
        try:
            results = execute_and_fetch(
                COORDINATOR, SHOW_STORAGE_INFO_QUERY, protocol=Protocol.BOLT_ROUTING, query_type=QueryType.READ
            )
            info = {row["storage info"]: row["value"] for row in results if "storage info" in row}
            vertices = info.get("vertex_count", "?")
            edges = info.get("edge_count", "?")
            memory = info.get("memory_res", "?")
            print(
                f"\n[STORAGE INFO @ {time.strftime('%H:%M:%S')}] vertices={vertices}  edges={edges}  memory_res={memory}"
            )
        except Exception as e:
            print(f"\n[STORAGE INFO ERROR] {e}")

        stop_event.wait(STORAGE_INFO_INTERVAL_SECONDS)


SHOW_REPLICAS_INTERVAL_SECONDS = 5


def replicas_worker(stop_event: threading.Event) -> None:
    while not stop_event.is_set():
        try:
            results = execute_and_fetch(COORDINATOR, SHOW_REPLICAS_QUERY, protocol=Protocol.BOLT_ROUTING)
            if results:
                headers = list(results[0].keys())
                print(f"\n[SHOW REPLICAS @ {time.strftime('%H:%M:%S')}]")
                print("  " + ",".join(headers))
                for replica in results:
                    print("  " + ",".join(str(replica[h]) for h in headers))
        except Exception as e:
            print(f"\n[SHOW REPLICAS ERROR] {e}")

        stop_event.wait(SHOW_REPLICAS_INTERVAL_SECONDS)


def show_replicas() -> None:
    results = execute_and_fetch(COORDINATOR, SHOW_REPLICAS_QUERY, protocol=Protocol.BOLT_ROUTING)
    if results:
        headers = list(results[0].keys())
        print(",".join(headers))
        for replica in results:
            values = [str(replica[h]) for h in headers]
            print(",".join(values))


def verify_replicas_ready() -> bool:
    results = execute_and_fetch(COORDINATOR, SHOW_REPLICAS_QUERY, protocol=Protocol.BOLT_ROUTING)
    all_ready = True

    for replica in results:
        name = replica.get("name", "unknown")
        data_info = replica.get("data_info", {})

        for db_name, db_status in data_info.items():
            status = db_status.get("status", "unknown")
            if status != "ready":
                print(f"WARN: Replica {name}, database {db_name} status is '{status}', expected 'ready'")
                all_ready = False

    return all_ready


def get_node_counts() -> dict[str, int]:
    labels = ["Concept", "Journal", "Publication", "Date", "Section", "Author"]
    counts = {}

    for label in labels:
        result = execute_and_fetch(
            COORDINATOR, f"MATCH (n:{label}) RETURN count(n) as cnt", protocol=Protocol.BOLT_ROUTING
        )
        counts[label] = result[0]["cnt"] if result else 0

    return counts


def main():
    print("=" * 60)
    print("Python High-Concurrency Workload")
    print("=" * 60)
    print(f"S3 Bucket: {S3_BUCKET}")
    print(f"Dataset Path: {S3_DATASET_PATH}")
    print(f"Region: {S3_REGION}")
    print("-" * 60)

    s3_secret = get_s3_secret()
    print("S3 credentials loaded from environment variables.")

    stop_event = threading.Event()
    storage_thread = threading.Thread(target=storage_info_worker, args=(stop_event,), daemon=True)
    storage_thread.start()
    print(f"Started background STORAGE INFO worker (interval: {STORAGE_INFO_INTERVAL_SECONDS}s)")

    replicas_thread = threading.Thread(target=replicas_worker, args=(stop_event,), daemon=True)
    replicas_thread.start()
    print(f"Started background SHOW REPLICAS worker (interval: {SHOW_REPLICAS_INTERVAL_SECONDS}s)")

    total_start = time.time()

    try:
        create_indices_and_constraints()
        import_data(s3_secret)
        run_journal_cartesian(s3_secret)
    finally:
        stop_event.set()
        storage_thread.join(timeout=5)
        replicas_thread.join(timeout=5)
        print("\nStopped background workers")

    total_elapsed = time.time() - total_start

    print("-" * 60)
    print(f"Total import time: {total_elapsed:.1f}s ({total_elapsed/60:.1f} minutes)")

    print("\nNode counts:")
    counts = get_node_counts()
    for label, count in counts.items():
        print(f"  {label}: {count:,}")

    print("\nWaiting 30 seconds before final verification...")
    time.sleep(30)

    print("\nFinal replica status:")
    show_replicas()

    if verify_replicas_ready():
        print("\nAll replicas are in sync!")
        print("Workload completed successfully!")
    else:
        print("\nERROR: Not all replicas are in sync!")
        sys.exit(1)


if __name__ == "__main__":
    main()
