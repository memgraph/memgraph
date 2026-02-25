#!/usr/bin/env python3
"""
Publications dataset import workload (LOAD PARQUET high-concurrency variant).
Imports scientific publications data from S3 parquet files using LOAD PARQUET clause.

Uses bolt+routing protocol with a single worker for imports,
then 5 concurrent workers for journal cartesian product edge merging.
"""
import os
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))

from common import Protocol, QueryType, execute_and_fetch, execute_query, run_parallel

COORDINATOR = "coord_1"

S3_BUCKET = "memgraph-stress-tests-bucket"
S3_DATASET_PATH = "publications-dataset-1-percent"
S3_REGION = "eu-west-1"

SHOW_REPLICAS_QUERY = "SHOW REPLICAS;"
SHOW_STORAGE_INFO_QUERY = "SHOW STORAGE INFO;"

NUM_JOURNAL_WORKERS = 5
NUM_JOURNAL_ITERATIONS = 1000

STORAGE_INFO_INTERVAL_SECONDS = 5
SHOW_REPLICAS_INTERVAL_SECONDS = 5


def configure_s3_credentials() -> None:
    """Set S3 credentials via SET DATABASE SETTING so LOAD PARQUET can access S3."""
    access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

    if not access_key or not secret_key:
        raise ValueError(
            "AWS credentials not found. Please set AWS_ACCESS_KEY_ID and "
            "AWS_SECRET_ACCESS_KEY environment variables."
        )

    settings = [
        f"SET DATABASE SETTING 'aws.region' TO '{S3_REGION}';",
        f"SET DATABASE SETTING 'aws.access_key' TO '{access_key}';",
        f"SET DATABASE SETTING 'aws.secret_key' TO '{secret_key}';",
    ]

    for query in settings:
        execute_query(COORDINATOR, query, protocol=Protocol.BOLT_ROUTING, query_type=QueryType.WRITE)


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


def import_data() -> None:
    s3_base = f"s3://{S3_BUCKET}/{S3_DATASET_PATH}"

    imports = [
        (
            "Concept nodes",
            f"""
LOAD PARQUET FROM "{s3_base}/concepts.parquet" AS row
MERGE (n:Concept {{concept_id: row.concept_id}})
SET n += row;""",
        ),
        (
            "Journal nodes",
            f"""
LOAD PARQUET FROM "{s3_base}/journals.parquet" AS row
MERGE (n:Journal {{journal_name: row.journal_name}})
SET n += row;""",
        ),
        (
            "Publication nodes",
            f"""
LOAD PARQUET FROM "{s3_base}/publications.parquet" AS row
MERGE (n:Publication {{doi: row.doi}});""",
        ),
        (
            "Date nodes",
            f"""
LOAD PARQUET FROM "{s3_base}/publish_dates.parquet" AS row
MERGE (n:Date {{publish_date: row.publish_date}})
SET n += row;""",
        ),
        (
            "Section nodes",
            f"""
LOAD PARQUET FROM "{s3_base}/sections.parquet" AS row
MERGE (n:Section {{section_id: row.section_id}})
SET n += row;""",
        ),
        (
            "Author nodes",
            f"""
LOAD PARQUET FROM "{s3_base}/authors.parquet" AS row
MERGE (n:Author {{author_id: row.author_id}})
SET n += row;""",
        ),
        (
            "AUTHORED_BY relationships",
            f"""
LOAD PARQUET FROM "{s3_base}/rel_authored_by.parquet" AS row
MERGE (a:Author {{author_id: row.author_id}})
MERGE (p:Publication {{doi: row.publication_doi}})
MERGE (p)-[:AUTHORED_BY]->(a);""",
        ),
        (
            "CITES relationships",
            f"""
LOAD PARQUET FROM "{s3_base}/rel_cites_paper.parquet" AS row
MERGE (citing:Publication {{doi: row.citing_doi}})
MERGE (cited:Publication {{doi: row.cited_doi}})
MERGE (citing)-[:CITES]->(cited);""",
        ),
        (
            "PART_OF relationships",
            f"""
LOAD PARQUET FROM "{s3_base}/rel_part_of.parquet" AS row
MERGE (section:Section {{section_id: row.section_id}})
MERGE (publication:Publication {{doi: row.publication_doi}})
MERGE (section)-[:PART_OF]->(publication);""",
        ),
        (
            "PUBLISHED_IN relationships",
            f"""
LOAD PARQUET FROM "{s3_base}/rel_published_in.parquet" AS row
MERGE (journal:Journal {{journal_name: row.journal_name}})
MERGE (publication:Publication {{doi: row.publication_doi}})
MERGE (publication)-[:PUBLISHED_IN]->(journal);""",
        ),
        (
            "PUBLISHED_ON relationships",
            f"""
LOAD PARQUET FROM "{s3_base}/rel_published_on.parquet" AS row
MERGE (publication:Publication {{doi: row.publication_doi}})
MERGE (d:Date {{publish_date: row.publish_date}})
MERGE (publication)-[:PUBLISHED_ON]->(d);""",
        ),
        (
            "REFERENCES relationships",
            f"""
LOAD PARQUET FROM "{s3_base}/rel_references.parquet" AS row
MERGE (concept:Concept {{concept_id: row.concept_id}})
MERGE (section:Section {{section_id: row.section_id}})
CREATE (section)-[:row.type {{score: row.confidence_score}}]->(concept);""",
        ),
        (
            "SUBFIELD_OF relationships",
            f"""
LOAD PARQUET FROM "{s3_base}/rel_subfield_of.parquet" AS row
MERGE (parent_concept:Concept {{concept_id: row.parent_concept_id}})
MERGE (child_concept:Concept {{concept_id: row.child_concept_id}})
MERGE (child_concept)-[:SUBFIELD_OF]->(parent_concept);""",
        ),
        (
            "SYNONYM_OF relationships",
            f"""
LOAD PARQUET FROM "{s3_base}/rel_synonym_of.parquet" AS row
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


def journal_cartesian_worker(worker_id: int) -> dict:
    """
    Each worker MATCHes the cartesian product of Journal nodes already in the DB
    and MERGEs RELATED_TO edges. Partitioning is done via (id(a) + id(b)) % NUM_WORKERS
    so each worker handles a distinct, non-overlapping slice.
    """
    query = f"""
MATCH (a:Journal), (b:Journal)
WHERE a.journal_name < b.journal_name
  AND (id(a) + id(b)) % {NUM_JOURNAL_WORKERS} = {worker_id}
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


def run_journal_cartesian() -> None:
    print(f"\nRunning {NUM_JOURNAL_WORKERS} concurrent journal cartesian product workers...")
    tasks = [(i,) for i in range(NUM_JOURNAL_WORKERS)]

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
                    data_info = replica.get("data_info", {})
                    for db_name, db_status in data_info.items():
                        if isinstance(db_status, dict) and db_status.get("status") == "invalid":
                            name = replica.get("name", "unknown")
                            print(f"\n  FATAL: Replica '{name}' has invalid status for database '{db_name}'!")
                            print(f"  Replica crashed — aborting workload.")
                            os._exit(1)
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
    print("LOAD PARQUET High-Concurrency Edge Write Workload")
    print("=" * 60)
    print(f"S3 Bucket: {S3_BUCKET}")
    print(f"Dataset Path: {S3_DATASET_PATH}")
    print(f"Region: {S3_REGION}")
    print("-" * 60)

    configure_s3_credentials()
    print("S3 credentials configured via SET DATABASE SETTING.")

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
        import_data()
        run_journal_cartesian()
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
