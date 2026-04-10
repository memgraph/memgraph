#!/usr/bin/env python3
"""
Responsive reads workload.

Creates 10M nodes in chunks while a read worker continuously runs
SHOW INDEXES and SHOW METRICS in the main thread. Each read query
must complete in under 1 second or the test fails immediately.
"""
import sys
import threading
import time

from cluster_monitor import ClusterMonitor
from ha_common import Protocol, QueryType, cleanup, execute_and_fetch, execute_query

COORDINATOR = "coord_1"
COORDINATORS = ["coord_1", "coord_2", "coord_3"]

TOTAL_NODES = 10_000_000
LABELS = [f"L{i}" for i in range(10)]
PROPERTIES = [f"p{i}" for i in range(10)]

SETUP_CHUNK = 1_000_000  # nodes per CREATE batch during setup

MAX_READ_LATENCY_S = 1.0

PRIVILEGES = ["MATCH", "CREATE", "MERGE", "DELETE", "SET", "REMOVE", "INDEX", "STATS", "AUTH", "REPLICATION"]
_created_users: list[str] = []
_created_roles: list[str] = []
_user_lock = threading.Lock()

ADMIN_USER = "admin"
ADMIN_PASSWORD = "test"
AUTH = (ADMIN_USER, ADMIN_PASSWORD)


def setup_admin_user() -> None:
    """Create the admin user before auth is enabled (unauthenticated connection)."""
    execute_query(
        COORDINATOR,
        f"CREATE USER {ADMIN_USER} IDENTIFIED BY '{ADMIN_PASSWORD}';",
        protocol=Protocol.BOLT_ROUTING,
        query_type=QueryType.WRITE,
    )
    execute_query(
        COORDINATOR,
        f"GRANT ALL PRIVILEGES TO {ADMIN_USER};",
        protocol=Protocol.BOLT_ROUTING,
        query_type=QueryType.WRITE,
    )
    print(f"  [Setup] Admin user '{ADMIN_USER}' created with all privileges.")


_user_counter = 0


def _next_user() -> tuple[str, str, str]:
    """Return (username, password, role_name) for the next sequential user."""
    global _user_counter
    _user_counter += 1
    n = _user_counter
    return f"user_{n}", f"pass_{n}", f"role_{n}"


def _cleanup_users() -> None:
    """Drop all users and roles created during the workload."""
    with _user_lock:
        users = list(_created_users)
        roles = list(_created_roles)
    for username in users:
        try:
            execute_query(
                COORDINATOR,
                f"DROP USER {username};",
                protocol=Protocol.BOLT_ROUTING,
                query_type=QueryType.WRITE,
                auth=AUTH,
            )
        except Exception:
            pass
    for role_name in roles:
        try:
            execute_query(
                COORDINATOR,
                f"DROP ROLE {role_name};",
                protocol=Protocol.BOLT_ROUTING,
                query_type=QueryType.WRITE,
                auth=AUTH,
            )
        except Exception:
            pass


def _create_nodes(done: threading.Event, error: list) -> None:
    """Create TOTAL_NODES nodes with labels and id property, in chunks."""
    try:
        execute_query(
            COORDINATOR,
            "CREATE INDEX ON :L0(id);",
            protocol=Protocol.BOLT_ROUTING,
            query_type=QueryType.WRITE,
            auth=AUTH,
        )
        t0 = time.time()
        for start in range(0, TOTAL_NODES, SETUP_CHUNK):
            end = min(start + SETUP_CHUNK, TOTAL_NODES)
            execute_query(
                COORDINATOR,
                "UNWIND range($start, $end - 1) AS id CREATE (:L0:L1:L2:L3:L4:L5:L6:L7:L8:L9 {id: id})",
                params={"start": start, "end": end},
                protocol=Protocol.BOLT_ROUTING,
                query_type=QueryType.WRITE,
                auth=AUTH,
            )
            elapsed = time.time() - t0
            print(f"  [Ingestion] {end:>10,}/{TOTAL_NODES:,} nodes  ({elapsed:.0f}s)")
        elapsed = time.time() - t0
        print(f"  [Ingestion] Done: {TOTAL_NODES:,} nodes in {elapsed:.1f}s")
    except Exception as e:
        error[0] = e
    finally:
        done.set()


def run_workload() -> None:
    """Create nodes in a background thread while the main thread polls read queries."""
    ingestion_done = threading.Event()
    ingestion_error = [None]

    print(f"\nCreating {TOTAL_NODES:,} nodes + read worker in parallel...")

    t0 = time.time()
    ingestion_thread = threading.Thread(target=_create_nodes, args=(ingestion_done, ingestion_error), daemon=True)
    ingestion_thread.start()

    iterations = 0
    max_latency = 0.0

    num_labels = len(LABELS)

    while not ingestion_done.is_set():
        username, password, role_name = _next_user()
        # Cycle through privilege subsets and labels deterministically
        n = _user_counter
        privileges = ", ".join(PRIVILEGES[: (n % len(PRIVILEGES)) + 1])
        label = LABELS[n % num_labels]

        with _user_lock:
            _created_users.append(username)
            _created_roles.append(role_name)

        steps: list[tuple[str, str, QueryType]] = [
            ("SHOW INDEXES", "SHOW INDEXES", QueryType.READ),
            ("SHOW METRICS", "SHOW METRICS", QueryType.READ),
            (
                f"CREATE USER {username}",
                f"CREATE USER IF NOT EXISTS {username} IDENTIFIED BY '{password}'",
                QueryType.WRITE,
            ),
            (f"CREATE ROLE {role_name}", f"CREATE ROLE IF NOT EXISTS {role_name}", QueryType.WRITE),
            (f"GRANT {privileges} TO {role_name}", f"GRANT {privileges} TO {role_name}", QueryType.WRITE),
            (f"SET ROLE {role_name} FOR {username}", f"SET ROLE FOR {username} TO {role_name}", QueryType.WRITE),
            (
                f"GRANT READ :{label} TO {username}",
                f"GRANT READ ON NODES CONTAINING LABELS :{label} TO {username}",
                QueryType.WRITE,
            ),
        ]

        for name, query, qtype in steps:
            t_step = time.time()
            try:
                execute_query(COORDINATOR, query, protocol=Protocol.BOLT_ROUTING, query_type=qtype, auth=AUTH)
            except SystemExit:
                raise
            except Exception as e:
                print(f"  [Worker] WARNING: '{name}' failed: {e}")
                time.sleep(0.5)
                continue

            latency = time.time() - t_step
            max_latency = max(max_latency, latency)
            iterations += 1

            if latency > MAX_READ_LATENCY_S:
                print(
                    f"  [Worker] FAIL: '{name}' took {latency:.3f}s "
                    f"(limit: {MAX_READ_LATENCY_S}s) on iteration {iterations}"
                )
                sys.exit(1)

            if iterations % 5 == 0:
                print(f"  [Worker] {iterations} steps OK (max latency so far: {max_latency:.3f}s)")

            time.sleep(0.5)

            if ingestion_done.is_set():
                break

    ingestion_thread.join()
    elapsed = time.time() - t0

    if ingestion_error[0] is not None:
        print(f"ERROR: Ingestion failed: {ingestion_error[0]}")
        sys.exit(1)

    print(f"  [Worker] Finished: {iterations} steps in {elapsed:.1f}s (max latency: {max_latency:.3f}s)")

    print("  [User Worker] Cleaning up created users and roles...")
    _cleanup_users()
    with _user_lock:
        print(f"  [User Worker] Dropped {len(_created_users)} users and {len(_created_roles)} roles")

    print(f"\nAll done in {elapsed:.1f}s ({elapsed / 60:.1f} min)")


def main():
    print("=" * 60)
    print("Responsive Reads Workload")
    print("=" * 60)
    print(f"Total nodes       : {TOTAL_NODES:,}")
    print(f"Chunk size        : {SETUP_CHUNK:,}")
    print(f"Labels per node   : {len(LABELS)}  ({', '.join(LABELS)})")
    print(f"Props per node    : {len(PROPERTIES)}  ({', '.join(PROPERTIES)})")
    print(f"Max read latency  : {MAX_READ_LATENCY_S}s")
    print("-" * 60)

    monitor = ClusterMonitor(
        coordinators=COORDINATORS,
        show_replicas=True,
        verify_up=True,
        storage_info=["vertex_count", "memory_res", "global_runtime_allocation_limit"],
        interval=2,
        auth=AUTH,
    )

    total_start = time.time()
    try:
        with monitor:
            setup_admin_user()
            run_workload()

            result = execute_and_fetch(
                COORDINATOR,
                "MATCH (n:L0) RETURN count(n) AS cnt",
                protocol=Protocol.BOLT_ROUTING,
                auth=AUTH,
            )
            node_count = result[0]["cnt"] if result else 0
            print(f"Node count (via :L0): {node_count:,}  (expected: {TOTAL_NODES:,})")

            monitor.show_replicas()

            ok = monitor.verify_all_ready() and monitor.verify_instances_up()
            if not ok:
                print("ERROR: Cluster is not healthy!")
                sys.exit(1)

            if node_count != TOTAL_NODES:
                print(f"ERROR: Node count mismatch — got {node_count:,}, expected {TOTAL_NODES:,}")
                sys.exit(1)

        total_elapsed = time.time() - total_start
        print("=" * 60)
        print(f"Completed in {total_elapsed:.1f}s ({total_elapsed / 60:.1f} min)")
        print("Workload completed successfully!")
    except SystemExit:
        raise
    finally:
        cleanup(auth=AUTH)


if __name__ == "__main__":
    main()
