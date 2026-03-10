"""
Shared utilities for HA stress tests (ha/docker, ha/native, ha/eks).

Auto-configures from the STRESS_DEPLOYMENT environment variable set by
continuous_integration, or can be configured manually via configure().
"""

import atexit
import importlib
import logging
import multiprocessing
import os
import sys
import time
from enum import Enum
from typing import Any, Callable, Iterable, TypeVar

from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, TransientError

# Suppress neo4j driver noise (connection pool chatter, retry warnings, etc.)
# so they don't drown out workload and monitor output.
logging.getLogger("neo4j").setLevel(logging.CRITICAL)

R = TypeVar("R")

SYNC_REPLICA_ERROR = "At least one SYNC replica has not confirmed committing last transaction."
WRITE_ON_REPLICA_ERROR = "Write queries are forbidden on the replica instance"

COORDINATOR_INSTANCES = {"coord_1", "coord_2", "coord_3"}


class Protocol(Enum):
    BOLT = "BOLT"
    BOLT_ROUTING = "BOLT_ROUTING"


class QueryType(Enum):
    READ = "READ"
    WRITE = "WRITE"


# ---------------------------------------------------------------------------
# Deployment-specific configuration
# ---------------------------------------------------------------------------

_instance_resolver: Callable[[str], tuple[str, int]] | None = None
_instance_names: list[str] = []
_deployment_common = None


def configure(
    resolver: Callable[[str], tuple[str, int]],
    instance_names: list[str] | None = None,
) -> None:
    """
    Register the function that maps an instance name to (host, port).

    Called automatically via _auto_configure() or manually by a deployment's common.py.
    """
    global _instance_resolver, _instance_names
    _instance_resolver = resolver
    if instance_names is not None:
        _instance_names = list(instance_names)


def _auto_configure() -> None:
    """Import the deployment's common.py based on STRESS_DEPLOYMENT env var.

    STRESS_DEPLOYMENT should be a sub-path under tests/stress/ (e.g. "ha/docker").
    The deployment's common.py calls ha_common.configure() to register its resolver.
    """
    global _deployment_common
    deployment = os.environ.get("STRESS_DEPLOYMENT")
    if not deployment:
        return

    ha_dir = os.path.dirname(os.path.abspath(__file__))
    stress_dir = os.path.dirname(ha_dir)
    deploy_dir = os.path.join(stress_dir, deployment)
    if deploy_dir not in sys.path:
        sys.path.insert(0, deploy_dir)
    if ha_dir not in sys.path:
        sys.path.insert(0, ha_dir)

    _deployment_common = importlib.import_module("common")


_auto_configure()


def get_instance_names() -> list[str]:
    """Return all known instance names for the current deployment."""
    return list(_instance_names)


def get_restart_fn() -> Callable[[str], None] | None:
    """Return the deployment's restart_container function, or None."""
    if _deployment_common and hasattr(_deployment_common, "restart_container"):
        return _deployment_common.restart_container
    return None


def get_restart_all_fn() -> Callable[[], None] | None:
    """Return the deployment's restart_all function, or None."""
    if _deployment_common and hasattr(_deployment_common, "restart_all"):
        return _deployment_common.restart_all
    return None


def _resolve(instance_name: str) -> tuple[str, int]:
    if _instance_resolver is None:
        raise RuntimeError(
            "ha_common not configured — set STRESS_DEPLOYMENT env var or import a deployment's common.py"
        )
    return _instance_resolver(instance_name)


# ---------------------------------------------------------------------------
# Driver creation
# ---------------------------------------------------------------------------


def create_bolt_driver_for(
    instance_name: str,
    host: str | None = None,
    auth: tuple[str, str] = ("", ""),
):
    """Create a bolt:// driver for a specific instance."""
    if host is None:
        resolved_host, port = _resolve(instance_name)
    else:
        _, port = _resolve(instance_name)
        resolved_host = host
    uri = f"bolt://{resolved_host}:{port}"
    return GraphDatabase.driver(uri, auth=auth)


def create_routing_driver_for(
    instance_name: str,
    host: str | None = None,
    auth: tuple[str, str] = ("", ""),
):
    """Create a neo4j:// (bolt+routing) driver for a coordinator instance."""
    if instance_name not in COORDINATOR_INSTANCES:
        raise ValueError(
            f"bolt+routing is only allowed on coordinator instances. "
            f"Got: {instance_name}. Valid coordinators: {COORDINATOR_INSTANCES}"
        )
    if host is None:
        resolved_host, port = _resolve(instance_name)
    else:
        _, port = _resolve(instance_name)
        resolved_host = host
    uri = f"neo4j://{resolved_host}:{port}"
    return GraphDatabase.driver(uri, auth=auth)


# ---------------------------------------------------------------------------
# PID-aware driver cache (safe across multiprocessing forks)
# ---------------------------------------------------------------------------

_driver_cache: dict[tuple[int, str, str, str, str], Any] = {}


def _get_or_create_driver(instance_name: str, protocol: Protocol, auth: tuple[str, str] = ("", "")):
    pid = os.getpid()
    key = (pid, instance_name, protocol.value, auth[0], auth[1])
    if key not in _driver_cache:
        if protocol == Protocol.BOLT_ROUTING:
            _driver_cache[key] = create_routing_driver_for(instance_name, auth=auth)
        else:
            _driver_cache[key] = create_bolt_driver_for(instance_name, auth=auth)
    return _driver_cache[key]


def _close_all_drivers():
    pid = os.getpid()
    to_close = [k for k in _driver_cache if k[0] == pid]
    for key in to_close:
        try:
            _driver_cache.pop(key).close()
        except Exception:
            pass


atexit.register(_close_all_drivers)


# ---------------------------------------------------------------------------
# Parallel execution with early termination
# ---------------------------------------------------------------------------


def run_parallel(
    worker_fn: Callable[..., R],
    tasks: Iterable[tuple],
    num_workers: int = 4,
) -> list[R]:
    """Run tasks in parallel. Terminates all workers immediately if any one fails."""
    task_list = list(tasks)
    with multiprocessing.Pool(processes=num_workers) as pool:
        async_results = [pool.apply_async(worker_fn, t) for t in task_list]
        while True:
            for ar in async_results:
                if ar.ready() and not ar.successful():
                    pool.terminate()
                    pool.join()
                    ar.get()
            if all(ar.ready() for ar in async_results):
                break
            time.sleep(0.5)
        return [ar.get() for ar in async_results]


# ---------------------------------------------------------------------------
# Query execution
# ---------------------------------------------------------------------------


def _run_query(
    instance_name: str,
    query: str,
    params: dict[str, Any] | None,
    protocol: Protocol,
    query_type: QueryType,
    apply_retry_mechanism: bool,
    result_handler: Callable,
    database: str | None = None,
    auth: tuple[str, str] = ("", ""),
) -> Any:
    """
    Internal query runner. Returns None if SYNC replica error occurs
    (write succeeded on MAIN). Retries on transient routing/connection errors.
    Prints FATAL and re-raises on other errors.
    """
    max_retries = 3
    retry_delay = 2

    for attempt in range(1, max_retries + 1):
        driver = _get_or_create_driver(instance_name, protocol, auth=auth)
        try:
            with driver.session(database=database) as session:
                if apply_retry_mechanism:
                    if query_type == QueryType.WRITE:
                        return session.execute_write(lambda tx: result_handler(tx.run(query, params or {})))
                    else:
                        return session.execute_read(lambda tx: result_handler(tx.run(query, params or {})))
                else:
                    return result_handler(session.run(query, params or {}))
        except Exception as e:
            if SYNC_REPLICA_ERROR in str(e):
                print(f"\nWARN: Sync replica error (instance={instance_name}): {e}")
                return None
            if isinstance(e, ServiceUnavailable) or WRITE_ON_REPLICA_ERROR in str(e):
                pid = os.getpid()
                key = (pid, instance_name, protocol.value, auth[0], auth[1])
                _driver_cache.pop(key, None)
                if attempt < max_retries:
                    print(
                        f"\nWARN: Routing/connection error (attempt {attempt}/{max_retries}), "
                        f"retrying in {retry_delay}s... (instance={instance_name})"
                    )
                    time.sleep(retry_delay)
                    continue
            print(f"\nFATAL: {e} (instance={instance_name}, protocol={protocol.value})")
            raise


def execute_query(
    instance_name: str,
    query: str,
    params: dict[str, Any] | None = None,
    protocol: Protocol = Protocol.BOLT,
    query_type: QueryType = QueryType.READ,
    apply_retry_mechanism: bool = False,
    database: str | None = None,
    auth: tuple[str, str] = ("", ""),
) -> None:
    """Execute a query on a specific instance. Discards results."""
    _run_query(
        instance_name,
        query,
        params,
        protocol,
        query_type,
        apply_retry_mechanism,
        result_handler=lambda result: result.consume(),
        database=database,
        auth=auth,
    )


def execute_with_manual_retries(
    instance_name: str,
    query: str,
    params: dict[str, Any] | None = None,
    protocol: Protocol = Protocol.BOLT,
    database: str | None = None,
    auth: tuple[str, str] = ("", ""),
    max_retries: int = 5,
    base_delay: float = 1.0,
) -> None:
    """Execute a query without the built-in retry mechanism.

    Intended for operational queries such as CREATE/DROP INDEX and
    CREATE/DROP CONSTRAINT, which do not behave like regular Cypher
    queries and are not handled correctly by the Neo4j driver's auto-retry.

    Retries with exponential backoff on TransientError.
    All other exceptions are logged and ignored.
    """
    for attempt in range(max_retries + 1):
        try:
            driver = _get_or_create_driver(instance_name, protocol, auth=auth)
            with driver.session(database=database) as session:
                session.run(query, params or {}).consume()
            return
        except Exception as e:
            if SYNC_REPLICA_ERROR in str(e):
                print(f"\nWARN: SYNC_REPLICA_ERROR (instance={instance_name}): {e}")
                return
            retriable = isinstance(e, (TransientError, ServiceUnavailable)) or WRITE_ON_REPLICA_ERROR in str(e)
            if not retriable:
                raise
            if attempt == max_retries:
                print(f"\nWARN: Retriable error after {max_retries} retries (instance={instance_name}): {e}")
                return
            delay = base_delay * (2**attempt)
            print(
                f"\nWARN: Retriable error (attempt {attempt + 1}/{max_retries + 1}), "
                f"retrying in {delay:.1f}s... (instance={instance_name})"
            )
            time.sleep(delay)


def execute_and_fetch_with_manual_retries(
    instance_name: str,
    query: str,
    params: dict[str, Any] | None = None,
    protocol: Protocol = Protocol.BOLT,
    database: str | None = None,
    auth: tuple[str, str] = ("", ""),
    max_retries: int = 5,
    base_delay: float = 1.0,
) -> list[dict[str, Any]]:
    """Fetch results without the built-in retry mechanism.

    Intended for operational queries such as SHOW INDEX INFO,
    which do not behave like regular Cypher queries and are not handled
    correctly by the Neo4j driver's auto-retry.

    Retries with exponential backoff on TransientError, ServiceUnavailable,
    or write-on-replica errors. All other exceptions are re-raised.
    """
    for attempt in range(max_retries + 1):
        try:
            driver = _get_or_create_driver(instance_name, protocol, auth=auth)
            with driver.session(database=database) as session:
                return [record.data() for record in session.run(query, params or {})]
        except Exception as e:
            if SYNC_REPLICA_ERROR in str(e):
                print(f"\nWARN: SYNC_REPLICA_ERROR (instance={instance_name}): {e}")
                return []
            retriable = isinstance(e, (TransientError, ServiceUnavailable)) or WRITE_ON_REPLICA_ERROR in str(e)
            if not retriable:
                raise
            if attempt == max_retries:
                print(f"\nWARN: Retriable error after {max_retries} retries (instance={instance_name}): {e}")
                return []
            delay = base_delay * (2**attempt)
            print(
                f"\nWARN: Retriable error (attempt {attempt + 1}/{max_retries + 1}), "
                f"retrying in {delay:.1f}s... (instance={instance_name})"
            )
            time.sleep(delay)
    return []


def execute_and_fetch(
    instance_name: str,
    query: str,
    params: dict[str, Any] | None = None,
    protocol: Protocol = Protocol.BOLT,
    query_type: QueryType = QueryType.READ,
    apply_retry_mechanism: bool = False,
    database: str | None = None,
    auth: tuple[str, str] = ("", ""),
) -> list[dict[str, Any]]:
    """Execute a query and return all results as a list of dicts."""
    result = _run_query(
        instance_name,
        query,
        params,
        protocol,
        query_type,
        apply_retry_mechanism,
        result_handler=lambda result: [record.data() for record in result],
        database=database,
        auth=auth,
    )
    return result if result is not None else []


# ---------------------------------------------------------------------------
# Cluster cleanup
# ---------------------------------------------------------------------------


def cleanup(coordinator: str = "coord_1", auth: tuple[str, str] = ("", "")) -> None:
    """
    Full wipe of the cluster: drops all tenant databases, then wipes indexes,
    constraints, and graph data from the default database.

    Runs via bolt+routing on the given coordinator so changes propagate to
    all data instances. Safe to call between workloads when the cluster
    stays running.
    """
    rows = execute_and_fetch(coordinator, "SHOW DATABASES;", protocol=Protocol.BOLT_ROUTING, auth=auth)
    tenant_dbs = [next(iter(row.values())) for row in rows if next(iter(row.values())) != "memgraph"]
    if tenant_dbs:
        print(f"Cleanup: dropping tenant databases {tenant_dbs}...")
        for db_name in tenant_dbs:
            execute_with_manual_retries(
                coordinator,
                f"DROP DATABASE {db_name}",
                protocol=Protocol.BOLT_ROUTING,
                auth=auth,
            )
        print("Cleanup: tenant databases dropped.")

    print("Cleanup: deleting all nodes and edges...")
    execute_with_manual_retries(
        coordinator,
        "USING PERIODIC COMMIT 10000 MATCH (n) DETACH DELETE n",
        protocol=Protocol.BOLT_ROUTING,
        auth=auth,
    )

    print("Cleanup: dropping all indexes...")
    indexes = execute_and_fetch(coordinator, "SHOW INDEX INFO;", protocol=Protocol.BOLT_ROUTING, auth=auth)
    for idx in indexes:
        itype = idx["index type"]
        label = idx["label"]
        prop = idx["property"]

        if isinstance(prop, list):
            prop_str = ", ".join(prop)
        else:
            prop_str = prop

        if itype == "label":
            query = f"DROP INDEX ON :{label};"
        elif itype == "label+property":
            query = f"DROP INDEX ON :{label}({prop_str});"
        elif itype == "edge-type":
            query = f"DROP EDGE INDEX ON :{label};"
        elif itype == "edge-type+property":
            query = f"DROP EDGE INDEX ON :{label}({prop_str});"
        elif itype == "edge-property":
            query = f"DROP GLOBAL EDGE INDEX ON :({prop_str});"
        elif itype == "point":
            query = f"DROP POINT INDEX ON :{label}({prop_str});"
        elif itype.startswith("label_text") or itype.startswith("edge-type_text"):
            # type field contains the index name: e.g. "label_text (name: myIndex)"
            index_name = itype.split("name:")[-1].strip().rstrip(")")
            query = f"DROP TEXT INDEX {index_name};"
        elif itype.startswith("label+property_vector"):
            index_name = itype.split("name:")[-1].strip().rstrip(")")
            query = f"DROP VECTOR INDEX {index_name};"
        elif itype.startswith("edge-type+property_vector"):
            index_name = itype.split("name:")[-1].strip().rstrip(")")
            query = f"DROP VECTOR EDGE INDEX {index_name};"
        else:
            print(f"Cleanup: unknown index type '{itype}', skipping.")
            continue

        execute_with_manual_retries(coordinator, query, protocol=Protocol.BOLT_ROUTING, auth=auth)
    print(f"Cleanup: dropped {len(indexes)} indexes.")

    print("Cleanup: dropping all constraints...")
    execute_with_manual_retries(coordinator, "DROP ALL CONSTRAINTS", protocol=Protocol.BOLT_ROUTING, auth=auth)

    print("Cleanup complete.")
