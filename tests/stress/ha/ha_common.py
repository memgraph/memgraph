"""
Shared utilities for HA stress tests (ha/docker, ha/native, ha/eks).

Auto-configures from the STRESS_DEPLOYMENT environment variable set by
continuous_integration, or can be configured manually via configure().
"""
import atexit
import importlib
import multiprocessing
import os
import sys
import time
from enum import Enum
from typing import Any, Callable, Iterable, TypeVar

from neo4j import GraphDatabase

R = TypeVar("R")

SYNC_REPLICA_ERROR = "At least one SYNC replica has not confirmed committing last transaction."

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

_driver_cache: dict[tuple[int, str, str], Any] = {}


def _get_or_create_driver(instance_name: str, protocol: Protocol):
    pid = os.getpid()
    key = (pid, instance_name, protocol.value)
    if key not in _driver_cache:
        if protocol == Protocol.BOLT_ROUTING:
            _driver_cache[key] = create_routing_driver_for(instance_name)
        else:
            _driver_cache[key] = create_bolt_driver_for(instance_name)
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
) -> Any:
    """
    Internal query runner. Returns None if SYNC replica error occurs
    (write succeeded on MAIN). Prints FATAL and re-raises on other errors.
    """
    driver = _get_or_create_driver(instance_name, protocol)

    try:
        with driver.session() as session:
            if apply_retry_mechanism:
                if query_type == QueryType.WRITE:
                    return session.execute_write(lambda tx: result_handler(tx.run(query, params or {})))
                else:
                    return session.execute_read(lambda tx: result_handler(tx.run(query, params or {})))
            else:
                return result_handler(session.run(query, params or {}))
    except Exception as e:
        if SYNC_REPLICA_ERROR in str(e):
            return None
        print(f"\nFATAL: {e} (instance={instance_name}, protocol={protocol.value})")
        raise


def execute_query(
    instance_name: str,
    query: str,
    params: dict[str, Any] | None = None,
    protocol: Protocol = Protocol.BOLT,
    query_type: QueryType = QueryType.READ,
    apply_retry_mechanism: bool = False,
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
    )


def execute_and_fetch(
    instance_name: str,
    query: str,
    params: dict[str, Any] | None = None,
    protocol: Protocol = Protocol.BOLT,
    query_type: QueryType = QueryType.READ,
    apply_retry_mechanism: bool = False,
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
    )
    return result if result is not None else []
