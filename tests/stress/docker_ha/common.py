"""
Common utilities for Docker HA stress tests.
"""
import multiprocessing
import os
import subprocess
from enum import Enum
from typing import Any, Callable, Iterable, TypeVar

from neo4j import GraphDatabase
from neo4j.exceptions import ClientError, DatabaseError


class Protocol(Enum):
    BOLT = "BOLT"
    BOLT_ROUTING = "BOLT_ROUTING"


class QueryType(Enum):
    READ = "READ"
    WRITE = "WRITE"


R = TypeVar("R")

# Error messages to ignore
SYNC_REPLICA_ERROR = "At least one SYNC replica has not confirmed committing last transaction."

# Deployment script path (relative to this file)
_COMMON_DIR = os.path.dirname(os.path.abspath(__file__))
DEPLOYMENT_SCRIPT = os.path.join(_COMMON_DIR, "deployment", "deployment.sh")

# Instance to bolt port mappings
INSTANCE_PORTS = {
    "data_1": 7687,
    "data_2": 7688,
    "coord_1": 7691,
    "coord_2": 7692,
    "coord_3": 7693,
}


def restart_container(instance_name: str) -> None:
    """
    Restart a specific container using the deployment script.

    Args:
        instance_name: Name of the instance to restart (e.g., "data_1", "coord_2")
    """
    print(f"Restarting instance: {instance_name}")
    result = subprocess.run(
        [DEPLOYMENT_SCRIPT, "restart", instance_name],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise Exception(f"Failed to restart {instance_name}: {result.stderr}")
    print(f"Instance {instance_name} restart triggered")


def create_bolt_driver_for(instance_name: str, host: str = "127.0.0.1", auth: tuple[str, str] = ("", "")):
    """
    Create a Neo4j/Memgraph driver for a specific instance.

    Args:
        instance_name: Name of the instance (data_1, data_2, coord_1, coord_2, coord_3)
        host: Host address. Defaults to "127.0.0.1".
        auth: Tuple of (username, password). Defaults to empty credentials.

    Returns:
        Neo4j driver instance.
    """
    if instance_name not in INSTANCE_PORTS:
        raise ValueError(f"Unknown instance: {instance_name}. Valid instances: {list(INSTANCE_PORTS.keys())}")

    port = INSTANCE_PORTS[instance_name]
    uri = f"bolt://{host}:{port}"
    return GraphDatabase.driver(uri, auth=auth)


COORDINATOR_INSTANCES = {"coord_1", "coord_2", "coord_3"}


def create_bolt_routing_driver_for(instance_name: str, host: str = "127.0.0.1", auth: tuple[str, str] = ("", "")):
    """
    Create a Neo4j/Memgraph routing driver for a coordinator instance.

    Args:
        instance_name: Name of the coordinator instance (coord_1, coord_2, coord_3)
        host: Host address. Defaults to "127.0.0.1".
        auth: Tuple of (username, password). Defaults to empty credentials.

    Returns:
        Neo4j driver instance with routing capability.

    Raises:
        ValueError: If instance_name is not a coordinator instance.
    """
    if instance_name not in INSTANCE_PORTS:
        raise ValueError(f"Unknown instance: {instance_name}. Valid instances: {list(INSTANCE_PORTS.keys())}")

    if instance_name not in COORDINATOR_INSTANCES:
        raise ValueError(
            f"bolt+routing is only allowed on coordinator instances. "
            f"Got: {instance_name}. Valid coordinators: {COORDINATOR_INSTANCES}"
        )

    port = INSTANCE_PORTS[instance_name]
    uri = f"neo4j://{host}:{port}"
    return GraphDatabase.driver(uri, auth=auth)


def run_parallel(
    worker_fn: Callable[..., R],
    tasks: Iterable[tuple],
    num_workers: int = 4,
) -> list[R]:
    """
    Run tasks in parallel using a process pool with starmap.

    Args:
        worker_fn: Function to execute for each task (must be a top-level function).
        tasks: Iterable of tuples, each tuple contains arguments for one worker call.
        num_workers: Number of parallel workers.

    Returns:
        List of results from each worker.
    """
    with multiprocessing.Pool(processes=num_workers) as pool:
        return pool.starmap(worker_fn, tasks)


def execute_query(
    instance_name: str,
    query: str,
    params: dict[str, Any] | None = None,
    protocol: Protocol = Protocol.BOLT,
    query_type: QueryType = QueryType.READ,
    apply_retry_mechanism: bool = False,
) -> Any:
    """
    Execute a query on a specific instance.

    Args:
        instance_name: Name of the instance (data_1, data_2, coord_1, coord_2, coord_3).
                       For BOLT_ROUTING, must be a coordinator (coord_1, coord_2, coord_3).
        query: Cypher query to execute.
        params: Optional query parameters.
        protocol: Protocol.BOLT or Protocol.BOLT_ROUTING.
        query_type: QueryType.READ or QueryType.WRITE (only used when apply_retry_mechanism=True).
        apply_retry_mechanism: If True, use transaction functions with automatic retry on transient errors.

    Returns:
        Query result counters.
    """
    if protocol == Protocol.BOLT_ROUTING:
        driver = create_bolt_routing_driver_for(instance_name)
    else:
        driver = create_bolt_driver_for(instance_name)

    try:
        with driver.session() as session:
            if apply_retry_mechanism:
                if query_type == QueryType.WRITE:
                    return session.execute_write(lambda tx: tx.run(query, params or {}).consume().counters)
                else:
                    return session.execute_read(lambda tx: tx.run(query, params or {}).consume().counters)
            else:
                result = session.run(query, params or {})
                return result.consume().counters
    except (ClientError, DatabaseError) as e:
        if SYNC_REPLICA_ERROR in str(e):
            # Ignore SYNC replica confirmation errors - the write succeeded on MAIN
            return None
        raise
    finally:
        driver.close()
