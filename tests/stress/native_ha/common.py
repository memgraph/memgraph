"""
Common utilities for Native HA stress tests.
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

SYNC_REPLICA_ERROR = "At least one SYNC replica has not confirmed committing last transaction."

_COMMON_DIR = os.path.dirname(os.path.abspath(__file__))
DEPLOYMENT_SCRIPT = os.path.join(_COMMON_DIR, "deployment", "deployment.sh")

INSTANCE_PORTS = {
    "data_1": 7687,
    "data_2": 7688,
    "data_3": 7689,
    "coord_1": 7691,
    "coord_2": 7692,
    "coord_3": 7693,
}


def restart_container(instance_name: str) -> None:
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
    if instance_name not in INSTANCE_PORTS:
        raise ValueError(f"Unknown instance: {instance_name}. Valid instances: {list(INSTANCE_PORTS.keys())}")

    port = INSTANCE_PORTS[instance_name]
    uri = f"bolt://{host}:{port}"
    return GraphDatabase.driver(uri, auth=auth)


COORDINATOR_INSTANCES = {"coord_1", "coord_2", "coord_3"}


def create_bolt_routing_driver_for(instance_name: str, host: str = "127.0.0.1", auth: tuple[str, str] = ("", "")):
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
    with multiprocessing.Pool(processes=num_workers) as pool:
        return pool.starmap(worker_fn, tasks)


def _run_query(
    instance_name: str,
    query: str,
    params: dict[str, Any] | None,
    protocol: Protocol,
    query_type: QueryType,
    apply_retry_mechanism: bool,
    result_handler: Callable,
) -> Any:
    if protocol == Protocol.BOLT_ROUTING:
        driver = create_bolt_routing_driver_for(instance_name)
    else:
        driver = create_bolt_driver_for(instance_name)

    try:
        with driver.session() as session:
            if apply_retry_mechanism:
                if query_type == QueryType.WRITE:
                    return session.execute_write(lambda tx: result_handler(tx.run(query, params or {})))
                else:
                    return session.execute_read(lambda tx: result_handler(tx.run(query, params or {})))
            else:
                return result_handler(session.run(query, params or {}))
    except (ClientError, DatabaseError) as e:
        if SYNC_REPLICA_ERROR in str(e):
            return None
        raise
    finally:
        driver.close()


def execute_query(
    instance_name: str,
    query: str,
    params: dict[str, Any] | None = None,
    protocol: Protocol = Protocol.BOLT,
    query_type: QueryType = QueryType.READ,
    apply_retry_mechanism: bool = False,
) -> None:
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
