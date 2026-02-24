"""
Common utilities for EKS HA stress tests.

Unlike docker_ha/common.py which uses hardcoded localhost ports, this module
resolves instance hosts at import time by querying kubectl for external
LoadBalancer IPs. All EKS instances expose bolt on port 7687.
"""

import os
import re
import subprocess
from enum import Enum
from functools import lru_cache
from typing import Any, Callable, TypeVar

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

BOLT_PORT = 7687

KUBECONFIG_PATH = os.environ.get("KUBECONFIG", "/tmp/eks-ha-kubeconfig")

COORDINATOR_INSTANCES = {"coord_1", "coord_2", "coord_3"}

# Maps logical instance names used by workload scripts to K8s service names
_INSTANCE_TO_SERVICE = {
    "coord_1": "memgraph-coordinator-1-external",
    "coord_2": "memgraph-coordinator-2-external",
    "coord_3": "memgraph-coordinator-3-external",
    "data_1": "memgraph-data-0-external",
    "data_2": "memgraph-data-1-external",
}


def _kubectl(*args: str) -> str:
    cmd = ["kubectl", f"--kubeconfig={KUBECONFIG_PATH}", *args]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        raise RuntimeError(f"kubectl failed: {result.stderr.strip()}")
    return result.stdout.strip()


def _resolve_lb_ip(service_name: str) -> str:
    """Resolve a LoadBalancer service to an IP (or hostname)."""
    ip = _kubectl(
        "get",
        "svc",
        service_name,
        "-o",
        "jsonpath={.status.loadBalancer.ingress[0].ip}",
    )
    if ip:
        return ip

    hostname = _kubectl(
        "get",
        "svc",
        service_name,
        "-o",
        "jsonpath={.status.loadBalancer.ingress[0].hostname}",
    )
    if not hostname:
        raise RuntimeError(f"No external IP/hostname for service {service_name}")

    # AWS returns DNS names; resolve to IP via dig
    dig = subprocess.run(
        ["dig", "+short", hostname],
        capture_output=True,
        text=True,
        timeout=10,
    )
    for line in dig.stdout.splitlines():
        if re.match(r"^\d+\.\d+\.\d+\.\d+$", line):
            return line

    return hostname


@lru_cache(maxsize=None)
def _get_instance_host(instance_name: str) -> str:
    svc = _INSTANCE_TO_SERVICE.get(instance_name)
    if svc is None:
        raise ValueError(f"Unknown instance: {instance_name}. " f"Valid: {list(_INSTANCE_TO_SERVICE.keys())}")
    return _resolve_lb_ip(svc)


def create_bolt_driver_for(
    instance_name: str,
    host: str | None = None,
    auth: tuple[str, str] = ("", ""),
):
    if host is None:
        host = _get_instance_host(instance_name)
    uri = f"bolt://{host}:{BOLT_PORT}"
    return GraphDatabase.driver(uri, auth=auth)


def create_bolt_routing_driver_for(
    instance_name: str,
    host: str | None = None,
    auth: tuple[str, str] = ("", ""),
):
    if instance_name not in COORDINATOR_INSTANCES:
        raise ValueError(
            f"bolt+routing is only allowed on coordinator instances. "
            f"Got: {instance_name}. Valid coordinators: {COORDINATOR_INSTANCES}"
        )
    if host is None:
        host = _get_instance_host(instance_name)
    uri = f"neo4j://{host}:{BOLT_PORT}"
    return GraphDatabase.driver(uri, auth=auth)


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
