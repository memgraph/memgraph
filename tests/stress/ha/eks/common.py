"""
Common utilities for EKS HA stress tests.

Thin wrapper around ha_common — resolves instance hosts at runtime by querying
kubectl for external LoadBalancer IPs. All EKS instances expose bolt on port 7687.
"""
import os
import re
import subprocess
import sys
from functools import lru_cache

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import ha_common
from ha_common import (  # noqa: F401 — re-exported for workloads
    COORDINATOR_INSTANCES,
    Protocol,
    QueryType,
    create_bolt_driver_for,
    create_routing_driver_for,
    execute_and_fetch,
    execute_query,
    get_instance_names,
    run_parallel,
)

BOLT_PORT = 7687

KUBECONFIG_PATH = os.environ.get("KUBECONFIG", "/tmp/eks-ha-kubeconfig")

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
def _resolve_instance(instance_name: str) -> tuple[str, int]:
    svc = _INSTANCE_TO_SERVICE.get(instance_name)
    if svc is None:
        raise ValueError(f"Unknown instance: {instance_name}. Valid: {list(_INSTANCE_TO_SERVICE.keys())}")
    return (_resolve_lb_ip(svc), BOLT_PORT)


ha_common.configure(_resolve_instance, instance_names=list(_INSTANCE_TO_SERVICE.keys()))
