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

_COMMON_DIR = os.path.dirname(os.path.abspath(__file__))
DEPLOYMENT_SCRIPT = os.path.join(_COMMON_DIR, "deployment", "deployment.sh")


def restart_instance(instance_name: str) -> None:
    """Restart a specific instance by calling the deployment script."""
    print(f"Restarting instance: {instance_name}")
    result = subprocess.run(
        [DEPLOYMENT_SCRIPT, "restart", instance_name],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise Exception(f"Failed to restart {instance_name}: {result.stderr}")
    print(f"Instance {instance_name} restart triggered")


def restart_all() -> None:
    """Restart all Memgraph pods via the deployment script (scale down/up)."""
    print("Restarting all Memgraph HA pods...")
    result = subprocess.run(
        [DEPLOYMENT_SCRIPT, "restart"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise Exception(f"Failed to restart all pods: {result.stderr}")
    _resolve_instance.cache_clear()
    print("All Memgraph instances restarted")
