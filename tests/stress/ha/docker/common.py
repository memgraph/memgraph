"""
Common utilities for Docker HA stress tests.

Thin wrapper around ha_common — provides deployment-specific instance resolution
and re-exports all shared symbols so workloads can `from common import ...` unchanged.
"""
import os
import subprocess
import sys

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

_COMMON_DIR = os.path.dirname(os.path.abspath(__file__))
DEPLOYMENT_SCRIPT = os.path.join(_COMMON_DIR, "deployment", "deployment.sh")

INSTANCE_PORTS = {
    "data_1": 7687,
    "data_2": 7688,
    "coord_1": 7691,
    "coord_2": 7692,
    "coord_3": 7693,
}


def _resolve_instance(instance_name: str) -> tuple[str, int]:
    if instance_name not in INSTANCE_PORTS:
        raise ValueError(f"Unknown instance: {instance_name}. Valid: {list(INSTANCE_PORTS.keys())}")
    return ("127.0.0.1", INSTANCE_PORTS[instance_name])


ha_common.configure(_resolve_instance, instance_names=list(INSTANCE_PORTS.keys()))


def restart_container(instance_name: str) -> None:
    """Restart a specific container using the deployment script."""
    print(f"Restarting instance: {instance_name}")
    result = subprocess.run(
        [DEPLOYMENT_SCRIPT, "restart", instance_name],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise Exception(f"Failed to restart {instance_name}: {result.stderr}")
    print(f"Instance {instance_name} restart triggered")
