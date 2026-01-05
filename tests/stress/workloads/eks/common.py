#!/usr/bin/env python3
"""
Common utilities for EKS workloads.
Provides functions for interacting with the EKS deployment script.
"""
import os
import subprocess

from neo4j import GraphDatabase

# Deployment script path (relative to this file)
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEPLOYMENT_SCRIPT = os.path.join(_SCRIPT_DIR, "..", "..", "deployments", "eks_ha.sh")


def get_service_ip(service_name: str) -> str:
    """
    Get the external IP of a LoadBalancer service using eks_ha.sh.

    Args:
        service_name: Name of the service (e.g., "data-0", "memgraph-data-0")

    Returns:
        The external IP address.

    Raises:
        Exception: If the IP cannot be retrieved.
    """
    result = subprocess.run(
        [DEPLOYMENT_SCRIPT, "get-ip", service_name],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise Exception(f"Failed to get IP for {service_name}: {result.stderr}")
    return result.stdout.strip()


def wait_for_service_ip(service_name: str, timeout: int = 300) -> str:
    """
    Wait for a LoadBalancer service to get an external IP using eks_ha.sh.

    Args:
        service_name: Name of the service (e.g., "data-0", "memgraph-data-0")
        timeout: Maximum time to wait in seconds

    Returns:
        The external IP when available.

    Raises:
        Exception: If the IP is not available within the timeout.
    """
    result = subprocess.run(
        [DEPLOYMENT_SCRIPT, "wait-ip", service_name, str(timeout)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise Exception(f"Failed to wait for IP for {service_name}: {result.stderr}")
    return result.stdout.strip()


def restart_instance(instance_name: str) -> None:
    """
    Restart a specific instance using the EKS deployment script.
    Deletes the pod and returns immediately (StatefulSet will recreate it).

    Args:
        instance_name: Name of the instance to restart (e.g., "data-0", "data-1")

    Raises:
        Exception: If the restart command fails.
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


def create_driver(uri: str, username: str = "", password: str = ""):
    """
    Create a new Neo4j/Memgraph database driver.

    Args:
        uri: Connection URI (e.g., "bolt://localhost:7687")
        username: Database username (empty for no auth)
        password: Database password (empty for no auth)

    Returns:
        A Neo4j driver instance.
    """
    return GraphDatabase.driver(uri, auth=(username, password))
