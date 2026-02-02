"""
Common utilities for Docker HA stress tests.
"""
import os
import subprocess

# Deployment script path (relative to this file)
_COMMON_DIR = os.path.dirname(os.path.abspath(__file__))
DEPLOYMENT_SCRIPT = os.path.join(_COMMON_DIR, "deployment", "deployment.sh")


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
