# Copyright 2026 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

"""
NUMA topology detection and CPU assignment utilities for mgbench.
"""

import os
import re
from typing import List, Optional, Tuple


class CPUCore:
    """Represents a CPU core with its physical and logical information."""

    def __init__(self, logical_id: int, physical_core: int, numa_node: int, is_hyperthread: bool):
        self.logical_id = logical_id
        self.physical_core = physical_core
        self.numa_node = numa_node
        self.is_hyperthread = is_hyperthread

    def __lt__(self, other):
        # Sort by NUMA node first, then physical core, then logical ID
        if self.numa_node != other.numa_node:
            return self.numa_node < other.numa_node
        if self.physical_core != other.physical_core:
            return self.physical_core < other.physical_core
        return self.logical_id < other.logical_id


class NUMANode:
    """Represents a NUMA node with its CPUs."""

    def __init__(self, node_id: int):
        self.node_id = node_id
        self.cpus: List[CPUCore] = []

    def get_primary_cores(self) -> List[int]:
        """Get primary cores (non-hyperthreads) in this NUMA node."""
        return [cpu.logical_id for cpu in self.cpus if not cpu.is_hyperthread]

    def get_hyperthreads(self) -> List[int]:
        """Get hyperthreads in this NUMA node."""
        return [cpu.logical_id for cpu in self.cpus if cpu.is_hyperthread]

    def get_primary_core_count(self) -> int:
        """Get number of primary cores in this NUMA node."""
        return len(self.get_primary_cores())

    def get_hyperthread_count(self) -> int:
        """Get number of hyperthreads in this NUMA node."""
        return len(self.get_hyperthreads())


class NUMATopology:
    """NUMA topology information."""

    def __init__(self):
        self.nodes: List[NUMANode] = []
        self.total_cores = 0
        self.total_hyperthreads = 0

    def get_numa_node_count(self) -> int:
        """Get number of NUMA nodes."""
        return len(self.nodes)

    def get_node(self, node_id: int) -> Optional[NUMANode]:
        """Get NUMA node by ID."""
        for node in self.nodes:
            if node.node_id == node_id:
                return node
        return None


def parse_cpu_list(cpu_list_str: str) -> List[int]:
    """
    Parse CPU list string (e.g., "0-7,16-23" or "0,2,4,6").

    Args:
        cpu_list_str: String representation of CPU list

    Returns:
        List of CPU IDs
    """
    cpus = []
    for range_str in cpu_list_str.split(","):
        range_str = range_str.strip()
        if "-" in range_str:
            # Range like "0-7"
            start, end = map(int, range_str.split("-"))
            cpus.extend(range(start, end + 1))
        else:
            # Single CPU
            cpus.append(int(range_str))
    return sorted(cpus)


def detect_topology() -> Optional[NUMATopology]:
    """
    Detect NUMA topology of the system.
    Works on Intel, AMD, and ARM architectures.

    Returns:
        NUMA topology or None if detection fails
    """
    topology = NUMATopology()

    # First, discover all NUMA nodes
    numa_nodes = []
    node_id = 0
    while node_id < 256:  # Reasonable limit
        cpulist_path = f"/sys/devices/system/node/node{node_id}/cpulist"
        if not os.path.exists(cpulist_path):
            break  # No more NUMA nodes
        numa_nodes.append(node_id)
        node_id += 1

    if not numa_nodes:
        # No NUMA nodes detected, assume single NUMA node (node 0)
        numa_nodes = [0]

    # For each NUMA node, get its CPUs
    for node_id in numa_nodes:
        numa_node = NUMANode(node_id)

        cpulist_path = f"/sys/devices/system/node/node{node_id}/cpulist"
        try:
            with open(cpulist_path, "r") as f:
                cpulist_str = f.read().strip()
        except IOError:
            continue

        node_cpus = parse_cpu_list(cpulist_str)

        # For each CPU, get its physical core and determine if it's a hyperthread
        for cpu_id in node_cpus:
            core = CPUCore(cpu_id, cpu_id, node_id, False)  # Default values

            # Get physical core ID
            core_id_path = f"/sys/devices/system/cpu/cpu{cpu_id}/topology/core_id"
            try:
                with open(core_id_path, "r") as f:
                    core.physical_core = int(f.read().strip())
            except IOError:
                core.physical_core = cpu_id  # Fallback

            # Determine if this is a hyperthread by checking siblings
            siblings_path = f"/sys/devices/system/cpu/cpu{cpu_id}/topology/thread_siblings_list"
            try:
                with open(siblings_path, "r") as f:
                    siblings_str = f.read().strip()
                    siblings = parse_cpu_list(siblings_str)
                    # Sort siblings to get the primary (lowest ID)
                    if siblings:
                        siblings.sort()
                        core.is_hyperthread = siblings[0] != cpu_id
            except IOError:
                core.is_hyperthread = False  # Assume primary if we can't determine

            numa_node.cpus.append(core)

        # Sort CPUs within the node (primary cores first, then hyperthreads)
        numa_node.cpus.sort()
        topology.nodes.append(numa_node)

    # Calculate totals
    for node in topology.nodes:
        for cpu in node.cpus:
            if cpu.is_hyperthread:
                topology.total_hyperthreads += 1
            else:
                topology.total_cores += 1

    return topology


def assign_cpus_to_clients(num_clients: int, topology: Optional[NUMATopology] = None) -> List[Optional[int]]:
    """
    Assign CPUs to clients using the same logic as PriorityThreadPool:
    - Fill one NUMA node at a time
    - Primary cores first, then hyperthreads
    - If not enough CPUs, return None for remaining clients (no pinning)

    Args:
        num_clients: Number of clients to assign CPUs to
        topology: NUMA topology (if None, will be detected)

    Returns:
        List of CPU IDs, one per client. None means no pinning for that client.
    """
    if topology is None:
        topology = detect_topology()
        if topology is None:
            return [None] * num_clients

    # Build CPU assignment: fill one NUMA node at a time, primary cores first, then hyperthreads
    cpu_assignments = []

    # First pass: assign primary cores, one NUMA node at a time
    for node in topology.nodes:
        primary_cores = node.get_primary_cores()
        for cpu in primary_cores:
            cpu_assignments.append(cpu)

    # Second pass: assign hyperthreads if we need more CPUs
    if len(cpu_assignments) < num_clients:
        for node in topology.nodes:
            hyperthreads = node.get_hyperthreads()
            for cpu in hyperthreads:
                cpu_assignments.append(cpu)
                if len(cpu_assignments) >= num_clients:
                    break
            if len(cpu_assignments) >= num_clients:
                break

    # If we don't have enough CPUs, extend by repeating assignments cyclically
    if len(cpu_assignments) < num_clients and len(cpu_assignments) > 0:
        # Repeat assignments cyclically
        original_len = len(cpu_assignments)
        while len(cpu_assignments) < num_clients:
            cpu_assignments.append(cpu_assignments[len(cpu_assignments) % original_len])

    # Return assignments (one per client)
    return cpu_assignments[:num_clients]


def assign_numa_nodes_to_clients(num_clients: int, topology: Optional[NUMATopology] = None) -> List[Optional[int]]:
    """
    Assign NUMA nodes to clients using the same logic as PriorityThreadPool:
    - Fill one NUMA node at a time
    - Primary cores first (one client per primary core), then hyperthreads
    - Clients are pinned to NUMA groups, not individual cores

    Strategy:
    1. Fill NUMA node 0 with primary cores (one client per primary core)
    2. Then fill NUMA node 1 with primary cores
    3. Continue until all primary cores across all NUMA nodes are used
    4. Then loop back and fill hyperthreaded cores in the same order

    Args:
        num_clients: Number of clients to assign NUMA nodes to
        topology: NUMA topology (if None, will be detected)

    Returns:
        List of NUMA node IDs, one per client. None means no pinning for that client.
    """
    if topology is None:
        topology = detect_topology()
        if topology is None:
            return [None] * num_clients

    numa_assignments = []

    # First pass: assign primary cores, one NUMA node at a time
    # Each primary core gets one client assigned to its NUMA node
    for node in topology.nodes:
        primary_core_count = node.get_primary_core_count()
        for _ in range(primary_core_count):
            numa_assignments.append(node.node_id)

    # Second pass: assign hyperthreads if we need more clients
    # Each hyperthread gets one client assigned to its NUMA node
    if len(numa_assignments) < num_clients:
        for node in topology.nodes:
            hyperthread_count = node.get_hyperthread_count()
            for _ in range(hyperthread_count):
                numa_assignments.append(node.node_id)
                if len(numa_assignments) >= num_clients:
                    break
            if len(numa_assignments) >= num_clients:
                break

    # If we don't have enough assignments, extend by repeating cyclically
    if len(numa_assignments) < num_clients and len(numa_assignments) > 0:
        # Repeat assignments cyclically
        original_len = len(numa_assignments)
        while len(numa_assignments) < num_clients:
            numa_assignments.append(numa_assignments[len(numa_assignments) % original_len])

    # Return assignments (one per client)
    return numa_assignments[:num_clients]
