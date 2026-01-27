// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace memgraph::utils::numa {

/**
 * Represents a CPU core with its physical and logical information.
 */
struct CPUCore {
  int logical_id;       // Logical CPU ID (as seen by OS)
  int physical_core;    // Physical core ID
  int numa_node;        // NUMA node this CPU belongs to
  bool is_hyperthread;  // Whether this is a hyperthread (SMT)

  bool operator<(const CPUCore &other) const {
    // Sort by NUMA node first, then physical core, then logical ID
    if (numa_node != other.numa_node) return numa_node < other.numa_node;
    if (physical_core != other.physical_core) return physical_core < other.physical_core;
    return logical_id < other.logical_id;
  }
};

/**
 * Represents a NUMA node with its CPUs.
 */
struct NUMANode {
  int node_id;
  std::vector<CPUCore> cpus;  // All CPUs in this NUMA node

  // Get primary cores (non-hyperthreads) first, then hyperthreads
  std::vector<int> GetPrimaryCores() const {
    std::vector<int> result;
    for (const auto &cpu : cpus) {
      if (!cpu.is_hyperthread) {
        result.push_back(cpu.logical_id);
      }
    }
    return result;
  }

  std::vector<int> GetHyperthreads() const {
    std::vector<int> result;
    for (const auto &cpu : cpus) {
      if (cpu.is_hyperthread) {
        result.push_back(cpu.logical_id);
      }
    }
    return result;
  }
};

/**
 * NUMA topology information.
 */
struct NUMATopology {
  std::vector<NUMANode> nodes;
  int total_cores;
  int total_hyperthreads;

  // Get number of NUMA nodes
  size_t GetNUMANodeCount() const { return nodes.size(); }

  // Get CPU for a specific NUMA node
  const NUMANode *GetNode(int node_id) const {
    for (const auto &node : nodes) {
      if (node.node_id == node_id) return &node;
    }
    return nullptr;
  }
};

/**
 * Detect NUMA topology of the system.
 * Works on Intel, AMD, and ARM architectures.
 *
 * For Intel/AMD: Uses /sys/devices/system/node/ and /sys/devices/system/cpu/
 * For ARM: Uses same approach (Linux kernel exposes same interface)
 *
 * @return NUMA topology or nullopt if detection fails
 */
std::optional<NUMATopology> DetectTopology();

/**
 * Get NUMA node for a given CPU.
 * @param cpu_id Logical CPU ID
 * @return NUMA node ID or -1 if not found
 */
int GetNUMANodeForCPU(int cpu_id);

/**
 * Parse CPU list string (e.g., "0-7,16-23" or "0,2,4,6").
 * @param cpu_list_str String representation of CPU list
 * @return Vector of CPU IDs
 */
std::vector<int> ParseCPUList(const std::string &cpu_list_str);

/**
 * Pin current thread to a specific CPU core.
 * @param cpu_id CPU core to pin to
 * @return true if successful, false otherwise
 */
bool PinThreadToCPU(int cpu_id);

/**
 * Pin current thread to a NUMA node (allows any CPU in that node).
 * @param numa_node NUMA node ID
 * @return true if successful, false otherwise
 */
bool PinThreadToNUMANode(int numa_node);

/**
 * Get current CPU that the calling thread is running on.
 * Uses syscall(SYS_getcpu) if available, falls back to /proc/self/stat.
 * @return CPU ID or -1 if detection fails
 */
int GetCurrentCPU();

/**
 * Get current NUMA node that the calling thread is running on.
 * @return NUMA node ID or -1 if detection fails
 */
int GetCurrentNUMANode();

/**
 * Detect where a network connection is coming from.
 * Uses SO_INCOMING_CPU (Linux 3.19+) and NAPI_ID to determine
 * which CPU/NUMA node handled the packet.
 *
 * @param socket_fd Socket file descriptor
 * @return CPU ID that received the packet, or -1 if detection fails
 */
int GetIncomingCPU(int socket_fd);

/**
 * Get NAPI ID for a socket (network device queue).
 * @param socket_fd Socket file descriptor
 * @return NAPI ID or -1 if not available
 */
int GetNAPIID(int socket_fd);

}  // namespace memgraph::utils::numa
