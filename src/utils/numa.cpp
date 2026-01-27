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

#include "utils/numa.hpp"

#include <algorithm>
#include <fstream>
#include <sstream>
#include <string>

#include <pthread.h>
#include <sched.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <linux/if_packet.h>
#include <linux/sockios.h>
#include <cstring>

#include <spdlog/spdlog.h>

#include "utils/logging.hpp"

namespace memgraph::utils::numa {

namespace {
// Helper to read a single integer from a file
std::optional<int> ReadIntFromFile(const std::string &path) {
  std::ifstream file(path);
  if (!file.is_open()) return std::nullopt;
  int value = 0;
  file >> value;
  return value;
}

// Helper to read a string from a file
std::optional<std::string> ReadStringFromFile(const std::string &path) {
  std::ifstream file(path);
  if (!file.is_open()) return std::nullopt;
  std::string value;
  std::getline(file, value);
  return value;
}
}  // namespace

std::vector<int> ParseCPUList(const std::string &cpu_list_str) {
  std::vector<int> cpus;
  std::istringstream iss(cpu_list_str);
  std::string range;

  while (std::getline(iss, range, ',')) {
    range.erase(0, range.find_first_not_of(" \t"));
    range.erase(range.find_last_not_of(" \t") + 1);

    size_t dash_pos = range.find('-');
    if (dash_pos != std::string::npos) {
      // Range like "0-7"
      int start = std::stoi(range.substr(0, dash_pos));
      int end = std::stoi(range.substr(dash_pos + 1));
      for (int i = start; i <= end; ++i) {
        cpus.push_back(i);
      }
    } else {
      // Single CPU
      cpus.push_back(std::stoi(range));
    }
  }

  return cpus;
}

std::optional<NUMATopology> DetectTopology() {
  NUMATopology topology;

  // First, discover all NUMA nodes
  std::vector<int> numa_nodes;
  for (int node = 0; node < 256; ++node) {  // Reasonable limit
    std::string cpulist_path = "/sys/devices/system/node/node" + std::to_string(node) + "/cpulist";
    std::ifstream file(cpulist_path);
    if (!file.is_open()) break;  // No more NUMA nodes
    numa_nodes.push_back(node);
  }

  if (numa_nodes.empty()) {
    spdlog::warn("No NUMA nodes detected, assuming single NUMA node (node 0)");
    numa_nodes.push_back(0);
  }

  // For each NUMA node, get its CPUs
  for (int node_id : numa_nodes) {
    NUMANode numa_node;
    numa_node.node_id = node_id;

    std::string cpulist_path = "/sys/devices/system/node/node" + std::to_string(node_id) + "/cpulist";
    auto cpulist_str = ReadStringFromFile(cpulist_path);
    if (!cpulist_str) continue;

    std::vector<int> node_cpus = ParseCPUList(*cpulist_str);

    // For each CPU, get its physical core and determine if it's a hyperthread
    for (const int cpu_id : node_cpus) {
      CPUCore core;
      core.logical_id = cpu_id;
      core.numa_node = node_id;

      // Get physical core ID
      std::string core_id_path = "/sys/devices/system/cpu/cpu" + std::to_string(cpu_id) + "/topology/core_id";
      auto core_id = ReadIntFromFile(core_id_path);
      if (core_id) {
        core.physical_core = *core_id;
      } else {
        core.physical_core = cpu_id;  // Fallback
      }

      // Determine if this is a hyperthread by checking siblings
      // If a core has multiple logical CPUs, all but the first are hyperthreads
      std::string siblings_path =
          "/sys/devices/system/cpu/cpu" + std::to_string(cpu_id) + "/topology/thread_siblings_list";
      auto siblings_str = ReadStringFromFile(siblings_path);
      if (siblings_str) {
        std::vector<int> siblings = ParseCPUList(*siblings_str);
        // Sort siblings to get the primary (lowest ID)
        if (!siblings.empty()) {
          std::sort(siblings.begin(), siblings.end());
        }
        core.is_hyperthread = (siblings[0] != cpu_id);
      } else {
        core.is_hyperthread = false;  // Assume primary if we can't determine
      }

      numa_node.cpus.push_back(core);
    }

    // Sort CPUs within the node (primary cores first, then hyperthreads)
    std::sort(numa_node.cpus.begin(), numa_node.cpus.end());
    topology.nodes.push_back(numa_node);
  }

  // Calculate totals
  topology.total_cores = 0;
  topology.total_hyperthreads = 0;
  for (const auto &node : topology.nodes) {
    for (const auto &cpu : node.cpus) {
      if (cpu.is_hyperthread) {
        topology.total_hyperthreads++;
      } else {
        topology.total_cores++;
      }
    }
  }

  spdlog::info("Detected {} NUMA node(s) with {} total cores ({} hyperthreads)", topology.nodes.size(),
               topology.total_cores, topology.total_hyperthreads);

  return topology;
}

int GetNUMANodeForCPU(int cpu_id) {
  if (cpu_id < 0) return -1;

  // Read /sys/devices/system/node/node*/cpulist to find which NUMA node contains this CPU
  for (int node = 0; node < 256; ++node) {  // Reasonable limit
    std::string path = "/sys/devices/system/node/node" + std::to_string(node) + "/cpulist";
    std::ifstream file(path);
    if (!file.is_open()) break;  // No more NUMA nodes

    std::string line;
    if (std::getline(file, line)) {
      std::vector<int> cpus = ParseCPUList(line);
      for (const int cpu : cpus) {
        if (cpu == cpu_id) {
          return node;
        }
      }
    }
  }
  return -1;  // Unknown
}

bool PinThreadToCPU(int cpu_id) {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(cpu_id, &cpuset);

  pthread_t thread = pthread_self();
  int result = pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
  if (result != 0) {
    spdlog::warn("Failed to pin thread to CPU {}: {}", cpu_id, strerror(result));
    return false;
  }
  return true;
}

bool PinThreadToNUMANode(int numa_node) {
  auto topology = DetectTopology();
  if (!topology) return false;

  const NUMANode *node = topology->GetNode(numa_node);
  if (!node) return false;

  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  for (const auto &cpu : node->cpus) {
    CPU_SET(cpu.logical_id, &cpuset);
  }

  pthread_t thread = pthread_self();
  int result = pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
  if (result != 0) {
    spdlog::warn("Failed to pin thread to NUMA node {}: {}", numa_node, strerror(result));
    return false;
  }
  return true;
}

int GetCurrentCPU() {
#ifdef SYS_getcpu
  unsigned cpu = 0;
  unsigned node = 0;
  if (syscall(SYS_getcpu, &cpu, &node, nullptr) == 0) {
    return static_cast<int>(cpu);
  }
#endif
  // Fallback: try to read from /proc/self/stat
  // Field 39 (0-indexed) is the CPU number
  std::ifstream stat_file("/proc/self/stat");
  if (stat_file.is_open()) {
    std::string line;
    std::getline(stat_file, line);
    std::istringstream iss(line);
    std::string token;
    // Skip first 38 fields
    for (int i = 0; i < 38 && iss >> token; ++i) {
    }
    if (iss >> token) {
      return std::stoi(token);
    }
  }
  return -1;  // Unknown
}

int GetCurrentNUMANode() {
  int cpu = GetCurrentCPU();
  if (cpu < 0) return -1;
  return GetNUMANodeForCPU(cpu);
}

int GetIncomingCPU(int socket_fd) {
  // SO_INCOMING_CPU is available on Linux 3.19+
  int cpu = -1;
  socklen_t len = sizeof(cpu);
  if (getsockopt(socket_fd, SOL_SOCKET, SO_INCOMING_CPU, &cpu, &len) == 0) {
    return cpu;
  }
  return -1;
}

int GetNAPIID(int socket_fd) {
  // NAPI_ID is available via SO_INCOMING_NAPI_ID (Linux 4.12+)
  int napi_id = -1;
  socklen_t len = sizeof(napi_id);
  if (getsockopt(socket_fd, SOL_SOCKET, SO_INCOMING_NAPI_ID, &napi_id, &len) == 0) {
    return napi_id;
  }
  return -1;
}

}  // namespace memgraph::utils::numa
