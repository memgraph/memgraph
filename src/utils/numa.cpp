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
#include <cstring>
#include <fstream>
#include <mutex>
#include <sstream>
#include <string>

#include <pthread.h>
#include <sched.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <linux/if_packet.h>
#include <linux/sockios.h>

#include <spdlog/spdlog.h>

namespace memgraph::utils::numa {

namespace {

// Robust integer parsing to avoid std::invalid_argument exceptions from sysfs files
std::optional<int> TryParseInt(const std::string &s) {
  try {
    if (s.empty()) return std::nullopt;
    return std::stoi(s);
  } catch (...) {
    return std::nullopt;
  }
}

// Helper to read a single integer from a file
std::optional<int> ReadIntFromFile(const std::string &path) {
  std::ifstream file(path);
  if (!file.is_open()) return std::nullopt;
  std::string value;
  file >> value;
  return TryParseInt(value);
}

// Helper to read a string from a file
std::optional<std::string> ReadStringFromFile(const std::string &path) {
  std::ifstream file(path);
  if (!file.is_open()) return std::nullopt;
  std::string value;
  std::getline(file, value);
  return value;
}

// Cached topology - initialized once at startup
std::optional<NUMATopology> g_cached_topology;
std::once_flag g_topology_init_flag;

// Internal function that performs the actual topology detection
std::optional<NUMATopology> DetectTopologyInternal() {
  NUMATopology topology;

  // First, discover all NUMA nodes
  std::vector<int> numa_nodes;
  for (int node = 0; node < 1024; ++node) {  // Modern systems can have many nodes
    std::string node_path = "/sys/devices/system/node/node" + std::to_string(node);
    if (access(node_path.c_str(), F_OK) != 0) break;
    numa_nodes.push_back(node);
  }

  if (numa_nodes.empty()) {
    spdlog::warn("No NUMA nodes detected via sysfs, assuming single NUMA node (node 0)");
    numa_nodes.push_back(0);
  }

  for (int node_id : numa_nodes) {
    NUMANode numa_node;
    numa_node.node_id = node_id;

    std::string cpulist_path = "/sys/devices/system/node/node" + std::to_string(node_id) + "/cpulist";
    auto cpulist_str = ReadStringFromFile(cpulist_path);
    if (!cpulist_str) continue;

    std::vector<int> node_cpus = ParseCPUList(*cpulist_str);

    for (const int cpu_id : node_cpus) {
      CPUCore core;
      core.logical_id = cpu_id;
      core.numa_node = node_id;

      std::string core_id_path = "/sys/devices/system/cpu/cpu" + std::to_string(cpu_id) + "/topology/core_id";
      auto core_id = ReadIntFromFile(core_id_path);
      core.physical_core = core_id.value_or(cpu_id);

      std::string siblings_path =
          "/sys/devices/system/cpu/cpu" + std::to_string(cpu_id) + "/topology/thread_siblings_list";
      auto siblings_str = ReadStringFromFile(siblings_path);
      if (siblings_str) {
        std::vector<int> siblings = ParseCPUList(*siblings_str);
        if (!siblings.empty()) {
          std::sort(siblings.begin(), siblings.end());
          core.is_hyperthread = (siblings[0] != cpu_id);
        } else {
          core.is_hyperthread = false;
        }
      } else {
        core.is_hyperthread = false;
      }

      numa_node.cpus.push_back(core);
    }

    std::sort(numa_node.cpus.begin(), numa_node.cpus.end());
    topology.nodes.push_back(std::move(numa_node));
  }

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

  spdlog::info("Detected {} NUMA node(s) with {} total physical cores ({} hyperthreads)", topology.nodes.size(),
               topology.total_cores, topology.total_hyperthreads);

  return topology;
}

const std::optional<NUMATopology> &GetCachedTopology() {
  std::call_once(g_topology_init_flag, []() {
    g_cached_topology = DetectTopologyInternal();
    if (g_cached_topology) {
      spdlog::info("NUMA topology cached successfully");
    }
  });
  return g_cached_topology;
}

}  // namespace

// --- Struct Implementations ---

std::vector<int> NUMANode::GetPrimaryCores() const {
  std::vector<int> result;
  for (const auto &cpu : cpus) {
    if (!cpu.is_hyperthread) result.push_back(cpu.logical_id);
  }
  return result;
}

std::vector<int> NUMANode::GetHyperthreads() const {
  std::vector<int> result;
  for (const auto &cpu : cpus) {
    if (cpu.is_hyperthread) result.push_back(cpu.logical_id);
  }
  return result;
}

const NUMANode *NUMATopology::GetNode(int node_id) const {
  for (const auto &node : nodes) {
    if (node.node_id == node_id) return &node;
  }
  return nullptr;
}

// --- API Implementation ---

void InitializeTopologyCache() { GetCachedTopology(); }

std::optional<NUMATopology> DetectTopology() { return GetCachedTopology(); }

std::vector<int> ParseCPUList(const std::string &cpu_list_str) {
  std::vector<int> cpus;
  if (cpu_list_str.empty()) return cpus;

  std::istringstream iss(cpu_list_str);
  std::string range;

  while (std::getline(iss, range, ',')) {
    size_t dash_pos = range.find('-');
    if (dash_pos != std::string::npos) {
      auto start_opt = TryParseInt(range.substr(0, dash_pos));
      auto end_opt = TryParseInt(range.substr(dash_pos + 1));
      if (start_opt && end_opt) {
        for (int i = *start_opt; i <= *end_opt; ++i) {
          cpus.push_back(i);
        }
      }
    } else {
      auto cpu_opt = TryParseInt(range);
      if (cpu_opt) cpus.push_back(*cpu_opt);
    }
  }
  return cpus;
}

int GetNUMANodeForCPU(int cpu_id) {
  if (cpu_id < 0) return -1;
  const auto &topology = GetCachedTopology();
  if (topology) {
    for (const auto &node : topology->nodes) {
      for (const auto &cpu : node.cpus) {
        if (cpu.logical_id == cpu_id) return node.node_id;
      }
    }
  }
  return -1;
}

bool PinThreadToCPU(int cpu_id) {
  // Use dynamic allocation for systems with > 1024 CPUs
  cpu_set_t *cpuset = CPU_ALLOC(cpu_id + 1);
  size_t size = CPU_ALLOC_SIZE(cpu_id + 1);
  CPU_ZERO_S(size, cpuset);
  CPU_SET_S(cpu_id, size, cpuset);

  int result = pthread_setaffinity_np(pthread_self(), size, cpuset);
  CPU_FREE(cpuset);

  if (result != 0) {
    spdlog::warn("Failed to pin thread to CPU {}: {}", cpu_id, strerror(result));
    return false;
  }
  return true;
}

bool PinThreadToNUMANode(int numa_node) {
  const auto &topology = GetCachedTopology();
  if (!topology) return false;
  return PinThreadToNUMANode(numa_node, *topology);
}

bool PinThreadToNUMANode(int numa_node, const NUMATopology &topology) {
  const NUMANode *node = topology.GetNode(numa_node);
  if (!node || node->cpus.empty()) return false;

  int max_cpu = 0;
  for (const auto &cpu : node->cpus) {
    max_cpu = std::max(cpu.logical_id, max_cpu);
  }

  cpu_set_t *cpuset = CPU_ALLOC(max_cpu + 1);
  size_t size = CPU_ALLOC_SIZE(max_cpu + 1);
  CPU_ZERO_S(size, cpuset);
  for (const auto &cpu : node->cpus) {
    CPU_SET_S(cpu.logical_id, size, cpuset);
  }

  int result = pthread_setaffinity_np(pthread_self(), size, cpuset);
  CPU_FREE(cpuset);

  if (result != 0) {
    spdlog::warn("Failed to pin thread to NUMA node {}: {}", numa_node, strerror(result));
    return false;
  }
  return true;
}

int GetCurrentCPU() {
#ifdef SYS_getcpu
  unsigned cpu = 0;
  if (syscall(SYS_getcpu, &cpu, nullptr, nullptr) == 0) {
    return static_cast<int>(cpu);
  }
#endif
  return -1;
}

int GetCurrentNUMANode() {
  unsigned cpu, node;
#ifdef SYS_getcpu
  // getcpu(cpu, node, cache_is_unused)
  if (syscall(SYS_getcpu, &cpu, &node, nullptr) == 0) {
    return static_cast<int>(node);
  }
#endif
  return -1;
}

int GetIncomingCPU(int socket_fd) {
  int cpu = -1;
  socklen_t len = sizeof(cpu);
  if (getsockopt(socket_fd, SOL_SOCKET, SO_INCOMING_CPU, &cpu, &len) == 0) {
    return cpu;
  }
  return -1;
}

int GetNAPIID(int socket_fd) {
  int napi_id = -1;
  socklen_t len = sizeof(napi_id);
  // Note: SO_INCOMING_NAPI_ID is the correct constant for newer kernels
  if (getsockopt(socket_fd, SOL_SOCKET, SO_INCOMING_NAPI_ID, &napi_id, &len) == 0) {
    return napi_id;
  }
  return -1;
}

}  // namespace memgraph::utils::numa
