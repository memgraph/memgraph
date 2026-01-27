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

#include <optional>
#include <string>
#include <vector>

namespace memgraph::utils::numa {

struct CPUCore {
  int logical_id;
  int physical_core;
  int numa_node;
  bool is_hyperthread;

  bool operator<(const CPUCore &other) const {
    if (numa_node != other.numa_node) return numa_node < other.numa_node;
    if (physical_core != other.physical_core) return physical_core < other.physical_core;
    return logical_id < other.logical_id;
  }
};

struct NUMANode {
  int node_id;
  std::vector<CPUCore> cpus;

  std::vector<int> GetPrimaryCores() const;
  std::vector<int> GetHyperthreads() const;
};

struct NUMATopology {
  std::vector<NUMANode> nodes;
  int total_cores = 0;
  int total_hyperthreads = 0;

  size_t GetNUMANodeCount() const { return nodes.size(); }
  const NUMANode *GetNode(int node_id) const;
};

std::optional<NUMATopology> DetectTopology();
void InitializeTopologyCache();
int GetNUMANodeForCPU(int cpu_id);
std::vector<int> ParseCPUList(const std::string &cpu_list_str);
bool PinThreadToCPU(int cpu_id);
bool PinThreadToNUMANode(int numa_node);
bool PinThreadToNUMANode(int numa_node, const NUMATopology &topology);
int GetCurrentCPU();
int GetCurrentNUMANode();
int GetIncomingCPU(int socket_fd);
int GetNAPIID(int socket_fd);

}  // namespace memgraph::utils::numa
