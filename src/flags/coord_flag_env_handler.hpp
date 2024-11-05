// Copyright 2024 Memgraph Ltd.
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
#include <string>

namespace memgraph::flags {

constexpr const char *kMgManagementPort = "MEMGRAPH_MANAGEMENT_PORT";
constexpr const char *kMgCoordinatorPort = "MEMGRAPH_COORDINATOR_PORT";
constexpr const char *kMgCoordinatorId = "MEMGRAPH_COORDINATOR_ID";
constexpr const char *kMgHaDurability = "MEMGRAPH_HA_DURABILITY";
constexpr const char *kMgNuRaftLogFile = "MEMGRAPH_NURAFT_LOG_FILE";
constexpr const char *kMgCoordinatorHostname = "MEMGRAPH_COORDINATOR_HOSTNAME";

// Contains values that could be set by environment variables or flags.
// These values could be used both for data instances and coordinator instances
struct CoordinationSetup {
  int management_port{0};
  int coordinator_port{0};
  uint32_t coordinator_id{0};
  std::string nuraft_log_file;
  bool ha_durability{false};
  std::string coordinator_hostname;

  explicit CoordinationSetup(int management_port, int coordinator_port, uint32_t coordinator_id,
                             std::string nuraft_log_file, bool ha_durability, std::string coordinator_hostname);
  CoordinationSetup() = default;

  std::string ToString();

  [[nodiscard]] auto IsDataInstanceManagedByCoordinator() const -> bool;
};

auto CoordinationSetupInstance() -> CoordinationSetup &;

void SetFinalCoordinationSetup();

}  // namespace memgraph::flags
