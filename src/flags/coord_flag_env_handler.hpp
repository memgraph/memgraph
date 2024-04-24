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

struct CoordinationSetup {
  int management_port{0};
  int coordinator_port{0};
  uint32_t coordinator_id{0};

  explicit CoordinationSetup(int management_port, int coordinator_port, uint32_t coordinator_id);

  std::string ToString();

  [[nodiscard]] auto IsCoordinatorManaged() const -> bool;
};

auto CoordinationSetupInstance() -> CoordinationSetup &;

void SetFinalCoordinationSetup();

}  // namespace memgraph::flags
