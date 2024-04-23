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

#include <cstdint>
#include <cstdlib>

#include "flags/coord_flag_env_handler.hpp"
#include "flags/coordination.hpp"
#include "utils/logging.hpp"

#include <spdlog/spdlog.h>

namespace memgraph::flags {

auto GetFinalCoordinationSetup() -> CoordinationSetup {
  CoordinationSetup coordination_setup{};

  auto *maybe_management_port = std::getenv(kMgManagementPort);
  auto *maybe_coordinator_port = std::getenv(kMgCoordinatorPort);
  auto *maybe_coordinator_id = std::getenv(kMgCoordinatorId);

  bool are_envs_set = maybe_management_port || maybe_coordinator_port || maybe_coordinator_id;
  bool are_flags_set = FLAGS_management_port || FLAGS_coordinator_port || FLAGS_coordinator_id;

  if (are_envs_set) {
    coordination_setup.management_port = maybe_management_port ? std::stoi(maybe_management_port) : 0;
    coordination_setup.coordinator_port = maybe_coordinator_port ? std::stoi(maybe_coordinator_port) : 0;
    coordination_setup.coordinator_id =
        maybe_coordinator_id ? static_cast<uint32_t>(std::stoul(maybe_coordinator_id)) : 0;
    spdlog::trace("Reading coordinator env variables management_port: {}, coordinator_port: {} and coordinator_id {}.",
                  coordination_setup.management_port, coordination_setup.coordinator_port,
                  coordination_setup.coordinator_id);
  } else if (are_flags_set) {
    coordination_setup.management_port = FLAGS_management_port;
    coordination_setup.coordinator_port = FLAGS_coordinator_port;
    coordination_setup.coordinator_id = FLAGS_coordinator_id;
    spdlog::trace("Reading coordinator config flags management_port: {}, coordinator_port: {} and coordinator_id {}.",
                  coordination_setup.management_port, coordination_setup.coordinator_port,
                  coordination_setup.coordinator_id);
  }

  if (are_envs_set && are_flags_set) {
    spdlog::trace(
        "Ignored coordinator setup(management_port, coordinator_port and coordinator_id) sent via flags as there is "
        "input in environment variables");
  }

  return coordination_setup;
}

}  // namespace memgraph::flags
