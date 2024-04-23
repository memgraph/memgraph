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

auto CoordinationSetupInstance() -> CoordinationSetup & {
  static auto instance = CoordinationSetup{};
  return instance;
}

auto GetFinalCoordinationSetup() -> CoordinationSetup {
  auto const *maybe_management_port = std::getenv(kMgManagementPort);
  auto const *maybe_coordinator_port = std::getenv(kMgCoordinatorPort);
  auto const *maybe_coordinator_id = std::getenv(kMgCoordinatorId);

  bool const are_envs_set = maybe_management_port || maybe_coordinator_port || maybe_coordinator_id;
  bool const are_flags_set = FLAGS_management_port || FLAGS_coordinator_port || FLAGS_coordinator_id;

  if (are_envs_set) {
    CoordinationSetupInstance() =
        CoordinationSetup(maybe_management_port ? std::stoi(maybe_management_port) : 0,
                          maybe_coordinator_port ? std::stoi(maybe_coordinator_port) : 0,
                          maybe_coordinator_id ? static_cast<uint32_t>(std::stoul(maybe_coordinator_id)) : 0);
    spdlog::trace("Read coordinator setup from env variables: {}.", CoordinationSetupInstance().ToString());
  } else if (are_flags_set) {
    CoordinationSetupInstance() =
        CoordinationSetup(FLAGS_management_port, FLAGS_coordinator_port, FLAGS_coordinator_id);
    spdlog::trace("Read coordinator setup from runtime flags {}.", CoordinationSetupInstance().ToString());
  }

  if (are_envs_set && are_flags_set) {
    spdlog::trace(
        "Ignored coordinator setup(management_port, coordinator_port and coordinator_id) sent via flags as there is "
        "input in environment variables");
  }

  return CoordinationSetupInstance();
}

}  // namespace memgraph::flags
