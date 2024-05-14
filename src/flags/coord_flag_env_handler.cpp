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
#include <string>
#include <string_view>

#include "flags/coord_flag_env_handler.hpp"
#include "flags/coordination.hpp"

#include <fmt/core.h>
#include <spdlog/spdlog.h>
#include <range/v3/range/conversion.hpp>
#include <range/v3/view/drop_while.hpp>
#include <range/v3/view/take_while.hpp>

namespace memgraph::flags {

CoordinationSetup::CoordinationSetup(int management_port, int coordinator_port, uint32_t coordinator_id)
    : management_port(management_port), coordinator_port(coordinator_port), coordinator_id(coordinator_id) {}

std::string CoordinationSetup::ToString() {
  return fmt::format("management port: {}, coordinator port {}, coordinator id", management_port, coordinator_port,
                     coordinator_id);
}

[[nodiscard]] auto CoordinationSetup::IsCoordinatorManaged() const -> bool { return management_port != 0; }

auto CoordinationSetupInstance() -> CoordinationSetup & {
  static auto instance = CoordinationSetup{0, 0, 0};
  return instance;
}

void SetFinalCoordinationSetup() {
#ifdef MG_ENTERPRISE
  auto const *maybe_management_port = std::getenv(kMgManagementPort);
  auto const *maybe_coordinator_port = std::getenv(kMgCoordinatorPort);
  auto const *maybe_coordinator_id = std::getenv(kMgCoordinatorId);

  bool const are_envs_set = maybe_management_port || maybe_coordinator_port || maybe_coordinator_id;
  bool const are_flags_set = FLAGS_management_port || FLAGS_coordinator_port || FLAGS_coordinator_id;

  if (are_envs_set && are_flags_set) {
    spdlog::trace(
        "Ignoring coordinator setup(management_port, coordinator_port and coordinator_id) sent via flags as there is "
        "input in environment variables");
  }

  auto const canonicalize_string = [](auto &&rng) {
    auto const is_space = [](auto c) { return c == ' '; };

    return rng | ranges::views::drop_while(is_space) | ranges::views::take_while(std::not_fn(is_space)) |
           ranges::to<std::string>;
  };

  CoordinationSetupInstance() = [&]() {
    if (!are_envs_set && !are_flags_set) {
      return CoordinationSetup{0, 0, 0};
    }
    if (are_envs_set) {
      spdlog::trace("Read coordinator setup from env variables: {}.", CoordinationSetupInstance().ToString());
      return CoordinationSetup(
          maybe_management_port ? std::stoi(canonicalize_string(std::string_view{maybe_management_port})) : 0,
          maybe_coordinator_port ? std::stoi(canonicalize_string(std::string_view{maybe_coordinator_port})) : 0,
          maybe_coordinator_id
              ? static_cast<uint32_t>(std::stoul(canonicalize_string(std::string_view{maybe_coordinator_id})))
              : 0);
    }
    spdlog::trace("Read coordinator setup from runtime flags {}.", CoordinationSetupInstance().ToString());
    return CoordinationSetup{FLAGS_management_port, FLAGS_coordinator_port, FLAGS_coordinator_id};
  }();  // iile
#endif
}

}  // namespace memgraph::flags
