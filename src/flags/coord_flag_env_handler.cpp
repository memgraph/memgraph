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

#include <algorithm>
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
#include <range/v3/view/transform.hpp>

namespace memgraph::flags {

CoordinationSetup::CoordinationSetup(int management_port, int coordinator_port, uint32_t coordinator_id,
                                     std::string nuraft_log_file, bool ha_durability, std::string coordinator_hostname)
    : management_port(management_port),  // data instance management port or coordinator instance management port
      coordinator_port(coordinator_port),
      coordinator_id(coordinator_id),
      nuraft_log_file(std::move(nuraft_log_file)),
      ha_durability(ha_durability),
      coordinator_hostname(std::move(coordinator_hostname)) {}

std::string CoordinationSetup::ToString() {
  return fmt::format(
      "management port: {}, coordinator port: {}, coordinator id: {}, nuraft_log_file: {} ha_durability: {}, "
      "coordinator_hostname: {}",
      management_port, coordinator_port, coordinator_id, nuraft_log_file, ha_durability, coordinator_hostname);
}

[[nodiscard]] auto CoordinationSetup::IsDataInstanceManagedByCoordinator() const -> bool {
  return management_port != 0 && coordinator_port == 0 && coordinator_id == 0;
}

auto CoordinationSetupInstance() -> CoordinationSetup & {
  static auto instance = CoordinationSetup{};
  return instance;
}

void SetFinalCoordinationSetup() {
#ifdef MG_ENTERPRISE

  std::vector<char const *> const maybe_coord_envs{std::getenv(kMgManagementPort), std::getenv(kMgCoordinatorPort),
                                                   std::getenv(kMgCoordinatorId),  std::getenv(kMgNuRaftLogFile),
                                                   std::getenv(kMgHaDurability),   std::getenv(kMgCoordinatorHostname)};

  bool const any_envs_set = std::ranges::any_of(maybe_coord_envs, [](char const *env) { return env != nullptr; });

  auto const is_flag_set = []<typename T>(T const &flag) { return flag != T{}; };

  bool const any_flags_set = is_flag_set(FLAGS_management_port) || is_flag_set(FLAGS_coordinator_port) ||
                             is_flag_set(FLAGS_coordinator_id) || is_flag_set(FLAGS_nuraft_log_file) ||
                             is_flag_set(FLAGS_coordinator_hostname);

  if (any_flags_set && any_envs_set) {
    spdlog::warn(
        "Both environment variables and flags are set for coordinator setup. Using environment variables as priority. "
        "Flags will be ignored.");
  }

  CoordinationSetupInstance() = [&]() {
    if (!any_flags_set && !any_envs_set) {
      return CoordinationSetup{};
    }

    auto const trim = [](char const *flag) -> std::optional<std::string> {
      if (flag == nullptr) {
        return std::nullopt;
      }

      auto const is_space = [](auto c) { return c == ' '; };

      return std::string_view{flag} | ranges::views::drop_while(is_space) |
             ranges::views::take_while(std::not_fn(is_space)) | ranges::to<std::string>;
    };

    auto const coord_envs =
        maybe_coord_envs | ranges::views::transform(trim) | ranges::to<std::vector<std::optional<std::string>>>;

    if (any_envs_set) {
      spdlog::trace("Coordinator will be initialized using environment variables.");
      return CoordinationSetup(coord_envs[0] ? std::stoi(coord_envs[0].value()) : 0,
                               coord_envs[1] ? std::stoi(coord_envs[1].value()) : 0,
                               coord_envs[2] ? static_cast<uint32_t>(std::stoul(coord_envs[2].value())) : 0,
                               coord_envs[3] ? coord_envs[3].value() : "",
                               coord_envs[4] ? static_cast<bool>(std::stoi(coord_envs[4].value())) : false,
                               coord_envs[5] ? coord_envs[5].value() : "");
    }

    spdlog::trace("Coordinator will be initilized using flags.");
    return CoordinationSetup{FLAGS_management_port, FLAGS_coordinator_port, FLAGS_coordinator_id,
                             FLAGS_nuraft_log_file, FLAGS_ha_durability,    FLAGS_coordinator_hostname};
  }();  // iile
#endif
}

}  // namespace memgraph::flags
