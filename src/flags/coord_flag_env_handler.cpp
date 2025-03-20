// Copyright 2025 Memgraph Ltd.
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

std::string CoordinationSetup::ToString() {
  return fmt::format(
      "management port: {}, coordinator port: {}, coordinator id: {}, nuraft_log_file: {}, "
      "coordinator_hostname: {}",
      management_port, coordinator_port, coordinator_id, nuraft_log_file, coordinator_hostname);
}

[[nodiscard]] auto CoordinationSetup::IsDataInstanceManagedByCoordinator() const -> bool {
  return management_port != 0 && coordinator_port == 0 && coordinator_id == 0;
}

[[nodiscard]] auto CoordinationSetup::IsCoordinator() const -> bool {
  return coordinator_id != 0 && coordinator_port != 0 && !coordinator_hostname.empty() && management_port != 0;
}

auto CoordinationSetupInstance() -> CoordinationSetup & {
  static auto instance = CoordinationSetup{};
  return instance;
}

void SetFinalCoordinationSetup() {
#ifdef MG_ENTERPRISE

  std::vector<char const *> const maybe_coord_envs{std::getenv(kMgManagementPort), std::getenv(kMgCoordinatorPort),
                                                   std::getenv(kMgCoordinatorId), std::getenv(kMgNuRaftLogFile),
                                                   std::getenv(kMgCoordinatorHostname)};

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
      return CoordinationSetup{
          .management_port = coord_envs[0] ? std::stoi(coord_envs[0].value()) : 0,
          .coordinator_port = coord_envs[1] ? std::stoi(coord_envs[1].value()) : 0,
          .coordinator_id = coord_envs[2] ? static_cast<int32_t>(std::stoul(coord_envs[2].value())) : 0,
          .nuraft_log_file = coord_envs[3] ? coord_envs[3].value() : "",
          .coordinator_hostname = coord_envs[4] ? coord_envs[4].value() : ""};
    }

    spdlog::trace("Coordinator will be initialized using flags.");
    return CoordinationSetup{.management_port = FLAGS_management_port,
                             .coordinator_port = FLAGS_coordinator_port,
                             .coordinator_id = FLAGS_coordinator_id,
                             .nuraft_log_file = FLAGS_nuraft_log_file,
                             .coordinator_hostname = FLAGS_coordinator_hostname};
  }();  // iile
#endif
}

}  // namespace memgraph::flags
