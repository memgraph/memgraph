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

#include <gflags/gflags.h>
#include <spdlog/spdlog.h>

#include <limits>
#include "utils/flag_validation.hpp"

#ifdef MG_ENTERPRISE
// NOLINTBEGIN(performance-avoid-endl)
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_VALIDATED_int32(management_port, 0, "Port on which coordinator servers will be started.",
                       FLAG_IN_RANGE(0, std::numeric_limits<uint16_t>::max()));  // NOLINT
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_VALIDATED_int32(coordinator_port, 0, "Port on which raft servers will be started.",
                       FLAG_IN_RANGE(0, std::numeric_limits<uint16_t>::max()));  // NOLINT
// NOLINTEND(performance-avoid-endl)
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_int32(coordinator_id, 0, "Unique ID of the raft server.");
// NOLINTNEXTLINE
DEFINE_VALIDATED_HIDDEN_uint32(
    instance_down_timeout_sec, 5, "Time duration after which an instance is considered down.", {
      spdlog::warn(
          "The instance_down_timeout_sec flag is deprecated and its value is ignored. Please set this setting by "
          "running \"SET COORDINATOR SETTING 'instance_down_timeout_sec' TO <YOUR-VALUE>\" query on any coordinator.");
      return true;
    });
// NOLINTNEXTLINE
DEFINE_VALIDATED_HIDDEN_uint32(
    instance_health_check_frequency_sec, 1, "The time duration between two health checks/pings.", {
      spdlog::warn(
          "The instance_health_check_frequency_sec is deprecated and its value is ignored. Please set this setting by "
          "running \"SET COORDINATOR SETTING 'instance_health_check_frequency_sec' TO <YOUR-VALUE>\" query on any "
          "coordinator.");
      return true;
    });
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_string(nuraft_log_file, "", "Path to the file where NuRaft logs are saved.");
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_string(coordinator_hostname, "", "Instance's hostname. Used as output of SHOW INSTANCES query.");
#endif
