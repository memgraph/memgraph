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

#include "flags/scheduler.hpp"

#include "utils/enum.hpp"
#include "utils/flag_validation.hpp"

namespace {
constexpr auto kAsio = "asio";
constexpr auto kPriorityQueueWithSidecar = "priority_queue";

inline constexpr std::array scheduler_type_mappings{
    std::pair{kAsio, SchedulerType::ASIO},
    std::pair{kPriorityQueueWithSidecar, SchedulerType::PRIORITY_QUEUE_WITH_SIDECAR}};
}  // namespace

const std::string scheduler_helper_string =
    fmt::format("Selects the scheduler used to execute bolt commands. Available schedulers: {}",
                memgraph::utils::GetAllowedEnumValuesString(scheduler_type_mappings));

// Bolt server flags.

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables, misc-unused-parameters)
DEFINE_VALIDATED_HIDDEN_string(scheduler, kPriorityQueueWithSidecar, scheduler_helper_string.c_str(), {
  return memgraph::utils::IsValidEnumValueString(value, scheduler_type_mappings).has_value();
});

SchedulerType GetSchedulerType() {
  return memgraph::utils::StringToEnum<SchedulerType>(FLAGS_scheduler, scheduler_type_mappings).value();
}
