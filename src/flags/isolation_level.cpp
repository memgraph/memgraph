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
#include "flags/isolation_level.hpp"

#include "utils/enum.hpp"
#include "utils/flag_validation.hpp"

#include "gflags/gflags.h"

#include <array>
#include <iostream>
#include <string_view>

inline constexpr std::array isolation_level_mappings{
    std::pair{std::string_view{"SNAPSHOT_ISOLATION"}, memgraph::storage::IsolationLevel::SNAPSHOT_ISOLATION},
    std::pair{std::string_view{"READ_COMMITTED"}, memgraph::storage::IsolationLevel::READ_COMMITTED},
    std::pair{std::string_view{"READ_UNCOMMITTED"}, memgraph::storage::IsolationLevel::READ_UNCOMMITTED}};

const std::string isolation_level_help_string =
    fmt::format("Default isolation level used for the transactions. Allowed values: {}",
                memgraph::utils::GetAllowedEnumValuesString(isolation_level_mappings));

// NOLINTNEXTLINE (cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_VALIDATED_string(isolation_level, "SNAPSHOT_ISOLATION", isolation_level_help_string.c_str(), {
  if (const auto result = memgraph::utils::IsValidEnumValueString(value, isolation_level_mappings); result.HasError()) {
    switch (result.GetError()) {
      case memgraph::utils::ValidationError::EmptyValue: {
        std::cout << "Isolation level cannot be empty." << std::endl;
        break;
      }
      case memgraph::utils::ValidationError::InvalidValue: {
        std::cout << "Invalid value for isolation level. Allowed values: "
                  << memgraph::utils::GetAllowedEnumValuesString(isolation_level_mappings) << std::endl;
        break;
      }
    }
    return false;
  }
  return true;
});

memgraph::storage::IsolationLevel memgraph::flags::ParseIsolationLevel() {
  const auto isolation_level =
      memgraph::utils::StringToEnum<memgraph::storage::IsolationLevel>(FLAGS_isolation_level, isolation_level_mappings);
  MG_ASSERT(isolation_level, "Invalid isolation level");
  return *isolation_level;
}
