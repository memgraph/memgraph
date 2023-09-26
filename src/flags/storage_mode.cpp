// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "flags/storage_mode.hpp"

#include "storage/v2/storage_mode.hpp"
#include "utils/enum.hpp"
#include "utils/flag_validation.hpp"

#include "gflags/gflags.h"

#include <array>

inline constexpr std::array storage_mode_mappings{
    std::pair{std::string_view{"IN_MEMORY_TRANSACTIONAL"}, memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL},
    std::pair{std::string_view{"IN_MEMORY_ANALYTICAL"}, memgraph::storage::StorageMode::IN_MEMORY_ANALYTICAL},
    std::pair{std::string_view{"ON_DISK_TRANSACTIONAL"}, memgraph::storage::StorageMode::ON_DISK_TRANSACTIONAL}};

const std::string storage_mode_help_string =
    fmt::format("Default storage mode Memgraph uses. Allowed values: {}",
                memgraph::utils::GetAllowedEnumValuesString(storage_mode_mappings));

// NOLINTNEXTLINE (cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_VALIDATED_string(storage_mode, "IN_MEMORY_TRANSACTIONAL", storage_mode_help_string.c_str(), {
  if (const auto result = memgraph::utils::IsValidEnumValueString(value, storage_mode_mappings); result.HasError()) {
    switch (result.GetError()) {
      case memgraph::utils::ValidationError::EmptyValue: {
        std::cout << "Storage mode cannot be empty." << std::endl;
        break;
      }
      case memgraph::utils::ValidationError::InvalidValue: {
        std::cout << "Invalid value for storage mode. Allowed values: "
                  << memgraph::utils::GetAllowedEnumValuesString(storage_mode_mappings) << std::endl;
        break;
      }
    }
    return false;
  }
  return true;
});

memgraph::storage::StorageMode memgraph::flags::ParseStorageMode() {
  const auto storage_mode =
      memgraph::utils::StringToEnum<memgraph::storage::StorageMode>(FLAGS_storage_mode, storage_mode_mappings);
  MG_ASSERT(storage_mode, "Invalid storage mode");
  return *storage_mode;
}
